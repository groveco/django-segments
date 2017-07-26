from functools import wraps
from django.db import models, transaction, DatabaseError, OperationalError
from django.core.exceptions import ValidationError
from django.db.models.query_utils import InvalidQuery
from django.contrib.auth import get_user_model
from django.conf import settings
from django.db.models import signals
from segments import app_settings
import logging

logger = logging.getLogger(__name__)


class SegmentExecutionError(Exception):
    """
    Any SQL issues encountered when Segments executing their SQL definitions will raise this exception.
    """
    pass


class SegmentDefinitionUnescaped(Exception):
    """
    Raised when an unescaped percent sign is encountered
    """
    pass


def live_sql(fn):
    """
    Function decorator for any segment methods that will execute user SQL (segment.definition). Userspace SQL can
    fail in any number of ways and this standardizes the error handling. This is necessary (vs. just making the actual
    execution of sql a class method) because most of the SQL access is done through RawQuerySets, which are lazy. So
    the execution doesn't necessarily fail when a RawQuerySet is created, but can happen much later in a function, when
    the results are reified. So we just wrap the whole darn function to capture any SQL errors.
    """
    @wraps(fn)
    def _wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except InvalidQuery:
            raise SegmentExecutionError('SQL definition must include the primary key of the %s model'
                                        % settings.AUTH_USER_MODEL)
        except (DatabaseError, OperationalError) as e:
            raise SegmentExecutionError('Error while executing SQL definition: %s' % e)
        except Exception as e:
            raise SegmentExecutionError(e)
    return _wrapper


class Segment(models.Model):

    """
    A segment, as defined by a SQL query. Segments are designed to be periodically refreshed, which populates
    an intermediate table of users (termed "members"), along with the segments they belong to. In other words,
    the SQL definition is not normally executed "live".

    There are a number of "live" methods on this class, and their corollaries on the SegmentMixin, all of which
    carry the following caveat:

    LIVE METHOD --  Use at your own risk. These methods perform the equivalent function of their non-live counterparts
    and are post-fixed with _live. They execute the underlying segment definition SQL synchronously and perform no
    validation on that SQL. Therefore they can be slow, and they can fail. Finally because this is live,
    and most of the time consuming code will (presumably) be using the non-live version of these methods, mysterious
    bugs can crop of if code makes the assumption that the normal and _live version of these functions are equivalent.
    """

    name = models.CharField(max_length=128)
    slug = models.SlugField(max_length=256, null=True, blank=True, unique=True)
    definition = models.TextField(help_text="SQL query returning IDs of users in the segment.", blank=True, null=True)
    static_ids = models.TextField(help_text="Newline-delimited list of IDs in the segment", blank=True, null=True)
    created_date = models.DateTimeField(auto_now_add=True)
    manager_name = models.CharField(max_length=128, default="objects", help_text="If using manager_method, specify the name of the manager (usually 'objects')")
    manager_method = models.CharField(max_length=128, null=True, blank=True,
                                      help_text='Methoed to call on ContentType.model_class().manager_name')

    ############
    # Public API
    ############

    def has_member(self, user):
        """
        Helper method. Return a bool indicating whether the user is a member of this segment.
        """
        if not user.id:
            return False
        return self.members.filter(id=user.id).exists()

    @live_sql
    def has_member_live(self, user):
        """
        Live version of helper method. Return a bool indicating whether the user is a member of this segment.

        Also updates the computed segment membership accordingly.
        """
        if not user.id:
            return False
        exists = False
        if self._is_sql_based:
            exists = bool(list(self._execute_raw_user_query(user=user)))
        if self.static_ids:
            exists = exists or user.id in self._parsed_static_ids
        if self.pk:  # Verify that segment is saved
            if exists:
                SegmentMembership.objects.get_or_create(user=user, segment=self)
            else:
                SegmentMembership.objects.filter(user=user, segment=self).delete()
        return exists

    @property
    def members(self):
        """Return a queryset of all users (typed as settings.AUTH_USER_MODEL) that are members of this segment."""
        # The ORM is smart enough to issue this as one query with a subquery
        return self._users_from_ids(self.member_set.all().values_list('user_id', flat=True))

    @property
    @live_sql
    def members_live(self):
        """
        Live version of .members. Issue SQL synchronously and return a raw queryset of all members.

        Refreshing a segment ultimately proxies to this method.
        """

        if self._is_sql_based and not self.static_ids:
            return self._execute_raw_user_query()

        if self.static_ids and not self._is_sql_based:
            return self._users_from_ids(self._parsed_static_ids)

        if self.static_ids and self._is_sql_based:  # If there are SQL users and static users, dedupe and retrieve
            from_sql_ids = [u.id for u in self._execute_raw_user_query()]
            distinct_users = set(from_sql_ids + self._parsed_static_ids)
            return self._users_from_ids(distinct_users)

    @live_sql
    def refresh(self):
        """Figure out the 'diff', add and remove SegmentMemberships, so the segment will contain relevant users only """
        with transaction.atomic():
            user_ids = []
            if self._is_sql_based:
                user_ids += [u.id for u in self._execute_raw_user_query()]
            if self.static_ids:
                user_ids += self._parsed_static_ids

            user_ids = set(user_ids)
            existing = set(self.member_set.values_list('user_id', flat=True))
            to_remove = existing - user_ids
            to_add = user_ids - existing

            memberships = [SegmentMembership(user_id=uid, segment=self) for uid in to_add]
            self._flush(to_remove)
            SegmentMembership.objects.bulk_create(memberships)

    #################
    # Private methods
    #################

    @property
    def _is_sql_based(self):
        return bool(self.definition or (self.manager_name and self.manager_method))

    def _sql(self):
        """
        If there is a SQL definition, use that.
        If there is a content type + manager method definition, generate the SQL (and relevant params)
        Returns SQL string and params to get merged in. Can't return merged SQL because it might be invalid
        as per https://github.com/django/django/blob/master/django/db/models/sql/query.py#L223
        """
        if self.definition:
            return self.definition, []
        if self.manager_name and self.manager_method:
            manager = getattr(get_user_model(), self.manager_name)
            fn = getattr(manager, self.manager_method)
            return fn().query.sql_with_params()

    def _users_from_ids(self, ids):
        return get_user_model().objects.filter(id__in=ids)

    def _execute_raw_user_query(self, user=None):
        """
        Helper that returns a RawQuerySet of user objects.
        """
        sql, params = self._get_sql(user)
        return get_user_model().objects.db_manager(app_settings.SEGMENTS_EXEC_CONNECTION).raw(sql, params)

    @property
    def _parsed_static_ids(self):
        def try_cast_int(to_cast):
            try:
                return int(to_cast)
            except ValueError:
                pass
        parsed = [try_cast_int(s.strip()) for s in self.static_ids.split('\n')] if self.static_ids else []
        return [i for i in parsed if i is not None]

    def _get_sql(self, user=None):
        """
        If needed, wraps the segment definition SQL to return a single result. Query optimizers take advantage of this
        to speed up the query when we are only interrogating the segment for a single user.
        """
        sql, params = self._sql()
        if user and user.id:
            return 'SELECT id FROM (%s) as temp WHERE id=%s' % (sql, user.id), params
        else:
            return sql, params

    def clean(self):
        """
        Validate that the definition SQL will execute. Needed for proper error handling in the Django admin.
        Executes the underlying definition SQL (via .members_live) and returns a list of the users in the segment.

        This could get memory-hungry if the query is returning a lot of users. Calling list() on the queryset executes
        it (makes it non-lazy). This is necessary in order to verify that the underlying SQL is in fact valid.
        """
        if self._is_sql_based:
            try:
                list(self._execute_raw_user_query())
            except SegmentExecutionError as e:
                raise ValidationError(e)

    def _flush(self, user_ids=None):
        """Delete old segment membership data."""
        queryset = SegmentMembership.objects.filter(segment=self)
        if user_ids is not None:
            queryset = queryset.filter(user_id__in=user_ids)
        queryset.delete()

    def __len__(self):
        """Calling len() on a segment returns the number of members of that segment."""
        return self.members.count()

    def __unicode__(self):
        return unicode(self.name)

    @property
    def static_users_sample(self):
        if self.static_ids:
            users = self._users_from_ids(self._parsed_static_ids)
            return '\n'.join([u.email for u in users[:100]])
        return ''

    @property
    @live_sql
    def sql_users_sample(self):
        if self._is_sql_based:
            users = self._execute_raw_user_query()
            return '\n'.join([u.email for u in users[:100]])
        return ''


def do_refresh(sender, instance, created, **kwargs):
    """
    Connected to Segment's post_save signal.

    Always refresh the segment if a new segment is being created. If this is just a save, only refresh if the option
    is set. Some consumers may want to refresh only according to a cron schedule (ie. asynchronously) instead of on
    every segment save.
    """
    from segments.tasks import refresh_segment
    if created or app_settings.SEGMENTS_REFRESH_ON_SAVE:
        if app_settings.SEGMENTS_REFRESH_ASYNC:
            refresh_segment.delay(instance.id)
        else:
            instance.refresh()
signals.post_save.connect(do_refresh, sender=Segment)


class SegmentMembership(models.Model):

    """
    Intermediate model that stores membership information for the segments. This data is generally used vs. executing
    the live Segment.definition SQL.
    """

    user = models.ForeignKey(settings.AUTH_USER_MODEL, related_name='segment_set')
    segment = models.ForeignKey(Segment, related_name='member_set')

    class Meta:
        unique_together = (('user', 'segment',),)


class SegmentMixin(object):

    """
    A Mixin for use with custom user models.

    Example implementation:

    >>> from django.contrib.auth.models import AbstractUser
    >>> from segments.models import SegmentMixin

    >>> class SegmentableUser(AbstractUser, SegmentMixin):
    >>>     pass

    Example usage:

    >>> u = SegmentableUser()
    >>> s = Segment(definition = "select * from %s" % SegmentableUser._meta.db_table)
    >>> print u.is_member(s)  # "True"
    """

    @property
    def segments(self):
        """Return all the segments to which this member belongs."""
        return Segment.objects.filter(id__in=self.segment_set.all().values_list('segment_id', flat=True))

    def is_member(self, segment):
        """Helper method. Proxies to segment.has_member(self)"""
        return segment.has_member(self)

    def is_member_live(self, segment):
        """Live version of helper method. Proxies to segment.has_member_live(self)"""
        return segment.has_member_live(self)

    def refresh_segments(self):
        """
        Remove user from all segments, execute the SQL definition of all segments and add the user back into any
        segments to which the user belongs.

        Note that this executes the SQL of all segments and can be very slow. Not recommended for use
        """
        for s in Segment.objects.all():
            s.has_member_live(self)

    def refresh_segments_async(self):
        """
        Invokes SegmentMixin.refresh_segments via a celery task
        """
        from segments.tasks import refresh_user_segments
        refresh_user_segments.delay(self.pk)
