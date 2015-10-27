from django.db import models, transaction, DatabaseError
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


class Segment(models.Model):

    """
    A segment, as defined by a SQL query. Segments are designed to be periodically refreshed, which populates
    an intermediate table of users (termed "members"), along with the segments they belong to. In other words,
    the SQL definition is not normally executed "live".

    There are a number of "live" methods on this class, and their corollaries on the SegmentMixin, all of which
    carry the following caveat:

    LIVE METHOD --  Use at your own risk. These methods perform the equivalent function of their non-live counterparts
    and are postfixed with _live. They execute the underlying segment definition SQL synchronously and perform no
    validation on that SQL. Therefore they can be slow, and they can fail. Finally because this is live,
    and most of the time consuming code will (presumably) be using the non-live version of these methods, mysterious
    bugs can crop of if code makes the assumption that the normal and _live version of these functions are equivalent.
    """

    name = models.CharField(max_length=128)
    definition = models.TextField(help_text="SQL query that returns IDs of users in the segment.")  # will hold raw SQL query
    created_date = models.DateTimeField(auto_now_add=True)

    def has_member(self, user):
        """
        Helper method. Return a bool indicating whether the user is a member of this segment.
        """
        return self.members.filter(id=user.id).exists()

    @property
    def members(self):
        """Return a queryset of all users (typed as settings.AUTH_USER_MODEL) that are members of this segment."""
        # The ORM is smart enough to issue this as one query with a subquery
        return get_user_model().objects.filter(id__in=self.member_set.all().values_list('user_id', flat=True))

    def has_member_live(self, user):
        """
        Live version of helper method. Return a bool indicating whether the user is a member of this segment.

        Also updates the computed segment membership accordingly.
        """
        exists = bool(list(get_user_model().objects.db_manager(app_settings.SEGMENTS_EXEC_CONNECTION).raw(self._wrap_sql_for_user(user))))
        if exists:
            SegmentMembership.objects.get_or_create(user=user, segment=self)
        else:
            SegmentMembership.filter(user=user, segment=self).delete()
        return exists

    def _wrap_sql_for_user(self, user):
        """
        Wraps the segment definition SQL to return a boolean
        :param user:
        :return:
        """
        return 'SELECT id FROM (%s) as temp WHERE id=%s' % (self.definition, user.id)

    @property
    def members_live(self):
        """
        Live version of .members. Issue SQL synchronously and return a raw queryset of all members.

        Refreshing a segment ultimately proxies to this method.
        """
        return get_user_model().objects.db_manager(app_settings.SEGMENTS_EXEC_CONNECTION).raw(self.definition)

    def clean(self):
        """Validate that the definition SQL will execute. Needed for proper error handling in the Django admin."""
        try:
            self.execute_definition()
        except SegmentExecutionError as e:
            raise ValidationError(e)

    def execute_definition(self):
        """
        Executes the underlying definition SQL (via .members_live) and returns a list of the users in the segment.

        This could get memory-hungry if the query is returning a lot of users. Calling list() on the queryset executes
        it (makes it non-lazy). This is necessary in order to verify that the underlying SQL is in fact valid.
        """
        try:
            return list(self.members_live)
        except InvalidQuery:
            raise SegmentExecutionError('SQL definition must include the primary key of the %s model' % settings.AUTH_USER_MODEL)
        except DatabaseError as e:
            raise SegmentExecutionError('Error while executing SQL definition: %s' % e)
        except Exception as e:
            raise SegmentExecutionError(e)

    def refresh(self):
        """Clear out old membership information, run the definition SQL, and create new SegmentMembership entries."""
        try:
            with transaction.atomic():
                self.flush()
                memberships = [SegmentMembership(user=u, segment=self) for u in self.execute_definition()]
                SegmentMembership.objects.bulk_create(memberships)
        except SegmentExecutionError as e:
            logger.exception("SEGMENTS: Error refreshing segment %s (id: %s): %s" % (self.name, self.id, e))
            raise e

    def flush(self):
        """Delete old segment membership data."""
        SegmentMembership.objects.filter(segment=self).delete()

    def __len__(self):
        """Calling len() on a segment returns the number of members of that segment."""
        return self.members.count()

    def __unicode__(self):
        return unicode(self.name)


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
            try:
                instance.refresh()
            except SegmentExecutionError:
                pass  # errors handled upstream
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
        with transaction.atomic():
            for s in Segment.objects.all():
                s.has_member_live(self)