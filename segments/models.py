import logging

from django.db import models, DatabaseError, OperationalError
from django.core.exceptions import FieldError
from django.conf import settings
from django.db.models import signals
from django.utils import timezone
from functools import wraps
from segments import app_settings
from segments.exceptions import SegmentExecutionError
from segments.helpers import SegmentHelper

logger = logging.getLogger(__name__)


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

        except FieldError:
            raise SegmentExecutionError(
                "SQL definition must include the primary key of the %s model"
                % settings.AUTH_USER_MODEL
            )

        except (DatabaseError, OperationalError) as e:
            raise SegmentExecutionError("Error while executing SQL definition: %s" % e)

        except Exception as e:
            raise SegmentExecutionError(e)

    return _wrapper


class Segment(models.Model):

    """
    A segment, as defined by a SQL query. Segments are designed to be stored in Redis and periodically refreshed.
    """

    name = models.CharField(max_length=128)
    slug = models.SlugField(max_length=256, null=True, blank=True, unique=True)
    definition = models.TextField(
        help_text="SQL query returning IDs of users in the segment.",
        blank=True,
        null=True,
    )
    priority = models.PositiveIntegerField(null=True, blank=True)
    members_count = models.PositiveIntegerField(null=True, blank=True, default=0)
    created_date = models.DateTimeField(auto_now_add=True)
    updated_date = models.DateTimeField(
        null=True, blank=True, db_index=True, auto_now=True
    )
    recalculated_date = models.DateTimeField(null=True, blank=True)

    helper = SegmentHelper()

    ############
    # Public API
    ############

    def has_member(self, user):
        """
        Helper method. Return a bool indicating whether the user is a member of this segment.
        """
        if not user.id:
            return False
        return self.helper.segment_has_member(self.id, user.id)

    def add_member(self, user):
        """ Helper method. Adds member to this segment. Returns a bool indicating the add status """
        if not user.id:
            return False
        return self.helper.add_segment_membership(self.id, user.id)

    @live_sql
    def refresh(self):
        members_count = self.helper.refresh_segment(self.id, self.definition)
        Segment.objects.select_for_update().filter(id=self.id).update(
            members_count=members_count, recalculated_date=timezone.now()
        )
        self.refresh_from_db()

    def __len__(self):
        """Calling len() on a segment returns the number of members of that segment."""
        return self.members_count

    # A lot of code that interfaces with Django models expects model instances to be
    # truthy, to distingiush them from `None`. Since we override `__len__`, `Segment`s
    # with no members will be incorrectly treated as non-existent.
    def __bool__(self):
        return True

    def __str__(self):
        return self.name


def do_refresh(sender, instance, created, **kwargs):
    """
    Connected to Segment's post_save signal.

    Always refresh the segment if a new segment is being created. If this is just a save, only refresh if the option
    is set. Some consumers may want to refresh only according to a cron schedule (ie. asynchronously) instead of on
    every segment save.
    """
    from segments.tasks import refresh_segment

    if app_settings.SEGMENTS_REFRESH_ON_SAVE:
        if app_settings.SEGMENTS_REFRESH_ASYNC:
            refresh_segment.delay(instance.id)
        else:
            instance.refresh()


signals.post_save.connect(do_refresh, sender=Segment)


def do_delete(sender, instance, *args, **kwargs):
    from segments.tasks import delete_segment

    delete_segment.delay(instance.id)


signals.post_delete.connect(do_delete, sender=Segment)


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
        return Segment.objects.filter(id__in=self.segment_ids).order_by("-priority")

    @property
    def segment_ids(self):
        """Return all the segment ids to which this member belongs."""
        return Segment.helper.get_user_segments(self.pk)

    def is_member(self, segment):
        """Helper method. Proxies to segment.has_member(self)"""
        return segment.has_member(self)
