from django.db import models, transaction, DatabaseError
from django.core.exceptions import ValidationError
from django.db.models.query_utils import InvalidQuery
from django.contrib.auth import get_user_model
from django.conf import settings
from django.db.models import signals
import app_settings
import logging

logger = logging.getLogger(__name__)


class SegmentExecutionError(Exception): pass

class Segment(models.Model):

    name = models.CharField(max_length=128)
    definition = models.TextField()  # will hold raw SQL query
    created_date = models.DateTimeField(auto_now_add=True)
    #description = models.CharField(blank=True, null=True)

    def has_member(self, user):
        return user in self.members.all()

    @property
    def members(self):
        """
        The ORM is smart enough to issue this as one query with a subquery
        """
        return get_user_model().objects.filter(id__in=self.member_set.all().values_list('user_id', flat=True))

    def has_member_live(self, user):
        """
        This issues the SQL synchronously to assess whether someone is a member of this segment.
        As with all 'live' methods in this library, these should be used at your own risk.
        They can be potentially very expensive, perform no caching, and perform no validation on the SQL.
        """
        return user in self.members_live

    @property
    def members_live(self):
        """
        Watch out! Executes live SQL with no safeguards.
        """
        return get_user_model().objects.db_manager(app_settings.SEGMENTS_CONNECTION_NAME).raw(self.definition)

    def clean(self):
        try:
            self.execute_definition()
        except SegmentExecutionError as e:
            raise ValidationError(e)

    def execute_definition(self):
        try:
            # Warning: This could get pretty big if the query is returning a lot of users
            # Calling list() on the queryset executes it (makes it non-lazy). This is necessary though, in order
            # to verify that the underlying SQL is in fact valid.
            return list(self.members_live)
        except InvalidQuery:
            raise SegmentExecutionError('SQL definition must include the primary key of the %s model' % settings.AUTH_USER_MODEL)
        except DatabaseError as e:
            raise SegmentExecutionError('Error while executing SQL definition: %s' % e)
        except Exception as e:
            raise SegmentExecutionError(e)

    def refresh(self):
        try:
            with transaction.atomic():
                self.flush()
                memberships = [SegmentMembership(user=u, segment=self) for u in self.execute_definition()]
                SegmentMembership.objects.bulk_create(memberships)
        except SegmentExecutionError as e:
            logger.exception("SEGMENTS: Error refreshing segment %s (id: %s): %s" % (self.name, self.id, e))
            raise e

    def flush(self):
        SegmentMembership.objects.filter(segment=self).delete()

    def __len__(self):
        return self.members.count()

    def __unicode__(self):
        return unicode(self.name)


def do_refresh(sender, instance, created, **kwargs):
    """
    Always refresh the segment if a new segment is being created.
    However if this is just a save, only refresh if the option is set. Some consumers may want to refresh only
    according to a cron schedule..
    """
    if created or app_settings.SEGMENTS_REFRESH_ON_SAVE:
        try:
            instance.refresh()
        except SegmentExecutionError:
            pass  # errors handled upstream
signals.post_save.connect(do_refresh, sender=Segment)


class SegmentMembership(models.Model):

    user = models.ForeignKey(settings.AUTH_USER_MODEL, related_name='segment_set')
    segment = models.ForeignKey(Segment, related_name='member_set')

    class Meta:
        unique_together = (('user', 'segment',),)


class SegmentMixin(object):

    @property
    def segments(self):
        return Segment.objects.filter(id__in=self.segment_set.all().values_list('segment_id', flat=True))

    def is_member(self, segment):
        return segment.has_member(self)

    #This is just too horrible to even make available. I should call it "shoot_database_in_head()"
    # @property
    # def segments_live(self):
    #     return [s for s in Segment.objects.all() if s.has_member_live(self)]

    def is_member_live(self, segment):
        """
        Watch out! Executes live SQL with no safeguards.
        """
        return segment.has_member_live(self)

    def refresh_segments(self):
        with transaction.atomic():
            self.flush_segments()
            memberships = []
            for s in Segment.objects.all():
                if self in s.execute_definition():
                    memberships.append(SegmentMembership(user=self, segment=s))
            SegmentMembership.objects.bulk_create(memberships)

    def flush_segments(self):
        SegmentMembership.objects.filter(user_id=self.id).delete()