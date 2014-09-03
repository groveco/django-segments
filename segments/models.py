from django.db import models, transaction, DatabaseError
from django.core.exceptions import ValidationError
from django.db.models.query_utils import InvalidQuery
from django.contrib.auth import get_user_model
from django.conf import settings
from django.db.models import signals
import app_settings
import logging

logger = logging.getLogger(__name__)


class Segment(models.Model):

    name = models.CharField(max_length=128)
    definition = models.TextField()  # will hold raw SQL query
    created_date = models.DateTimeField(auto_now_add=True)

    #created_by = models.ForeignKey(settings.AUTH_USER_MODEL)
    #description = models.CharField(blank=true, null=True)

    def has_member(self, user):
        return user in self.members.all()

    def execute_definition(self):
        try:
            return list(get_user_model().objects.db_manager(app_settings.SEGMENTS_CONNECTION_NAME).raw(self.definition))
        except InvalidQuery:
            raise ValidationError('SQL definition must include the primary key of the %s model' % settings.AUTH_USER_MODEL)
        except DatabaseError as e:
            raise ValidationError('Error while executing SQL definition: %s' % e)
        except Exception as e:
            raise ValidationError(e)

    def refresh(self):
        try:
            with transaction.atomic():
                self.flush()
                memberships = [SegmentMembership(user=u, segment=self) for u in self.execute_definition()]
                SegmentMembership.objects.bulk_create(memberships)
        except DatabaseError as e:
            logger.exception(e)

    def flush(self):
        SegmentMembership.objects.filter(segment=self).delete()

    def __len__(self):
        return self.members.count()

    @property
    def members(self):
        """
        The ORM is smart enough to issue this as one query with a subquery
        """
        return get_user_model().objects.filter(id__in=self.member_set.all().values_list('user_id', flat=True))


def do_refresh(sender, instance, created, **kwargs):
    """
    Always refresh the segment if a new segment is being created.
    However if this is just a save, only refresh if the option is set. Some consumers may want to refresh only
    according to a cron schedule..
    """
    if created or app_settings.SEGMENTS_REFRESH_ON_SAVE:
        instance.refresh()
signals.post_save.connect(do_refresh, sender=Segment)


class SegmentMembership(models.Model):

    user = models.ForeignKey(settings.AUTH_USER_MODEL, related_name='segment_set')
    segment = models.ForeignKey(Segment, related_name='member_set')


class SegmentMixin(object):

    @property
    def segments(self):
        return Segment.objects.filter(id__in=self.segment_set.all().values_list('segment_id', flat=True))

    def is_member(self, segment):
        return segment.has_member(self)