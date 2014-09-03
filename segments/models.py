from django.db import models, transaction, DatabaseError
from django.core.exceptions import ValidationError
from django.db.models.query_utils import InvalidQuery
from django.contrib.auth import get_user_model
from django.conf import settings
import app_settings
import logging

logger = logging.getLogger(__name__)


class Segment(models.Model):

    name = models.CharField(max_length=128)
    definition = models.TextField()  # will hold raw SQL query
    created_date = models.DateTimeField(auto_now_add=True)

    #created_by = models.ForeignKey(settings.AUTH_USER_MODEL)
    #description = models.CharField(blank=true, null=True)

    def user_belongs(self, user):
        return user in self.members.all()

    def execute_definition(self):
        try:
            return list(get_user_model().objects.db_manager(app_settings.SEGMENTS_CONNECTION_NAME).raw(self.definition))
        except InvalidQuery:
            raise ValidationError('SQL definition must include the primary key of the %s model' % settings.AUTH_USER_MODEL)
        except DatabaseError:
            raise ValidationError('Sql definition is not valid')
        except Exception as e:
            raise ValidationError(e)

    def refresh(self):
        try:
            with transaction.atomic():
                self.flush()
                for u in self.execute_definition():
                    SegmentMembership.objects.create(user=u, segment=self)
        except DatabaseError as e:
            logger.exception(e)

    def flush(self):
        SegmentMembership.objects.filter(segment=self).delete()

    def __len__(self):
        return self.members.count()

    @property
    def members(self):
        # There does not appear to be a way to do this in one query, without resorting to in-memory filtering
        return get_user_model().objects.filter(id__in=self.member_set.all().values_list('id', flat=True))


class SegmentMembership(models.Model):

    user = models.ForeignKey(settings.AUTH_USER_MODEL)
    segment = models.ForeignKey(Segment, related_name='member_set')


class SegmentMixin(object):

    @property
    def segments(self):
        return SegmentMembership.objects.filter(user_id=self.id)