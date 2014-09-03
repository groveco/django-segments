from django.db import models, transaction, DatabaseError
from django.core.exceptions import ValidationError
from django.contrib.auth.models import User
import logging

logger = logging.getLogger(__name__)


class Segment(models.Model):

    name = models.CharField()
    definition = models.TextField()  # will hold raw SQL query
    created_by = models.ForeignKey(User)
    created_date = models.DateTimeField(auto_created=True)

    def clean(self):
        try:
            list(self.get_customers_from_sql())
        except:
            raise ValidationError('Sql definition is not valid')

    def user_belongs(self, user):
        return user in self.members.all()

    def live_segment(self):
        return User.objects.raw(self.definition)

    def refresh(self):
        try:
            with transaction.atomic():
                self.flush()
                for u in self.live_segment():
                    SegmentMembership.create(user=u, segment=self)
        except DatabaseError as e:
            logger.exception(e)

    def flush(self):
        SegmentMembership.filter(segment=self).delete()

    def __len__(self):
        return self.members.count()


class SegmentMixin(models.Model):

    segments = models.ManyToManyField(Segment, through=SegmentMembership, related_name='members')


class SegmentMembership(models.Model):

    user = models.ForeignKey(User)
    segment = models.ForeignKey(Segment)