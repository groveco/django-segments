from django.contrib.auth import get_user_model
from segments import models
from segments.tests.models import SegmentableUser
import factory


def user_table():
    return get_user_model()._meta.db_table


class SegmentFactory(factory.DjangoModelFactory):
    name = "Segment 1"

    class Meta:
        model = models.Segment


class AllUserSegmentFactory(factory.DjangoModelFactory):
    name = "Segment 1"

    definition = "select * from %s" % user_table()

    class Meta:
        model = models.Segment


class UserFactory(factory.DjangoModelFactory):
    username = factory.Sequence(lambda n: "name{0}".format(n))

    class Meta:
        model = SegmentableUser
