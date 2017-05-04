from django.contrib.auth.models import AbstractUser
from django.db.models import Manager
from segments.models import SegmentMixin


class SegmentableUserManager(Manager):

    def test_values_list(self):
        return self.all().values_list('id', flat=True)

    def test_filter(self):
        # this is 'double quoted' on purpose due to a known issue with SQLite
        # Should not be an issue with
        return self.filter(username="Chris").all()


class OtherSegmentableUserManager(Manager):

    def test_filter(self):
        return self.filter(username="Susan").all()


class SegmentableUser(AbstractUser, SegmentMixin):

    objects = SegmentableUserManager()
    special = OtherSegmentableUserManager()


related_names_for = ('groups', 'user_permissions')
for field_name in related_names_for:
    field = SegmentableUser._meta.get_field(field_name)
    field.rel.related_name = '+'

