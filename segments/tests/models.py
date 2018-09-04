from django.contrib.auth.base_user import AbstractBaseUser
from django.db import models
from segments.models import SegmentMixin


class SegmentableUserManager(models.Manager):

    def test_values_list(self):
        return self.all().values_list('id', flat=True)

    def test_filter(self):
        # this is 'double quoted' on purpose due to a known issue with SQLite
        # Should not be an issue with
        return self.filter(username="Chris").all()


class OtherSegmentableUserManager(models.Manager):

    def test_filter(self):
        return self.filter(username="Susan").all()


class SegmentableUser(AbstractBaseUser, SegmentMixin):

    username = models.CharField(max_length=150, unique=True,)
    email = models.EmailField(blank=True)

    EMAIL_FIELD = 'email'
    USERNAME_FIELD = 'username'

    objects = SegmentableUserManager()
    special = OtherSegmentableUserManager()