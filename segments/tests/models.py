from django.contrib.auth.models import AbstractUser
from segments.models import SegmentMixin

class SegmentableUser(AbstractUser, SegmentMixin): pass