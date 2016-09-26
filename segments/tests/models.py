from django.contrib.auth.models import AbstractUser
from segments.models import SegmentMixin

class SegmentableUser(AbstractUser, SegmentMixin):
    pass

related_names_for = ('groups', 'user_permissions')
for field_name in related_names_for:
    field = SegmentableUser._meta.get_field(field_name)
    field.rel.related_name = '+'

