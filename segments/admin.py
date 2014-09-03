from django.contrib import admin
from models import Segment


class SegmentAdmin(admin.ModelAdmin):
    list_display = ('name', 'members_field')

    def members_field(self, segment):
        return len(segment)

admin.site.register(Segment, SegmentAdmin)
