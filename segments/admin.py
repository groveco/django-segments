from django.contrib import admin
from segments.models import Segment, SegmentMembership
from segments.actions import generate_refresh_action


class SegmentAdmin(admin.ModelAdmin):
    list_display = ('name', 'members_count')
    readonly_fields = ('created_date', 'members_count',)

    def members_count(self, segment):
        return len(segment)

    actions = [generate_refresh_action()]


class SegmentMembershipInline(admin.TabularInline):
    model = SegmentMembership
    extra = 0

admin.site.register(Segment, SegmentAdmin)
