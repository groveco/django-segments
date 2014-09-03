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

    can_delete = False
    extra = 0
    editable_fields = []

    def get_readonly_fields(self, request, obj=None):
        return ['user', 'segment']

    def has_add_permission(self, request):
        return False

admin.site.register(Segment, SegmentAdmin)
