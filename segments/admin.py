from django.contrib import admin
from django.contrib import messages
from segments.models import Segment, SegmentMembership
from segments import app_settings


class SegmentAdmin(admin.ModelAdmin):

    """
    The Segment list view in the Django admin has a "refresh" action available to refresh the selected Segments.
    """

    prepopulated_fields = {"slug": ("name",)}
    list_display = ('name', 'members_count', 'definition', 'content_type', 'manager_method')
    readonly_fields = ('created_date', 'members_count', 'static_users_sample', 'sql_users_sample')
    fields = ('name', 'slug', 'members_count', 'definition', 'content_type', 'manager_method',
              'sql_users_sample', 'static_ids', 'static_users_sample', 'created_date')

    def members_count(self, segment):
        return len(segment)

    actions = ('refresh',)

    def refresh(self, request, queryset):
        for s in queryset:
            s.refresh()
        self.message_user(request, 'Refreshed %s segments.' % len(queryset))

    def save_model(self, request, obj, form, change):
        if app_settings.SEGMENTS_REFRESH_ASYNC and (not change or app_settings.SEGMENTS_REFRESH_ON_SAVE):
            messages.add_message(request, messages.INFO, "Segment refresh started...")
        return super(SegmentAdmin, self).save_model(request, obj, form, change)


class SegmentMembershipInline(admin.TabularInline):

    """
    Add this to the inlines collection in your user ModelAdmin and all segment memberships will appear in the Django
    admin when viewing a user record.

    >>> class MyUserClassAdmin(admin.ModelAdmin):
    >>>     inlines = [SegmentMembershipInline,]
    """

    can_delete = False
    extra = 0
    editable_fields = []
    model = SegmentMembership

    def get_readonly_fields(self, request, obj=None):
        return ['user', 'segment']

    def has_add_permission(self, request):
        return False

admin.site.register(Segment, SegmentAdmin)
