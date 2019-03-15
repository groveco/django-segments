from django.contrib import admin
from django.contrib import messages
from segments.models import Segment
from segments import app_settings
from segments.tasks import refresh_segment


class SegmentAdmin(admin.ModelAdmin):

    """
    The Segment list view in the Django admin has a "refresh" action available to refresh the selected Segments.
    """

    prepopulated_fields = {"slug": ("name",)}
    list_display = ('name', 'priority', 'members_count', 'definition')
    readonly_fields = ('created_date', 'members_count', 'updated_date', 'recalculated_date')
    fields = ('name', 'slug', 'priority', 'members_count', 'definition', 'created_date', 'updated_date', 'recalculated_date')
    ordering = ('-priority',)

    actions = ('refresh',)

    def refresh(self, request, queryset):
        for s in queryset:
            if app_settings.SEGMENTS_REFRESH_ASYNC:
                refresh_segment.delay(s.id)
            else:
                s.refresh()
                self.message_user(request, 'Refreshed %s segments.' % len(queryset))

    def save_model(self, request, obj, form, change):
        if app_settings.SEGMENTS_REFRESH_ASYNC and (not change or app_settings.SEGMENTS_REFRESH_ON_SAVE):
            messages.add_message(request, messages.INFO, "Segment refresh started...")
        return super(SegmentAdmin, self).save_model(request, obj, form, change)


admin.site.register(Segment, SegmentAdmin)
