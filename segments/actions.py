def generate_refresh_action():

    def refresh(modeladmin, request, queryset):
            for s in queryset:
                s.refresh()
            modeladmin.message_user(request, 'Refreshed %s shipments' % len(queryset))

    refresh.short_description = 'Refresh these segments'
    return refresh