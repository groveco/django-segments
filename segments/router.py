import app_settings


class SegmentsRouter(object):

    """
    This router ensures that, given the recommended DB configuration of a readonly connection for
    settings.SEGMENTS_EXEC_CONNECTION, relations can be created between user objects returned from
    the segment's SQL definition (which is executed using SEGMENTS_EXEC_CONNECTION) and SegmentMemberships.
    """

    def db_for_read(self, model, **hints):
        """
        No hint returned. Read-only routing to the SEGMENTS_EXEC_CONNECTION is handled manually in
        the Segment model. This is 'less surprising' than routing all Segment reads through the
        SEGMENTS_EXEC_CONNECTION.
        """
        return None

    def db_for_write(self, model, **hints):
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """
        If we're trying to create a relationship between two objects, one of which is a model
        from the Segments app, and at least one of which uses SEGMENTS_EXEC_CONNECTION, then allow it.
        Namely this targets Segment.refresh() where something like this happens:

            SegmentMembership(segment=s, user=u)

        Where 'u' is coming from a SEGMENTS_EXEC_CONNECTION queryset.
        """
        is_segments = obj1._meta.app_label == 'segments'
        is_conn = obj2._state.db == app_settings.SEGMENTS_EXEC_CONNECTION
        return True if is_segments and is_conn else None

    def allow_syncdb(self, db, model):
        return None