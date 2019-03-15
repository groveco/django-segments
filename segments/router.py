from segments.app_settings import SEGMENTS_EXEC_CONNECTION


class SegmentsRouter(object):

    """
    This router ensures that, given the recommended DB configuration of a readonly connection for
    settings.SEGMENTS_EXEC_CONNECTION, relations can be created between user objects returned from
    the segment's SQL definition (which is executed using SEGMENTS_EXEC_CONNECTION) and Segments.
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

    def allow_syncdb(self, db, model):
        return None
