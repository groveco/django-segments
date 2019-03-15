from django.conf import settings

"""
Highly recommended you set SEGMENTS_EXEC_CONNECTION to a readonly DB connection.

If you are using the SEGMENTS_EXEC_CONNECTION setting, you must add this to your settings.py:
DATABASE_ROUTERS = ['segments.router.SegmentsRouter',]
"""
SEGMENTS_EXEC_CONNECTION = getattr(settings, 'SEGMENTS_EXEC_CONNECTION', 'default')
SEGMENTS_REDIS_URI = getattr(settings, 'SEGMENTS_REDIS_URI', None)
SEGMENTS_REFRESH_ON_SAVE = getattr(settings, 'SEGMENTS_REFRESH_ON_SAVE', True)
SEGMENTS_REFRESH_ASYNC = getattr(settings, 'SEGMENTS_REFRESH_ASYNC', False)
