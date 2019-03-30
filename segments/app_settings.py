from django.conf import settings

"""
Highly recommended you set SEGMENTS_EXEC_CONNECTION to a readonly DB connection.
"""
SEGMENTS_EXEC_CONNECTION = getattr(settings, 'SEGMENTS_EXEC_CONNECTION', 'default')
SEGMENTS_REDIS_URI = getattr(settings, 'SEGMENTS_REDIS_URI', None)
SEGMENTS_REFRESH_ON_SAVE = getattr(settings, 'SEGMENTS_REFRESH_ON_SAVE', True)
SEGMENTS_REFRESH_ASYNC = getattr(settings, 'SEGMENTS_REFRESH_ASYNC', False)
SEGMENTS_CELERY_QUEUE = getattr(settings, 'SEGMENTS_CELERY_QUEUE', 'default')
