import logging
from celery import Celery
from segments.app_settings import SEGMENTS_CELERY_QUEUE
from segments.helpers import SegmentHelper
from segments.models import Segment, SegmentExecutionError

logger = logging.getLogger(__name__)


app = Celery()
app.config_from_object('django.conf:settings', namespace='CELERY')


@app.task(queue=SEGMENTS_CELERY_QUEUE)
def refresh_segments():
    """Celery task to refresh all segments."""
    segments = list(Segment.objects.all())
    for s in segments:
        try:
            refresh_segment.delay(s.id)
        except SegmentExecutionError:
            logger.exception("SEGMENTS: Error refreshing segment id %s", s.id)


@app.task(queue=SEGMENTS_CELERY_QUEUE)
def refresh_segment(segment_id):
    """Celery task to refresh an individual Segment."""
    try:
        s = Segment.objects.get(pk=segment_id)
        s.refresh()
    except Segment.DoesNotExist:
        logger.exception("SEGMENTS: Unable to refresh segment id %s. DoesNotExist.", segment_id)


@app.task(queue=SEGMENTS_CELERY_QUEUE)
def delete_segment(segment_id):
    """Celery task to delete an individual Segment from Redis """
    SegmentHelper().delete_segment(segment_id)
