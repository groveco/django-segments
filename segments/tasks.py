from segments.models import Segment, SegmentExecutionError
from segments.helpers import SegmentHelper
from celery import task
import logging

logger = logging.getLogger(__name__)


@task(name='refresh_segments')
def refresh_segments():
    """Celery task to refresh all segments."""
    segments = list(Segment.objects.all())
    for s in segments:
        try:
            refresh_segment.delay(s.id)
        except SegmentExecutionError:
            logger.exception("SEGMENTS: Error refreshing segment id %s", s.id)


@task(name='refresh_segment')
def refresh_segment(segment_id):
    """Celery task to refresh an individual Segment."""
    try:
        s = Segment.objects.get(pk=segment_id)
        s.refresh()
    except Segment.DoesNotExist:
        logger.exception("SEGMENTS: Unable to refresh segment id %s. DoesNotExist.", segment_id)

@task(name='delete_segment')
def delete_segment(segment_id):
    """Celery task to delete an individual Segment from Redis """
    SegmentHelper().delete_segment(segment_id)
