import factory
import fakeredis
from celery import Celery
from django.db.models import signals
from django.test import TestCase, override_settings

from segments.helpers import SegmentHelper
from segments.models import Segment
from segments.tasks import refresh_segments, refresh_segment
from segments.tests.factories import (
    SegmentFactory,
    UserFactory,
    AllUserSegmentFactory,
    user_table,
)
import segments.app_settings
from mock import Mock, patch

# Make a celery app
test_celery_app = Celery()
test_celery_app.config_from_object("django.conf:settings", namespace="CELERY")


class TestTasks(TestCase):
    def setUp(self):
        helper = SegmentHelper(
            redis_obj=fakeredis.FakeStrictRedis(charset="utf-8", decode_responses=True)
        )
        Segment.helper = helper
        helper.redis.flushdb()

    @patch("segments.models.Segment.refresh")
    def test_refresh(self, mocked_segment):
        s1 = AllUserSegmentFactory()
        s1.refresh = Mock(return_value=True)
        mocked_segment.assert_called_once_with()

        refresh_segments()

        self.assertEqual(mocked_segment.call_count, 2)

    @override_settings(SEGMENTS_REFRESH_ON_SAVE=False)
    def test_refresh_handles_bad_queries(self):
        user = UserFactory()

        with factory.django.mute_signals(signals.post_save):
            s1 = SegmentFactory(definition="fail")
            s2 = SegmentFactory(
                definition="select %s from %s" % (user.pk, user_table())
            )

        refresh_segments()
        self.assertListEqual(list(Segment.helper.get_segment_members(s1.id)), [])
        self.assertListEqual(
            list(Segment.helper.get_segment_members(s2.id)), [str(user.pk)]
        )

    def test_refresh_existing_segment(self):
        segments.app_settings.SEGMENTS_REFRESH_ON_SAVE = True
        segments.app_settings.SEGMENTS_REFRES_ASYNC = False
        u1 = UserFactory()
        s = AllUserSegmentFactory()
        u2 = UserFactory()
        self.assertEqual(len(s), 1)
        s.refresh()
        self.assertEqual(len(s), 2)

    # Just making sure the logging code works
    def test_refresh_non_existing_segment(self):
        SegmentFactory(definition="SELECT 1;")
        bad_id = Segment.objects.order_by("pk").last().pk + 1
        with self.assertLogs(logger="segments.tasks", level="ERROR") as cm:
            refresh_segment(bad_id)  # bad ID
        self.assertIn(
            "SEGMENTS: Unable to refresh segment id %s. DoesNotExist." % bad_id,
            cm.output[0],
        )

    def test_delete_segment(self):
        user = UserFactory()
        segment = AllUserSegmentFactory()
        self.assertTrue(segment.has_member(user))

        segment.delete()
        self.assertFalse(segment.has_member(user))
