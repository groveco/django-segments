from django.test import TestCase
from segments.tasks import refresh_segments
from segments.tests.factories import SegmentFactory
from mock import Mock, patch


class TestTasks(TestCase):

    @patch('segments.tasks.Segment.objects.all')
    def test_refresh(self, mocked_segment):
        s1 = SegmentFactory()
        s1.refresh = Mock(return_value=True)

        s2 = SegmentFactory()
        s2.refresh = Mock(return_value=True)

        mocked_segment.return_value = [s1, s2]

        refresh_segments()

        self.assertEqual(s1.refresh.call_count, 1)

    @patch('segments.tasks.Segment.objects.all')
    def test_refresh_handles_bad_queries(self, mocked_segment):
        s1 = SegmentFactory(definition="fail")
        s1.refresh = Mock(return_value=True)

        s2 = SegmentFactory()
        s2.refresh = Mock(return_value=False)

        mocked_segment.return_value = [s1, s2]

        refresh_segments()

        self.assertEqual(s1.refresh.call_count, 1)