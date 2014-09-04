from django.test import TestCase
from segments.tasks import refresh_segments, refresh_segment
from segments.tests.factories import SegmentFactory, UserFactory
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

    def test_refresh_existing_segment(self):
        UserFactory()
        s = SegmentFactory()
        UserFactory()
        self.assertEqual(len(s), 1)
        refresh_segment(s.id)
        self.assertEqual(len(s), 2)

    # Just making sure the logging code works
    def test_refresh_non_existing_segment(self):
        s = SegmentFactory()
        refresh_segment(s.id + 1)  #bad ID
        pass