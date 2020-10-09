import factory
import fakeredis
from django.db.models import signals
from django.test import TestCase

from segments.helpers import SegmentHelper
from segments.tests.factories import SegmentFactory, UserFactory, user_table, AllUserSegmentFactory
from segments import app_settings
from segments.models import SegmentExecutionError, Segment
from mock import Mock, patch


class TestSegment(TestCase):
    databases = '__all__'

    def setUp(self):
        self.u = UserFactory()
        Segment.helper = SegmentHelper(
            redis_obj=fakeredis.FakeStrictRedis(
                charset='utf-8',
                decode_responses=True
            )
        )

    def test_basic_segment(self):
        s = AllUserSegmentFactory()
        self.assertEqual(len(s), 1)

    def test_segment_invalid(self):
        try:
            s = SegmentFactory(definition='fail')
        except SegmentExecutionError:
            pass

    def test_user_belongs_to_segment(self):
        definition = 'select * from %s where id = %s' % (user_table(), self.u.id)
        s = SegmentFactory(definition=definition)
        s.refresh()
        self.assertTrue(s.has_member(self.u))

    def test_user_doesnt_belong_to_segment(self):
        definition = 'select * from %s where id != %s' % (user_table(), self.u.id)
        s = SegmentFactory(definition=definition)
        self.assertFalse(s.has_member(self.u))

    def test_segment_refresh(self):
        s = AllUserSegmentFactory()
        UserFactory()
        s.refresh()
        self.assertEqual(len(s), 2)

        # Change up the segment to only match one user
        s.definition = 'select * from %s where id = %s limit 1' % (user_table(), self.u.id)
        s.save()
        self.assertEqual(len(s), 1)
        s.refresh()
        self.assertEqual(len(s), 1)

        # Add a 3rd user, should still only store one user
        u3 = UserFactory()
        s.refresh()
        self.assertEqual(len(s), 1)

        # Expand the definition to include 3 users again
        s.definition = 'select * from %s' % (user_table())
        s.save()
        s.refresh()
        self.assertEqual(len(s), 3)

        # Remove one user
        u3.delete()
        s.refresh()
        self.assertEqual(len(s), 2)

    def test_multiple_segments(self):
        s1 = AllUserSegmentFactory()
        s2 = AllUserSegmentFactory()
        self.assertEqual(len(s2), 1)

    def test_refresh_after_create(self):
        s = AllUserSegmentFactory.build()
        s.refresh = Mock()
        s.save()
        s.refresh.assert_called_with()

    def test_refresh_after_save(self):
        s = AllUserSegmentFactory()
        s.refresh = Mock()
        s.save()
        self.assertEqual(s.refresh.call_count, 1)

    def test_refresh_not_called_after_save_if_disabled(self):
        app_settings.SEGMENTS_REFRESH_ON_SAVE = False
        s = AllUserSegmentFactory()
        s.refresh = Mock()
        s.save()
        self.assertEqual(s.refresh.call_count, 0)
        app_settings.SEGMENTS_REFRESH_ON_SAVE = True

    @patch('segments.tasks.refresh_segment')
    def test_refresh_async_called_if_enabled(self, mocked_refresh):
        mocked_refresh.delay = Mock()
        app_settings.SEGMENTS_REFRESH_ASYNC = True
        AllUserSegmentFactory()
        self.assertEqual(mocked_refresh.delay.call_count, 1)
        app_settings.SEGMENTS_REFRESH_ASYNC = False

    def test_multiple_dbs(self):
        """
        This seems crazy, but it's the only way to get test coverage on this in the test environment.
        For instance Segments is using a SEGMENTS_CONNECT_NAME of 'readonly' vs. 'default' everywhere else
        in the application.

        There's no way to actually set that up with a sqlite test DB, so we simulate it here by explicitly
        creating a "mirror" user object directly in the second database.
        """
        from segments.tests.models import SegmentableUser
        app_settings.SEGMENTS_EXEC_CONNECTION = 'other'
        SegmentableUser.objects.using(app_settings.SEGMENTS_EXEC_CONNECTION).create()
        s = AllUserSegmentFactory()
        s.refresh()
        self.assertEqual(len(s), 1)
        app_settings.SEGMENTS_EXEC_CONNECTION = 'default'


class TestMixin(TestCase):

    def setUp(self):
        Segment.helper = SegmentHelper(
            redis_obj=fakeredis.FakeStrictRedis(
                charset='utf-8',
                decode_responses=True
            )
        )
        self.u = UserFactory()
        self.s = AllUserSegmentFactory()
        app_settings.SEGMENTS_REFRESH_ASYNC = False
        app_settings.SEGMENTS_REFRESH_ON_SAVE = True

    def test_mixin_gives_fields(self):
        self.assertEqual(self.u.segments.count(), 1)
        self.assertEqual(self.u.segments.first(), self.s)

        # create non active segment
        AllUserSegmentFactory(is_active=False)
        self.assertEqual(self.u.segments.count(), 1)
        
        # create active segment
        AllUserSegmentFactory()
        self.assertEqual(self.u.segments.count(), 2)

    def test_is_member(self):
        self.assertTrue(self.u.is_member(self.s))

    def test_is_not_member(self):
        definition = 'select * from %s where id != %s' % (user_table(), self.u.id)
        s2 = SegmentFactory(definition=definition)
        self.assertFalse(self.u.is_member(s2))
