from django.test import TestCase
from segments.tests.factories import SegmentFactory, UserFactory, user_table
from segments import app_settings
from segments.models import SegmentMembership, SegmentExecutionError
from mock import Mock, patch


class TestSegment(TestCase):

    def setUp(self):
        self.u = UserFactory()

    def test_basic_segment(self):
        s = SegmentFactory()
        self.assertEqual(len(s), 1)

    def test_segment_invalid(self):
        try:
            s = SegmentFactory(definition='fail')
            self.fail()
        except SegmentExecutionError:
            pass

    def test_flush(self):
        s = SegmentFactory()
        self.assertEqual(1, SegmentMembership.objects.count())
        s._flush()
        self.assertEqual(0, SegmentMembership.objects.count())

    def test_segment_valid(self):
        s = SegmentFactory()
        self.assertEqual(len([u for u in s._execute_raw_user_query()]), 1)

    def test_user_belongs_to_segment(self):
        definition = 'select * from %s where id = %s' % (user_table(), self.u.id)
        s = SegmentFactory(definition=definition)
        self.assertTrue(s.has_member(self.u))

    def test_user_doesnt_belong_to_segment(self):
        definition = 'select * from %s where id != %s' % (user_table(), self.u.id)
        s = SegmentFactory(definition=definition)
        self.assertFalse(s.has_member(self.u))

    def test_user_belongs_to_segment_live(self):
        s = SegmentFactory()
        u2 = UserFactory()
        self.assertFalse(s.has_member(u2))
        self.assertTrue(s.has_member_live(u2))

    def test_has_members_live_saves_changes(self):
        s = SegmentFactory()
        u2 = UserFactory()
        self.assertFalse(s.has_member(u2))
        self.assertTrue(s.has_member_live(u2))
        self.assertTrue(s.has_member(u2))

    def test_segment_refresh(self):
        s = SegmentFactory()
        UserFactory()
        self.assertEqual(len(s), 1)
        s.refresh()
        self.assertEqual(len(s), 2)
        s.definition = 'select * from %s where id = %s' % (user_table(), self.u.id)
        self.assertEqual(len(s), 2)
        s.refresh()
        self.assertEqual(len(s), 1)

    def test_multiple_segments(self):
        SegmentFactory()
        s2 = SegmentFactory()
        self.assertEqual(len(s2), 1)

    def segment_flushed_during_reset(self):
        """
        Assert that new SegmentMembership objects are created after a refresh, even though the members themselves
        are the same.
        """
        s = SegmentFactory()

        orig_member_ids = s.member_set.all().values_list('id', flat=True)
        orig_members = s.members.all().values_list('id', flat=True)

        s.refresh()

        refreshed_member_ids = s.member_set.all().values_list('id', flat=True)
        refreshed_members = s.members.all().values_list('id', flat=True)

        self.assertNotEqual(set(orig_member_ids), set(refreshed_member_ids))
        self.assertEqual(set(orig_members), set(refreshed_members))

    def test_refresh_after_create(self):
        s = SegmentFactory.build()
        s.refresh = Mock()
        s.save()
        s.refresh.assert_called_with()

    def test_refresh_after_save(self):
        s = SegmentFactory()
        s.refresh = Mock()
        s.save()
        self.assertEqual(s.refresh.call_count, 1)

    def test_refresh_not_called_after_save_if_disabled(self):
        app_settings.SEGMENTS_REFRESH_ON_SAVE = False
        s = SegmentFactory()
        s.refresh = Mock()
        s.save()
        self.assertEqual(s.refresh.call_count, 0)
        app_settings.SEGMENTS_REFRESH_ON_SAVE = True

    @patch('segments.tasks.refresh_segment')
    def test_refresh_async_called_if_enabled(self, mocked_refresh):
        mocked_refresh.delay = Mock()
        app_settings.SEGMENTS_REFRESH_ASYNC = True
        SegmentFactory()
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
        s = SegmentFactory()
        s.refresh()
        self.assertEqual(s.members.count(), 1)
        app_settings.SEGMENTS_EXEC_CONNECTION = 'default'


class TestSegmentStatic(TestCase):

    def test_parse_static_ids(self):
        s = SegmentFactory(static_ids="12\na\n2.0\n1 \n  234234")
        self.assertListEqual(s._parsed_static_ids, [12,1,234234])

    def test_invalid_id_in_static_ids(self):
        s = SegmentFactory(static_ids="10")
        self.assertEqual(len(list(s.members_live)), 0)

    def test_dedupes_when_user_present_in_static_and_dynamic(self):
        u = UserFactory()
        definition = 'select * from %s where id = %s' % (user_table(), u.id)
        s = SegmentFactory(definition=definition, static_ids='%s' % u.id)
        self.assertEqual(len(list(s.members_live)), 1)

    def test_static_only_members(self):
        u = UserFactory()
        s = SegmentFactory(static_ids='%s' % u.id, definition=None)
        self.assertEqual(len(s), 1)

    def test_static_only_members_live(self):
        u = UserFactory()
        s = SegmentFactory.build(static_ids='%s' % u.id, definition=None)
        self.assertEqual(len(list(s.members_live)), 1)

    def test_static_only_has_member(self):
        u = UserFactory()
        s = SegmentFactory(static_ids='%s' % u.id, definition=None)
        self.assertTrue(s.has_member(u))

    def test_static_only_has_member_live(self):
        u = UserFactory()
        s = SegmentFactory.build(static_ids='%s' % u.id, definition=None)
        self.assertTrue(s.has_member_live(u))

    def test_static_and_dynamic_members(self):
        u = UserFactory()
        s = SegmentFactory(static_ids='foo\n123123')
        self.assertEqual(len(s), 1)

    def test_static_and_dynamic_members_live(self):
        u = UserFactory()
        s = SegmentFactory.build(static_ids='foo\n123123')
        self.assertEqual(len(list(s.members_live)), 1)

    def test_static_and_dynamic_has_member(self):
        u = UserFactory()
        s = SegmentFactory(static_ids='foo\n123123')
        self.assertTrue(s.has_member(u))

    def test_static_and_dynamic_has_member_live(self):
        u = UserFactory()
        s = SegmentFactory.build(static_ids='foo\n123123')
        self.assertTrue(s.has_member_live(u))


class TestMixin(TestCase):

    def setUp(self):
        self.u = UserFactory()
        self.s = SegmentFactory()

    def test_mixin_gives_fields(self):
        self.assertEqual(self.u.segments.count(), 1)
        self.assertEqual(self.u.segments.first(), self.s)

    def test_is_member(self):
        self.assertTrue(self.u.is_member(self.s))

    def test_is_member_live(self):
        u2 = UserFactory()
        self.assertFalse(u2.is_member(self.s))
        self.assertTrue(u2.is_member_live(self.s))

    def test_is_not_member(self):
        definition = 'select * from %s where id != %s' % (user_table(), self.u.id)
        s2 = SegmentFactory(definition=definition)
        self.assertFalse(self.u.is_member(s2))

    def test_refresh_memberships(self):
        u2 = UserFactory()
        self.assertEqual(u2.segments.count(), 0)
        u2.refresh_segments()
        self.assertEqual(u2.segments.count(), 1)