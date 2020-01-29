import fakeredis
from django.test import TestCase
from segments.helpers import SegmentHelper, chunk_items, execute_raw_user_query
from segments.tests.factories import SegmentFactory, UserFactory, user_table
from mock import patch


class TestSegmentHelper(TestCase):

    def setUp(self):
        self.helper = SegmentHelper()
        self.user = UserFactory()
        SegmentHelper.redis = fakeredis.FakeStrictRedis(
            charset='utf-8',
            decode_responses=True)

    def test_add_segment_membership(self):
        s = SegmentFactory()
        s.add_member(self.user)
        self.assertTrue(self.helper.segment_has_member(s.id, self.user.id))

    def test_segment_has_member_when_segment_exists(self):
        s = SegmentFactory()
        s.add_member(self.user)
        self.assertTrue(self.helper.segment_has_member(s.id, self.user.id))

    def test_segment_has_member_nonexistant_segment(self):
        s = SegmentFactory()
        s.add_member(self.user)
        self.helper.remove_segment_membership(99999, self.user.id)
        self.assertTrue(self.helper.segment_has_member(s.id, self.user.id))

    def test_remove_segment_membership_segment_exists(self):
        s = SegmentFactory()
        s.add_member(self.user)
        self.helper.remove_segment_membership(s.id, self.user.id)
        self.assertFalse(self.helper.segment_has_member(s.id, self.user.id))

    def test_get_user_segments_when_segment_exists(self):
        s = SegmentFactory()
        s.add_member(self.user)
        segments = self.helper.get_user_segments(self.user.id)
        self.assertTrue(len(segments) > 0)

    def test_get_user_segments_when_invalid_user(self):
        s = SegmentFactory()
        s.add_member(self.user)
        segments = self.helper.get_user_segments(9999)
        self.assertEquals(len(segments), 0)

    def test_get_segment_membership_count(self):
        s = SegmentFactory()
        s.add_member(self.user)
        self.assertEquals(self.helper.get_segment_membership_count(s.id), 1)

    def test_get_segment_members_valid_segment(self):
        s = SegmentFactory()
        s.add_member(self.user)
        members = self.helper.get_segment_members(s.id)
        self.assertEquals(len(list(members)), 1)

    def test_get_segment_members_invalid_segment(self):
        s = SegmentFactory()
        s.add_member(self.user)
        members = self.helper.get_segment_members(99999)
        self.assertEquals(len(list(members)), 0)

    def test_get_refreshed_users(self):
        s = SegmentFactory()
        self.helper.refresh_segment(s.id, 'select %s from %s' % (self.user.pk, user_table()))
        self.assertEquals(len(list(self.helper.get_refreshed_users())), 1)

    def test_remove_refreshed_user(self):
        s = SegmentFactory()
        self.helper.refresh_segment(s.id, 'select %s from %s' % (self.user.pk, user_table()))
        self.helper.remove_refreshed_user(self.user.id)
        self.assertEquals(len(list(self.helper.get_refreshed_users())), 0)

    def test_refresh_segment_invalid_sql(self):
        s = SegmentFactory()
        invalid_sql = 'abc select '
        self.assertEquals(self.helper.refresh_segment(s.id, invalid_sql), 0)

    def test_refresh_segment_valid_sql(self):
        s = SegmentFactory()
        valid_sql = 'select * from %s' % user_table()
        self.assertEquals(self.helper.refresh_segment(s.id, valid_sql), 1)

    @patch('segments.tasks.delete_segment.delay')
    def test_delete_segment(self, p_delete_segment):
        s = SegmentFactory()
        s.add_member(self.user)
        s.delete()
        self.assertFalse(self.helper.segment_has_member(s.id, self.user.id))
        self.assertTrue(p_delete_segment.called)

    def test_diff_segment(self):
        s1 = SegmentFactory()
        u1 = UserFactory()
        s1.add_member(u1)
        s2 = SegmentFactory()
        u2 = UserFactory()
        s2.add_member(u1)
        s2.add_member(u2)

        s1_key = self.helper.segment_key % s1.id
        s2_key = self.helper.segment_key % s2.id
        self.helper.diff_segment(s2_key, s1_key, 'diff_test')
        self.assertEquals(self.helper.redis.smembers('diff_test'), {str(u2.id)})

    def test_chunk_items(self):
        members = [1, 2, 3]
        for i in members:
            self.assertEquals(len(list(chunk_items(members, len(members), i))[0]), i)
        self.assertEquals(len(list(chunk_items([], len(members), 1))[0]), 0)

    def test_raw_user_query(self):
        invalid = [
            '',
            None,
            12345,
            "select 'pretendemail'",
            "SELECT * FROM ( VALUES (0), (NULL),) as foo;",
            "SELECT * FROM ( VALUES (0), ('0'),) as foo;",
        ]
        for i in invalid:
            items = execute_raw_user_query(i)
            self.assertEquals(len(items), 0)

        valid_sql = 'select * from %s' % user_table()
        items = execute_raw_user_query(valid_sql)
        self.assertEquals(len(items), 1)
