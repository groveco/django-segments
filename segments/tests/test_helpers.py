import fakeredis
from django.db.utils import OperationalError
from django.test import TestCase
from segments.helpers import SegmentHelper, execute_raw_user_query
from segments.models import Segment
from segments.tests.factories import SegmentFactory, UserFactory, user_table
from mock import patch


class TestSegmentHelper(TestCase):

    def setUp(self):
        self.user = UserFactory()
        self.helper = SegmentHelper(
            redis_obj=fakeredis.FakeStrictRedis(
                charset='utf-8',
                decode_responses=True
            )
        )

        self.helper.redis.flushdb()
        Segment.helper = self.helper

    def test_add_segment_membership(self):
        s = SegmentFactory()
        s.add_member(self.user)
        self.assertTrue(self.helper.segment_has_member(s.id, self.user.id))

    def test_segment_has_member_when_segment_exists(self):
        s = SegmentFactory()
        s.add_member(self.user)
        self.assertTrue(self.helper.segment_has_member(s.id, self.user.id))

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
        self.assertRaises(OperationalError, self.helper.refresh_segment, s.id, invalid_sql)

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

    def test_raw_user_query_returns_empty_list(self):
        empty_queries = [
            '',
            None,
            1,
            True,
            'any string that does not contain s.elect'
        ]
        for query in empty_queries:
            items_generator = execute_raw_user_query(query)
            self.assertEquals(sum(1 for _ in items_generator), 0)

        user = UserFactory()
        valid_sql = 'select id from %s' % user_table()
        items_generator = execute_raw_user_query(valid_sql)
        self.assertSetEqual(
            set([self.user.id, user.id]),
            set([i for i in items_generator])
        )
