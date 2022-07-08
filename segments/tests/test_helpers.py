import fakeredis
from django.db.models.query_utils import InvalidQuery
from django.db.utils import OperationalError
from django.test import TestCase

from segments.helpers import SegmentHelper
from segments.models import Segment
from segments.tests.factories import SegmentFactory, UserFactory, user_table
from mock import patch


class TestSegmentHelper(TestCase):
    def setUp(self):
        self.helper = SegmentHelper(
            redis_obj=fakeredis.FakeStrictRedis(encoding="utf-8", decode_responses=True)
        )

        Segment.helper = self.helper
        self.user = UserFactory()
        self.segment = SegmentFactory(definition="SELECT 0;")
        self.helper.redis.flushdb()

    def test_add_segment_membership(self):
        self.segment.add_member(self.user)
        self.assertTrue(self.helper.segment_has_member(self.segment.id, self.user.id))

    def test_segment_has_member_when_segment_exists(self):
        self.segment.add_member(self.user)
        self.assertTrue(self.helper.segment_has_member(self.segment.id, self.user.id))

    def test_get_user_segments_when_segment_exists(self):
        self.segment.add_member(self.user)
        segments = self.helper.get_user_segments(self.user.id)
        self.assertTrue(len(segments) > 0)

    def test_get_user_segments_when_invalid_user(self):
        self.segment.add_member(self.user)
        segments = self.helper.get_user_segments(9999)
        self.assertEquals(len(segments), 0)

    def test_get_segment_membership_count(self):
        self.segment.add_member(self.user)
        self.assertEquals(self.helper.get_segment_membership_count(self.segment.id), 1)

    def test_get_segment_members_valid_segment(self):
        self.segment.add_member(self.user)
        members = self.helper.get_segment_members(self.segment.id)
        self.assertEquals(len(list(members)), 1)

    def test_get_segment_members_invalid_segment(self):
        self.segment.add_member(self.user)
        members = self.helper.get_segment_members(99999)
        self.assertEquals(len(list(members)), 0)

    def test_get_refreshed_users(self):
        self.helper.refresh_segment(
            self.segment.id, "select %s from %s" % (self.user.pk, user_table())
        )
        self.assertEquals(len(list(self.helper.get_refreshed_users())), 1)

    def test_remove_refreshed_user(self):
        self.helper.refresh_segment(
            self.segment.id, "select %s from %s" % (self.user.pk, user_table())
        )
        self.helper.remove_refreshed_user(self.user.id)
        self.assertEquals(len(list(self.helper.get_refreshed_users())), 0)

    @patch("segments.helpers.logger")
    def test_refresh_segment_invalid_sql(self, mock_logger):
        invalid_sql = "abc select "
        self.assertEquals(self.helper.refresh_segment(self.segment.id, invalid_sql), 0)
        mock_logger.exception.assert_called_with(
            'SEGMENTS: refresh_segment(1, abc select ): near "abc": syntax error'
        )

    def test_refresh_segment_valid_sql(self):
        valid_sql = "select * from %s" % user_table()
        self.assertEquals(self.helper.refresh_segment(self.segment.id, valid_sql), 1)

    @patch("segments.tasks.delete_segment.delay")
    def test_delete_segment(self, p_delete_segment):
        self.segment.add_member(self.user)
        self.segment.delete()
        self.assertFalse(self.helper.segment_has_member(self.segment.id, self.user.id))
        self.assertTrue(p_delete_segment.called)


class TestExecuteQuery(TestCase):
    def setUp(self):
        self.helper = SegmentHelper(
            redis_obj=fakeredis.FakeStrictRedis(encoding="utf-8", decode_responses=True)
        )

        self.helper.redis.flushdb()

    def test_invalid_raw_user_query_raises_exception(self):
        empty_queries = ["", None, 1, True, "any string that does not contain s.elect"]
        for query in empty_queries:
            with self.assertRaises(InvalidQuery, msg=f'Passed query: "{query}"') as cm:
                generator = self.helper.execute_raw_user_query(query)
                for _ in generator:
                    pass

    def test_valid_query_returns_generator(self):
        user1 = UserFactory()
        user2 = UserFactory()
        valid_sql = "select id from %s" % user_table()
        items_generator = self.helper.execute_raw_user_query(valid_sql)
        self.assertSetEqual(
            set([user1.id, user2.id]), set([i for i in items_generator])
        )

    def test_returns_all_valid_values_and_logs_exception_for_invalid_results(self):
        values = ["1", "2", " 3 ", "not valid", "4", " 5 "]

        for value, expected in zip(values, [True, True, False, False, True, False]):
            self.assertIs(self.helper.is_valid_member_id(value), expected)

        query = "SELECT " + " UNION ALL SELECT ".join(f"'{v}'" for v in values)

        with self.assertLogs("segments.helpers", "ERROR") as cm:
            generator = self.helper.execute_raw_user_query(query)
            results = set(generator)

        self.assertEqual(len(cm.output), 3)
        for entry in cm.output:
            self.assertIn("Query returned invalid result: ", entry)

        self.assertSetEqual(set(["1", "2", "4"]), results)
