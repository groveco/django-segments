from django.test import TestCase
from django.core.exceptions import ValidationError
from segments.tests.factories import SegmentFactory, UserFactory, user_table


class TestSegment(TestCase):

    def setUp(self):
        self.u = UserFactory()

    def test_basic_segment(self):
        s = SegmentFactory()
        self.assertEqual(len(s), 1)

    def test_segment_invalid(self):
        try:
            SegmentFactory(definition='fail')
            self.fail()
        except ValidationError:
            pass

    def test_segment_valid(self):
        s = SegmentFactory()
        self.assertEqual(len([u for u in s.execute_definition()]), 1)

    def test_user_belongs_to_segment(self):
        definition = 'select * from %s where id = %s' % (user_table(), self.u.id)
        s = SegmentFactory(definition=definition)
        self.assertTrue(s.user_belongs(self.u))

    def test_user_doesnt_belong_to_segment(self):
        definition = 'select * from %s where id != %s' % (user_table(), self.u.id)
        s = SegmentFactory(definition=definition)
        self.assertFalse(s.user_belongs(self.u))

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
        s.definition = 'select * from %s where id = %s' % (user_table(), self.u.id)

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


class TestMixin(TestCase):

    def test_mixin(self):
        u = UserFactory()
        s = SegmentFactory()
        self.assertEqual(u.segments.count(), 1)
        self.assertEqual(u.segments.first(), s)
