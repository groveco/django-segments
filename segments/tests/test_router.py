from django.test import TestCase
from segments.router import SegmentsRouter
from segments import app_settings
from mock import Mock


class TestRouter(TestCase):

    def test_allow_relation(self):
        app_settings.SEGMENTS_EXEC_CONNECTION = 'foo'
        router = SegmentsRouter()

        obj1 = Mock()
        obj1._meta.app_label = 'segments'
        obj2 = Mock()
        obj2._state.db = app_settings.SEGMENTS_EXEC_CONNECTION

        self.assertTrue(router.allow_relation(obj1, obj2))

        obj1._meta.app_label = 'tests'
        self.assertFalse(router.allow_relation(obj1, obj2))

        app_settings.SEGMENTS_EXEC_CONNECTION = 'default'