import logging
import redis

from django.contrib.auth import get_user_model
from django.db import connections
from segments import app_settings

logger = logging.getLogger(__name__)


class SegmentHelper(object):
    segment_key = 's:%s'
    segment_member_key = 'sm:%s'
    segment_member_refresh_key = 'sm:refresh'

    def __init__(self):
        self.__redis = None

    @property
    def redis(self):
        if not self.__redis:
            self.__redis = redis.StrictRedis.from_url(
                    app_settings.SEGMENTS_REDIS_URI,
                    charset='utf-8',
                    decode_responses=True)
        return self.__redis

    def segment_has_member(self, segment_id, user_id):
        user_key = self.segment_member_key % user_id
        exists = False
        try:
            exists = self.redis.sismember(user_key, segment_id)
        except Exception as e:
            logger.exception('SEGMENTS: segment_has_member(%s, %s): %s' % (segment_id, user_id, e))
        return exists

    def add_segment_membership(self, segment_id, user_id):
        user_key = self.segment_member_key % user_id
        live_key = self.segment_key % segment_id
        try:
            self.redis.sadd(user_key, segment_id)
            self.redis.sadd(live_key, user_id)
            self.redis.sadd(self.segment_member_refresh_key, user_id)
        except Exception as e:
            logger.exception('SEGMENTS: add_segment_membership(%s, %s): %s' % (segment_id, user_id, e))
            return False
        return True

    def remove_segment_membership(self, segment_id, user_id):
        user_key = self.segment_member_key % user_id
        live_key = self.segment_key % segment_id
        try:
            self.redis.srem(user_key, segment_id)
            self.redis.srem(live_key, user_id)
            self.redis.sadd(self.segment_member_refresh_key, user_id)
        except Exception as e:
            logger.exception('SEGMENTS: remove_segment_membership(%s, %s): %s' % (segment_id, user_id, e))
            return False
        return True

    def get_user_segments(self, user_id):
        user_key = self.segment_member_key % user_id
        items = []
        try:
            items = self.redis.smembers(user_key)
        except Exception as e:
            logger.exception('SEGMENTS: get_user_segments(%s): %s' % (user_id, e))
        return items

    def get_segment_membership_count(self, segment_id):
        live_key = self.segment_key % segment_id
        return self.redis.scard(live_key)

    def get_segment_members(self, segment_id):
        live_key = self.segment_key % segment_id
        return self.redis.sscan_iter(live_key)

    def get_refreshed_users(self):
        try:
            return self.redis.sscan_iter(self.segment_member_refresh_key)
        except Exception as e:
            return None

    def remove_refreshed_user(self, user_id):
        try:
            self.redis.srem(self.segment_member_refresh_key, user_id)
        except Exception as e:
            return None

    def refresh_segment(self, segment_id, sql):
        live_key = self.segment_key % segment_id
        add_key = 'add_s:%s:' % segment_id
        new_key = 'new_s:%s:' % segment_id
        del_key = 'del_s:%s:' % segment_id

        # Run the SQL query and store the latest set members
        members, count = execute_raw_user_query(sql)
        for id_block in chunk_items(members, count, 10000):
            self.redis.sadd(add_key, *set(x[0] for x in id_block))

        # Store any new member adds
        self.diff_segment(add_key, live_key, new_key)

        # Store any member removals
        self.diff_segment(live_key, add_key, del_key)

        # Sync the current set members to the live set
        self.redis.sinterstore(live_key, add_key)

        # Sync the segment for new members
        for user_id in self.redis.sscan_iter(new_key):
            self.add_segment_membership(segment_id, user_id)

        # Sync the segment for deleted members
        for user_id in self.redis.sscan_iter(del_key):
            self.remove_segment_membership(segment_id, user_id)

        # Cleanup the sets
        for key in (add_key, del_key, new_key):
            self.redis.delete(key)

        # Set a one week expire on the refresh queue in case it's not of interest to the consumer
        self.redis.expire(self.segment_member_refresh_key, 604800)

        # Return the total number of members in this segment
        return self.redis.scard(live_key)

    def delete_segment(self, segment_id):
        segment_key = self.segment_key % segment_id
        for user_id in self.redis.sscan_iter(segment_key):
            self.remove_segment_membership(segment_id, user_id)
        self.redis.delete(segment_key)

    def diff_segment(self, key_1, key_2, key_3):
        try:
            self.redis.sdiffstore(key_3, key_1, key_2)
        except Exception as e:
            logger.exception('SEGMENTS: diff_segment(%s, %s, %s): %s' % (key_1, key_2, key_3, e))


def chunk_items(items, length, chunk_size):
    for item in range(0, length, chunk_size):
        yield items[item:item + chunk_size]

def execute_raw_user_query(sql):
    """
    Helper that returns an array containing a RawQuerySet of user ids and their total count.
    """
    if sql is None or not type(sql) == str or 'select' not in sql.lower():
        return [[], 0]

    with connections[app_settings.SEGMENTS_EXEC_CONNECTION].cursor() as cursor:
        # Fetch the raw queryset of ids and count them
        logger.info('SEGMENTS user query running: %s' % sql)
        cursor.execute(sql)
        result = cursor.fetchall() or []
        total = len(result)

        # Guardrail: Try to ensure results are integers
        if total > 0 and result[0]:
            if not type(result[0][0]) == int:
                raise RuntimeError('Query returned non-integer results')

        return [result, total]

