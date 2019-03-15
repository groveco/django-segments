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
        self.redis = None

    def get_redis(self):
        if not self.redis:
            self.redis = redis.StrictRedis.from_url(app_settings.SEGMENTS_REDIS_URI)
        return self.redis

    def segment_has_member(self, segment_id, user_id):
        user_key = self.segment_member_key % user_id
        exists = False
        try:
            exists = self.get_redis().sismember(user_key, segment_id)
        except Exception as e:
            pass
        return exists

    def add_segment_membership(self, segment_id, user_id):
        user_key = self.segment_member_key % user_id
        try:
            self.get_redis().sadd(user_key, segment_id)
        except Exception as e:
            return False
        return True

    def remove_segment_membership(self, segment_id, user_id):
        user_key = self.segment_member_key % user_id
        try:
            self.get_redis().srem(user_key, segment_id)
        except Exception as e:
            return False
        return True

    def get_user_segments(self, user_id):
        user_key = self.segment_member_key % user_id
        items = []
        try:
            items = self.get_redis().smembers(user_key)
        except Exception as e:
            pass
        return items

    def get_segment_membership_count(self, segment_id):
        live_key = self.segment_key % segment_id
        return self.get_redis().scard(live_key)

    def get_segment_members(self, segment_id):
        live_key = self.segment_key % segment_id
        return self.get_redis().smembers_iter(live_key)

    def refresh_segment(self, segment_id, sql):
        live_key = self.segment_key % segment_id
        add_key = 'add_s:%s:' % segment_id
        new_key = 'new_s:%s:' % segment_id
        del_key = 'del_s:%s:' % segment_id

        redis = self.get_redis()

        # Run the SQL query and store the latest set members
        members, count = execute_raw_user_query(sql)
        for id_block in chunk_items(members, count, 10000):
            redis.sadd(add_key, *set(x[0] for x in id_block))

        # Store any new member adds
        self.diff_segment(add_key, live_key, new_key)

        # Store any member removals
        self.diff_segment(live_key, add_key, del_key)

        # Sync the current set members to the live set
        redis.sinterstore(live_key, add_key)

        # Sync the segment for new members
        for user_id in self.get_redis().sscan_iter(new_key):
            self.add_segment_membership(segment_id, user_id)

        # Sync the segment for deleted members
        for user_id in self.get_redis().sscan_iter(del_key):
            self.remove_segment_membership(segment_id, user_id)

        # Copy the new adds and deletes to the member changed list
        redis.sunionstore(new_key, self.segment_member_refresh_key, self.segment_member_refresh_key)
        redis.sunionstore(del_key, self.segment_member_refresh_key, self.segment_member_refresh_key)

        # Cleanup the sets
        for key in (add_key, del_key, new_key):
            redis.delete(key)

        # Set a one week expire on the refresh queue in case it's not of interest to the consumer
        redis.expire(self.segment_member_refresh_key, 604800)

        # Return the total number of members in this segment
        return redis.scard(live_key)

    def diff_segment(self, key_1, key_2, key_3):
        try:
            self.get_redis().sdiffstore(key_3, key_1, key_2)
        except Exception as e:
            pass


def chunk_items(items, length, chunk_size):
    for item in range(0, length, chunk_size):
        yield items[item:item + chunk_size]

def execute_raw_user_query(sql):
    """
    Helper that returns a RawQuerySet of user objects.
    """
    return get_user_model().objects.raw(sql).using(app_settings.SEGMENTS_EXEC_CONNECTION)

def execute_raw_user_query(sql):
    """
    Helper that returns a RawQuerySet of user objects.
    """
    with connections[app_settings.SEGMENTS_EXEC_CONNECTION].cursor() as cursor:
        try:
            count_sql = 'select count(*) from %s ' % sql.lower().split('from')[1]
            user_sql = 'select %s from %s' % (get_user_model()._meta.pk.name, sql.lower().split('from')[1])
            count = cursor.execute(count_sql).fetchone()
            count = count[0]
            result = cursor.execute(user_sql).fetchall()
            return [result, count]
        except Exception as e:
            logger.error('Error: segments user query error: %s' % e)

        return [[], 0]