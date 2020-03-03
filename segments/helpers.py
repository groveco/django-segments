import logging
import redis

from django.db import connections
from segments import app_settings

logger = logging.getLogger(__name__)

REDIS_SSCAN_COUNT = app_settings.SEGMENTS_REDIS_SSCAN_COUNT
BATCH_SIZE = app_settings.SEGMENTS_REDIS_PIPELINE_BATCH_SIZE


class SegmentHelper(object):
    segment_key = 's:%s'
    segment_member_key = 'sm:%s'
    segment_member_refresh_key = 'sm:refresh'

    def __init__(self, redis_obj=None):
        self.__redis = redis_obj

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
        self.run_pipeline(
            iterable=(user_id for user_id in self.execute_raw_user_query(sql=sql) if user_id is not None),
            operation=lambda pipeline, user_id: pipeline.sadd(add_key, user_id)
        )

        # Store any new member adds
        self.redis.sdiffstore(
            dest=new_key,
            keys=[add_key, live_key]
        )

        # Store any member removals
        self.redis.sdiffstore(
            dest=del_key,
            keys=[live_key, add_key]
        )

        # Sync the current set members to the live set
        self.redis.sinterstore(
            dest=live_key,
            keys=[add_key]
        )

        # Sync the segment for new members
        self.redis.sunionstore(
            dest=self.segment_member_refresh_key,
            keys=[self.segment_member_refresh_key, new_key]
        )

        # Add segment id to each member's sets
        self.run_pipeline(
            iterable=self.redis.sscan_iter(new_key, count=REDIS_SSCAN_COUNT),
            operation=lambda pipeline, user_id: pipeline.sadd(self.segment_member_key % user_id, segment_id)
        )

        # Sync the segment for deleted members
        self.redis.sunionstore(
            dest=self.segment_member_refresh_key,
            keys=[self.segment_member_refresh_key, del_key]
        )

        # Remove segment id from member's sets
        self.run_pipeline(
            iterable=self.redis.sscan_iter(del_key, count=REDIS_SSCAN_COUNT),
            operation=lambda pipeline, user_id: pipeline.srem(self.segment_member_key % user_id, segment_id)
        )

        # Cleanup the sets
        self.redis.delete(add_key, del_key, new_key)

        # Set a one week expire on the refresh queue in case it's not of interest to the consumer
        self.redis.expire(self.segment_member_refresh_key, 604800)

        # Return the total number of members in this segment
        return self.redis.scard(live_key)

    def delete_segment(self, segment_id):
        segment_key = self.segment_key % segment_id

        # Add all segment users to refreshed users set
        self.redis.sunionstore(
            dest=self.segment_member_refresh_key,
            keys=[self.segment_member_refresh_key, segment_key]
        )

        # Remove segment id from member's sets
        self.run_pipeline(
            iterable=self.redis.sscan_iter(segment_key, count=REDIS_SSCAN_COUNT),
            operation=lambda pipeline, user_id: pipeline.srem(self.segment_member_key % user_id, segment_id)
        )

        self.redis.delete(segment_key)

    def run_pipeline(self, iterable, operation=lambda pipeline, user_id: None):
        with self.redis.pipeline(transaction=False) as pipeline:
            for user_id in iterable:
                operation(pipeline, user_id)
                if len(pipeline) >= BATCH_SIZE:
                    pipeline.execute()
            pipeline.execute()

    def is_valid_member_id(self, value):
        if isinstance(value, int):
            return True

        if isinstance(value, str) and value.isdigit():
            return True

        logger.info(f'SEGMENTS: {value} is not valid member id')
        return False

    def execute_raw_user_query(self, sql):
        """
        Helper that returns an array containing a RawQuerySet of user ids and their total count.
        """
        if sql is None or not type(sql) == str or 'select' not in sql.lower():
            return

        with connections[app_settings.SEGMENTS_EXEC_CONNECTION].cursor() as cursor:
            # Fetch the raw queryset of ids and count them
            logger.info('SEGMENTS user query running: %s' % sql)
            cursor.execute(sql)

            chunk = 1  # just need for 1st iteration
            while chunk:
                chunk = cursor.fetchmany(size=app_settings.SEGMENTS_CURSOR_FETCHMANY_SIZE)
                for row in chunk:
                    if self.is_valid_member_id(row[0]):
                        yield row[0]
