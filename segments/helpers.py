import logging
import redis

from django.db import connections
from django.core.exceptions import FieldDoesNotExist

from segments import app_settings

logger = logging.getLogger(__name__)

REDIS_SSCAN_COUNT = app_settings.SEGMENTS_REDIS_SSCAN_COUNT
BATCH_SIZE = app_settings.SEGMENTS_REDIS_PIPELINE_BATCH_SIZE


class SegmentHelper(object):
    segment_key = "s:%s"
    segment_member_key = "sm:%s"
    segment_member_refresh_key = "sm:refresh"

    def __init__(self, redis_obj=None):
        self.__redis = redis_obj

    @property
    def redis(self):
        if not self.__redis:
            self.__redis = redis.StrictRedis.from_url(
                app_settings.SEGMENTS_REDIS_URI, charset="utf-8", decode_responses=True
            )
        return self.__redis

    def segment_has_member(self, segment_id, user_id):
        user_key = self.segment_member_key % user_id
        exists = False
        try:
            exists = self.redis.sismember(user_key, segment_id)
        except Exception as e:
            logger.exception(
                "SEGMENTS: segment_has_member(%s, %s): %s" % (segment_id, user_id, e)
            )
        return exists

    def add_segment_membership(self, segment_id, user_id):
        user_key = self.segment_member_key % user_id
        live_key = self.segment_key % segment_id
        try:
            self.redis.sadd(user_key, segment_id)
            self.redis.sadd(live_key, user_id)
            self.redis.sadd(self.segment_member_refresh_key, user_id)
        except Exception as e:
            logger.exception(
                "SEGMENTS: add_segment_membership(%s, %s): %s"
                % (segment_id, user_id, e)
            )
            return False
        return True

    def get_user_segments(self, user_id):
        user_key = self.segment_member_key % user_id
        items = []
        try:
            items = self.redis.smembers(user_key)
        except Exception as e:
            logger.exception("SEGMENTS: get_user_segments(%s): %s" % (user_id, e))
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
        except Exception:
            return None

    def remove_refreshed_user(self, user_id):
        try:
            self.redis.srem(self.segment_member_refresh_key, user_id)
        except Exception:
            return None

    """
    O(N) is 2E + 4U + 2R + 3Ndiff + 3Ldiff
    E is the number of customers in the existing segment
    U is the number in the updated segment
    R is the number of users in the refresh key (which gets flushed with segments_customer_sync_sentry)
    Ndiff is the number of users NEW to the segment (in the updated segment but not the old segment)
    Ldiff is the number of users LEAVING the segment (in the old segment but not the updated version)
    """

    def refresh_segment(self, segment_id, sql):
        live_key = self.segment_key % segment_id
        add_key = "add_s:%s:" % segment_id
        new_key = "new_s:%s:" % segment_id
        del_key = "del_s:%s:" % segment_id

        try:
            # Run the SQL query and store the latest set members
            # sadd is O(1) for each elem added, so O(U) to add U customers in the updated segment
            # running total U
            self.run_pipeline(
                iterable=(
                    user_id
                    for user_id in self.execute_raw_user_query(sql=sql)
                    if user_id is not None
                ),
                operation=lambda pipeline, user_id: pipeline.sadd(add_key, user_id),
            )

            # Store any new member adds
            # sdiffstore O(N) where N is the total number of elements in all given sets.
            # so O(E+U) where E num existing users in segment and U is num users in updated segment
            # running total E + 2U
            self.redis.sdiffstore(dest=new_key, keys=[add_key, live_key])

            # Store any member removals
            # sdiffstore O(N) where N is the total number of elements in all given sets.
            # so O(E+U) where E num existing users in segment and U is num users in updated segment
            # running total 2E + 3U
            self.redis.sdiffstore(dest=del_key, keys=[live_key, add_key])

            # Sync the current set members to the live set
            #  O(N*M) worst case where N is the cardinality of the smallest set and M is the number of sets.
            # so O(U) where U is num users in updated segment (wonder if this could be changed to repointing live_key at add_key)
            # running total 2E + 4U
            self.redis.sinterstore(dest=live_key, keys=[add_key])

            # Sync the segment for new members
            # Time complexity: O(N) where N is the total number of elements in all given sets.
            # O(R+Ndiff), R = num users needing to be refreshed, Ndiff is # of users new to the segment (in updated but not old)
            # Running total: 2E + 4U + R + Ndiff
            self.redis.sunionstore(
                dest=self.segment_member_refresh_key,
                keys=[self.segment_member_refresh_key, new_key],
            )

            # Add segment id to each member's sets
            # sscan: O(1) for every call. O(N) for a complete iteration, including enough command calls for
            # the cursor to return back to 0. N is the number of elements inside the collection.
            # so O(Ndiff) where Ndiff is the number of users new to the segment. not sure how many command calls to return cursor to 0.
            # Running total: 2E + 4U + R + 2Ndiff

            # sadd is O(1) for each elem added, so O(Ndiff) to add Ndiff customers new to the segment
            # Running total: 2E + 4U + R + 3Ndiff
            self.run_pipeline(
                iterable=self.redis.sscan_iter(new_key, count=REDIS_SSCAN_COUNT),
                operation=lambda pipeline, user_id: pipeline.sadd(
                    self.segment_member_key % user_id, segment_id
                ),
            )

            # Sync the segment for deleted members
            # Time complexity: O(N) where N is the total number of elements in all given sets.
            # O(R+Ldiff), R = num users needing to be refreshed, Ldiff is # of users leaving the segment
            # Running total: 2E + 4U + 2R + 3Ndiff + Ldiff
            self.redis.sunionstore(
                dest=self.segment_member_refresh_key,
                keys=[self.segment_member_refresh_key, del_key],
            )

            # Remove segment id from member's sets
            # sscan: O(1) for every call. O(N) for a complete iteration, including enough command calls for
            # the cursor to return back to 0. N is the number of elements inside the collection.
            # so O(Ldiff), Ldiff is the number of users leaving the segment. not sure how many command calls to return cursor to 0.
            # Running total: 2E + 4U + 2R + 3Ndiff + 2Ldiff

            # sadd is O(1) for each elem added, so O(Ldiff) total
            # Running total: 2E + 4U + 2R + 3Ndiff + 3Ldiff
            self.run_pipeline(
                iterable=self.redis.sscan_iter(del_key, count=REDIS_SSCAN_COUNT),
                operation=lambda pipeline, user_id: pipeline.srem(
                    self.segment_member_key % user_id, segment_id
                ),
            )
        except Exception as e:
            logger.exception(f"SEGMENTS: refresh_segment({segment_id}, {sql}): {e}")
        finally:
            # Cleanup the sets
            # O(1) for each key, O(3)
            self.redis.delete(add_key, del_key, new_key)

            # Set a one week expire on the refresh queue in case it's not of interest to the consumer
            # O(1)
            self.redis.expire(self.segment_member_refresh_key, 604800)

            # Return the total number of members in this segment
            # O(1)
            return self.redis.scard(live_key)

    def delete_segment(self, segment_id):
        segment_key = self.segment_key % segment_id

        # Add all segment users to refreshed users set
        self.redis.sunionstore(
            dest=self.segment_member_refresh_key,
            keys=[self.segment_member_refresh_key, segment_key],
        )

        # Remove segment id from member's sets
        self.run_pipeline(
            iterable=self.redis.sscan_iter(segment_key, count=REDIS_SSCAN_COUNT),
            operation=lambda pipeline, user_id: pipeline.srem(
                self.segment_member_key % user_id, segment_id
            ),
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

        logger.info(f"SEGMENTS: {value} is not valid member id")
        return False

    def execute_raw_user_query(self, sql):
        """
        Helper that returns an array containing a RawQuerySet of user ids and their total count.
        """
        if sql is None or not isinstance(sql, str) or "select" not in sql.lower():
            raise FieldDoesNotExist

        with connections[app_settings.SEGMENTS_EXEC_CONNECTION].cursor() as cursor:
            # Fetch the raw queryset of ids and count them
            logger.info("SEGMENTS user query running: %s" % sql)
            cursor.execute(sql)

            chunk = 1  # just need for 1st iteration
            while chunk:
                chunk = cursor.fetchmany(
                    size=app_settings.SEGMENTS_CURSOR_FETCHMANY_SIZE
                )
                for row in chunk:
                    if self.is_valid_member_id(row[0]):
                        yield row[0]
                    else:
                        logger.error(f"Invalid result for sql query:\n{sql}", stack_info=True, exc_info=True)
