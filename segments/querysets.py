from django.db import models


class SegmentQuerySet(models.QuerySet):
    def acitve(self):
        return self.exclude(is_deleted=True)
