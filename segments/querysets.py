from django.db import models


class SegmentQuerySet(models.QuerySet):
    def acitve(self):
        return self.filter(is_deleted=False)
