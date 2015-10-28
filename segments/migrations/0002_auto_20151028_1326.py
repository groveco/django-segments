# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('segments', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='segment',
            name='static_ids',
            field=models.TextField(help_text=b'Newline delimited list of static IDs to hold in the segment', null=True, blank=True),
        ),
        migrations.AlterField(
            model_name='segment',
            name='definition',
            field=models.TextField(help_text=b'SQL query that returns IDs of users in the segment.', null=True, blank=True),
        ),
    ]
