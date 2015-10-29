# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('segments', '0002_auto_20151028_1326'),
    ]

    operations = [
        migrations.AddField(
            model_name='segment',
            name='slug',
            field=models.SlugField(max_length=256, unique=True, null=True, blank=True),
        ),
        migrations.AlterField(
            model_name='segment',
            name='definition',
            field=models.TextField(help_text=b'SQL query returning IDs of users in the segment.', null=True, blank=True),
        ),
        migrations.AlterField(
            model_name='segment',
            name='static_ids',
            field=models.TextField(help_text=b'Newline-delimited list of IDs in the segment', null=True, blank=True),
        ),
    ]
