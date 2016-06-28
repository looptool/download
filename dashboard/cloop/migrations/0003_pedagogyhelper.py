# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cloop', '0002_course_owner'),
    ]

    operations = [
        migrations.CreateModel(
            name='PedagogyHelper',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('pedagogyhelper_json', models.TextField()),
                ('course', models.ForeignKey(to='cloop.Course')),
            ],
        ),
    ]
