# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Course',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('code', models.CharField(max_length=100)),
                ('title', models.TextField()),
                ('offering', models.TextField()),
                ('start_date', models.DateField()),
                ('no_weeks', models.IntegerField()),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('lms_type', models.CharField(default=b'Blackboard', max_length=50, choices=[(b'Blackboard', b'Blackboard'), (b'Moodle', b'Moodle'), (b'Macquarie Uni Moodle', b'Macquarie Uni Moodle')])),
            ],
        ),
        migrations.CreateModel(
            name='CourseRepeatingEvent',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('title', models.TextField()),
                ('start_week', models.IntegerField(blank=True)),
                ('end_week', models.IntegerField(blank=True)),
                ('day_of_week', models.CharField(default=b'0', max_length=50, choices=[(b'0', b'Monday'), (b'1', b'Tuesday'), (b'2', b'Wednesday'), (b'3', b'Thursday'), (b'4', b'Friday'), (b'5', b'Saturday'), (b'6', b'Sunday')])),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('course', models.ForeignKey(to='cloop.Course')),
            ],
        ),
        migrations.CreateModel(
            name='CourseSingleEvent',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('title', models.TextField()),
                ('event_date', models.DateTimeField(null=True, blank=True)),
                ('course', models.ForeignKey(to='cloop.Course')),
            ],
        ),
        migrations.CreateModel(
            name='CourseSubmissionEvent',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('title', models.TextField()),
                ('start_date', models.DateTimeField(null=True, blank=True)),
                ('end_date', models.DateTimeField(null=True, blank=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('event_type', models.CharField(default=b'A', max_length=50, blank=True, choices=[(b'A', b'Assignment'), (b'Q', b'Quiz')])),
                ('course', models.ForeignKey(to='cloop.Course')),
            ],
        ),
    ]
