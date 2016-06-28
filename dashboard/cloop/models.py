from django.db import models
from django.contrib.auth.models import User

class Course(models.Model):
    code = models.CharField(max_length=100, blank=False)
    title = models.TextField(blank=False)
    offering = models.TextField(blank=False)
    #owner = models.OneToOneField(User,blank=False)
    owner = models.ManyToManyField(User)
    start_date = models.DateField(blank=False)
    no_weeks = models.IntegerField(blank=False)
    created_at = models.DateTimeField(auto_now_add=True)

    BLACKBOARD = 'Blackboard'
    MOODLE = 'Moodle'
    MACMOODLE = 'Macquarie Uni Moodle'
    LMSTYPE_OPTIONS = (
        (BLACKBOARD, BLACKBOARD),
        (MOODLE, MOODLE),
        (MACMOODLE,MACMOODLE)
    )
    lms_type = models.CharField(max_length=50, choices=LMSTYPE_OPTIONS, default=BLACKBOARD)

    def __unicode__(self):
        return self.code

class CourseSubmissionEvent(models.Model):
    title = models.TextField(blank=False)
    course = models.ForeignKey(Course)
    start_date = models.DateTimeField(null=True, blank=True)
    end_date = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    EVENTTYPE_OPTIONS = (
        ('A', 'Assignment'),('Q', 'Quiz')
    )
    event_type = models.CharField(max_length=50, choices=EVENTTYPE_OPTIONS, default='A', blank=True)

    def __unicode__(self):
        return self.title

class CourseSingleEvent(models.Model):
    title = models.TextField(blank=False)
    course = models.ForeignKey(Course)
    event_date = models.DateTimeField(null=True, blank=True)

    def __unicode__(self):
        return self.title

class CourseRepeatingEvent(models.Model):
    title = models.TextField(blank=False)
    course = models.ForeignKey(Course)
    start_week = models.IntegerField(blank=True)
    end_week = models.IntegerField(blank=True)

    EVENTTYPE_OPTIONS = (
        ('0', 'Monday'),('1', 'Tuesday'), ('2', 'Wednesday'), ('3', 'Thursday'), ('4', 'Friday'), ('5', 'Saturday'), ('6', 'Sunday')
    )
    day_of_week = models.CharField(max_length=50, choices=EVENTTYPE_OPTIONS, default='0')
    created_at = models.DateTimeField(auto_now_add=True)

    def __unicode__(self):
        return self.title
