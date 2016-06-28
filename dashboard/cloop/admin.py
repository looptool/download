from django.contrib import admin
from cloop.models import Course, CourseSubmissionEvent, CourseSingleEvent, CourseRepeatingEvent

admin.site.register(Course)
admin.site.register(CourseSubmissionEvent)
admin.site.register(CourseSingleEvent)
admin.site.register(CourseRepeatingEvent)
