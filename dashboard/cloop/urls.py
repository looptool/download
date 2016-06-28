from django.conf.urls import patterns, include, url
from cloop import views

urlpatterns = patterns('',
    url(r'^$', 'cloop.views.home', name='home'),
    url(r'^home/', 'cloop.views.home', name='home'),
    url(r'^mycourses/', 'cloop.views.mycourses', name='mycourses'),
    url(r'^coursedashboard/$', 'cloop.views.coursedashboard', name='coursedashboard'),
    url(r'^overallcoursedashboard/$', 'cloop.views.overallcoursedashboard', name='overallcoursedashboard'),
    url(r'^coursemembers/$', 'cloop.views.coursemembers', name='coursemembers'),
    url(r'^coursemember/$', 'cloop.views.coursemember', name='coursemember'),
    url(r'^coursepage/$', 'cloop.views.coursepage', name='coursepage'),
    url(r'^content/$', 'cloop.views.content', name='content'),
    url(r'^coursestructure/$', 'cloop.views.coursestructure', name='coursestructure'),
    url(r'^communication/$', 'cloop.views.communication', name='communication'),
    url(r'^assessment/$', 'cloop.views.assessment', name='assessment'),
    url(r'^logout/$', 'django.contrib.auth.views.logout',  {'next_page': '/home/'}, name='loggedout'),
)
