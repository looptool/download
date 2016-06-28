from django.conf.urls import include, url
from django.contrib import admin
admin.autodiscover()
from cloop import views

urlpatterns = [
    url(r'^admin/', include(admin.site.urls)),
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
    url(r'^pedagogyhelper/$', 'cloop.views.pedagogyhelper', name='pedagogyhelper'),
    url(r'^pedagogyhelperdownload/$', 'cloop.views.pedagogyhelperdownload', name='pedagogyhelperdownload'),
    url(r'^logout/$', 'django.contrib.auth.views.logout', {'next_page': '/'})
]
