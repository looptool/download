"""
    Loop Dashboard utils

    Contains methods to process data for dashboards dynamically

    Todo:
    - Pandas has bug that is stopping the database from being connected to using a connection string from the settings.py file

"""

from django.db import connections
from datetime import datetime, date, timedelta
import time
from cloop.models import Course, CourseRepeatingEvent, CourseSingleEvent, CourseSubmissionEvent
import json
import pandas as pd
from sqlalchemy import create_engine
from collections import defaultdict


def get_contenttypes(course_type):
    communication_types = None
    assessment_types = None
    if course_type == "Moodle":
        communication_types = ['forum']
        assessment_types = ['quiz', 'assign']
    elif course_type == "MoodleMacquarie":
        communication_types = ['forum_discussions', 'oublog', 'forum', 'oublog_posts', 'forum_posts', 'dialogue', 'dialogue_conversations']
        assessment_types = ['quiz','quiz_attempts','grade_grades','turnitintool', 'assign']
    elif course_type == "Blackboard":
        communication_types = ['resource/x-bb-discussionboard', 'course/x-bb-collabsession', 'resource/x-bb-discussionfolder']
        assessment_types = ['assessment/x-bb-qti-test', 'course/x-bb-courseassessment', 'resource/x-turnitin-assignment']

    assessment_types_str = ','.join("'{0}'".format(x) for x in assessment_types)
    communication_types_str = ','.join("'{0}'".format(x) for x in communication_types)

    return  communication_types, assessment_types, communication_types_str, assessment_types_str

def get_prepostevent_treetable(course_id, course_weeks, treelist_json, curr_evt, course_type):

    communication_types, assessment_types, communication_types_str, assessment_types_str = get_contenttypes()

    engine = create_engine('mysql://root:root@localhost/cloop_olap?unix_socket=/Applications/MAMP/tmp/mysql/mysql.sock', echo=False)
    sql = "SELECT F.user_id, F.page_id, F.pageview, D.date_week, D.date_dayinweek FROM fact_coursevisits F INNER JOIN dim_dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d"%(course_id)
    connection = engine.raw_connection()
    df = pd.read_sql(sql, connection)

    treelist  = json.loads(treelist_json)
    weeks_str_list = ','.join(map(str, course_weeks))

    table = []
    table.append("<thead><tr>")
    table.append("<th>Name</th><th>Type</th><th>View</th>")
    for x in range(len(course_weeks)):
        table.append("<th>Week %d</th>" % (x+1))
    table.append("</tr>")
    table.append("</thead>")
    table.append("<tbody>\n")
    for page in treelist:
        pageid2 = int(page[0]) #int(row[2])

        cursor = connections['olap'].cursor()
        res_sql = "SELECT title, content_type, content_id, parent_id, order_no FROM dim_pages WHERE content_id=%d AND course_id=%d order by order_no;" %(pageid2, course_id)
        row_count = cursor.execute(res_sql);
        row = cursor.fetchall()
        cursor.close()

        order_no = row[0][4]
        content_type = row[0][1]
        classname = ""
        parentidstring =""

        pagestoinclude = None
        if course_type=="Moodle":
            pagestoinclude = ['page', 'folder', 'section', 'course_modules']
        else:
            pagestoinclude = ['course/x-bb-coursetoc', 'resource/x-bb-folder', 'resource/x-bb-stafffolder', 'resource/x-bb-discussionfolder']

        if row[0][1] in pagestoinclude:
            classname = "folder"
        else:
            classname = "file"

        if row[0][3]==0:
            parentidstring = ""
        else:
            parentidstring = "data-tt-parent-id='%s'" % (str(row[0][3]))
        if (row[0][1]=='section'):
            table.append("<tr data-tt-id='%s' %s class='stats-row'><td class='stats-title'><span class='%s'>%s</span></td><td class='stats-title'>%s</td><td class='stats-title'><a href='/coursepage?course_id=%d&page_id=%s&section_order=%s' class='btn btn-small btn-primary'>View</a></td></td>" % (str(pageid2), parentidstring, classname, row[0][0], row[0][1], course_id, str(pageid2), str(row[0][4])))
        else:
            table.append("<tr data-tt-id='%s' %s class='stats-row'><td class='stats-title'><span class='%s'>%s</span></td><td class='stats-title'>%s</td><td class='stats-title'><a href='/coursepage?course_id=%d&page_id=%s' class='btn btn-small btn-primary'>View</a></td>" % (str(pageid2), parentidstring, classname, row[0][0], row[0][1], course_id, str(pageid2)))

        filterd_df = df[(df.page_id == int(pageid2))]
        grouped_df = filterd_df.groupby('date_week').sum()

        total_counts_dict = defaultdict(int)

        for index, row in grouped_df.iterrows():
            total_counts_dict[index] = int(row['pageview'])

        for week in course_weeks:
            totalcounts = total_counts_dict[week]

            if totalcounts > 0:
                # Insert sparkline
                evt_pre_list = range(0,curr_evt)
                eventpre_df = filterd_df[filterd_df.date_dayinweek.isin(evt_pre_list)]
                eventpre_counts = len(eventpre_df.index)
                eventpost_counts = totalcounts - eventpre_counts
                width, height = scale_chart(totalcounts)
                table.append('<td><span class="sparklines2" sparkType="pie" values="%d,%d" sparkwidth="%d" sparkheight="%d"></span></td>' % (eventpre_counts, eventpost_counts, width, height))
            else:
                table.append('<td>&nbsp;</td>')

        table.append("</tr>\n")

    table.append("</tr>\n")
    table.append("</tbody>\n")
    table = ''.join(table)

    return table

def get_prepostevent_table(course_id, curr_evt, contenttype, course_weeks, course_type):

    weeks_str_list = ','.join(map(str, course_weeks))

    communication_types, assessment_types, communication_types_str, assessment_types_str = get_contenttypes()

    if (contenttype == "communication" or contenttype == "forum"):
        #contenttype = "'forum'"
        contentdisplaytype = ','.join("'{0}'".format(x) for x in communication_types)

    else:
        #contenttype = "'quiz','assign'"
        contentdisplaytype = ','.join("'{0}'".format(x) for x in assessment_types)

    table = []
    table.append("<thead><tr>")
    table.append("<th>Name</th><th>Type</th><th>View</th>")
    for x in range(len(course_weeks)):
        table.append("<th>Week %d</th>" % (x+1))
    table.append("</tr>")
    table.append("</thead>")
    table.append("<tbody>\n")

    cursor = connections['olap'].cursor()

    res_sql = "SELECT title, content_type, content_id, parent_id FROM dim_pages WHERE content_type IN (%s) AND course_id=%d order by order_no;" %(contentdisplaytype, course_id)

    row_count = cursor.execute(res_sql);
    result = cursor.fetchall()
    cursor.close()

    engine = create_engine('mysql://root:root@localhost/cloop_olap29?unix_socket=/Applications/MAMP/tmp/mysql/mysql.sock', echo=False)
    sql = "SELECT F.user_id, F.page_id, F.pageview, D.date_week, D.date_dayinweek  FROM fact_coursevisits F INNER JOIN dim_dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d"%(course_id)
    connection = engine.raw_connection()
    df = pd.read_sql(sql, connection)

    for row in result:
        pageid2 = int(row[2])
        table.append("<tr class='stats-row'><td class='stats-title'><span>%s</span></td><td class='stats-title'>%s</td><td class='stats-title'><a href='/coursepage?course_id=%d&page_id=%s'>View</a></td>" % (row[0], row[1], course_id, str(pageid2)))

        for week in course_weeks:
            filterd_df = df[(df.page_id == pageid2)&(df.date_week == week)]
            totalcounts = len(filterd_df.index)
            if totalcounts > 0:
                # Insert sparkline
                evt_pre_list = range(0,curr_evt)
                eventpre_df = filterd_df[filterd_df.date_dayinweek.isin(evt_pre_list)]
                eventpre_counts = len(eventpre_df.index)
                eventpost_counts = totalcounts - eventpre_counts
                width, height = scale_chart(totalcounts)
                table.append('<td><span class="sparklines2" sparkType="pie" values="%d,%d" sparkwidth="%d" sparkheight="%d"></span></td>' % (eventpre_counts, eventpost_counts, width, height))
            else:
                table.append('<td>&nbsp;</td>')
        table.append("</tr>\n")

    table.append("</tr>\n")
    table.append("</tbody>\n")
    table = ''.join(table)

    return table

def getusers_prepostevent_table(course_id, curr_evt, course_weeks):

    week_totals = [0] * len(course_weeks)
    weeks_str_list = ','.join(map(str, course_weeks))

    table = []
    table.append("<thead><tr>")
    table.append("<th>Firstname</th><th>Lastname</th><th>Account Type</th><th>View</th>")
    for x in range(len(course_weeks)):  #range(0, 10):
        table.append("<th>Week %d</th>" % (x+1))
    table.append("</tr>")
    table.append("</thead>")
    table.append("<tbody>\n")

    cursor = connections['olap'].cursor()
    sql = "SELECT user_pk,firstname,lastname,role, lms_id FROM dim_users WHERE course_id=%d ORDER BY lastname;" %(course_id) #need to make dynamic
    row_count = cursor.execute(sql);
    result = cursor.fetchall()
    cursor.close()

    engine = create_engine('mysql://root:root@localhost/cloop_olap?unix_socket=/Applications/MAMP/tmp/mysql/mysql.sock', echo=False)
    sql2 = "SELECT F.user_id, F.page_id, F.pageview, D.date_week, D.date_dayinweek FROM fact_coursevisits F INNER JOIN dim_dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d"%(course_id)
    connection = engine.raw_connection()
    df = pd.read_sql(sql2, connection)
    print df.head(2)

    for row in result:
        table.append("<tr class='stats-row'><td class='stats-title'>%s</td><td class='stats-title'>%s</td><td class='stats-title'>%s</td><td class='stats-title'><a href='/coursemember?user_id=%s&course_id=%s'>View</a></td>" % (row[1],row[2],row[3], row[0], course_id))
        filterd_df = df[(df.user_id == int(row[4]))]
        grouped_df = filterd_df.groupby('date_week').sum()

        total_counts_dict = defaultdict(int)

        for index, row in grouped_df.iterrows():
            total_counts_dict[index] = int(row['pageview'])

        for week in course_weeks:
            totalcounts = total_counts_dict[week]
            if totalcounts > 0:
                # Insert sparkline
                evt_pre_list = range(0,curr_evt)
                eventpre_df = filterd_df[filterd_df.date_dayinweek.isin(evt_pre_list)]
                eventpre_counts = len(eventpre_df.index) #get_usereventpre_postcounts(week, row[4], course_id, curr_evt)
                eventpost_counts = totalcounts - eventpre_counts
                width, height = scale_chart(totalcounts)
                table.append('<td><span class="sparklines2" sparkType="pie" values="%d,%d" sparkwidth="%d" sparkheight="%d"></span></td>' % (eventpre_counts, eventpost_counts, width, height))
            else:
                table.append('<td>&nbsp;</td>')

        table.append("</tr>\n")

    table.append("</tbody>\n")
    return ''.join(table)

def get_userweekcountforviz(week,userid,course_id):
    pageviews = 0
    cursor = connections['olap'].cursor()
    sql = "SELECT SUM(F.pageview) AS pageviews FROM fact_coursevisits F INNER JOIN dim_dates  D ON F.Date_Id = D.Id WHERE F.user_id='%s' AND F.course_id=%d AND D.Date_week=%d;" % (userid, course_id, int(week))
    row_count = cursor.execute(sql);
    result = cursor.fetchall()
    for row in result:
           if row[0] is not None:
                pageviews = int(row[0])
    cursor.close()
    return pageviews

def get_usereventpre_postcounts(week,userid,course_id, curr_evt):
    evt_pre_list = range(0,curr_evt)
    curr_evt_str = ','.join(map(str, evt_pre_list))
    pageviews = 0
    cursor = connections['olap'].cursor()
    sql = "SELECT SUM(F.pageview) AS pageviews FROM fact_coursevisits F INNER JOIN dim_dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.user_id=%s AND D.date_week=%d AND D.date_dayinweek IN (%s);" % (course_id, str(userid), week, curr_evt_str)
    row_count = cursor.execute(sql);
    result = cursor.fetchall()
    for row in result:
           if row[0] is not None:
                pageviews = int(row[0])
    cursor.close()
    return int(pageviews)

def get_weekcountforviz(week, pageid, course_id):
    pageviews = 0
    cursor = connections['olap'].cursor()
    sql = "SELECT SUM(F.pageview) AS pageviews FROM fact_coursevisits F INNER JOIN dim_dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.page_id=%d AND D.date_week=%d;" % (course_id, int(pageid), int(week))
    row_count = cursor.execute(sql);
    result = cursor.fetchall()
    for row in result:
           if row[0] is not None:
                pageviews = int(row[0])
    cursor.close()
    return pageviews

def get_eventpre_postcounts(week, pageid, course_id, curr_evt):
    cursor = connections['olap'].cursor()
    evt_pre_list = range(0,curr_evt)
    curr_evt_str = ','.join(map(str, evt_pre_list))
    pageviews = 0
    pageid2 = int(pageid)
    sql = "SELECT SUM(F.pageview) AS pageviews FROM fact_coursevisits F INNER JOIN dim_dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.page_id=%d AND D.date_week=%d AND D.date_dayinweek IN (%s);" % (course_id, int(pageid2), int(week), curr_evt_str)
    row_count = cursor.execute(sql);
    result = cursor.fetchall()
    for row in result:
           if row[0] is not None:
                pageviews = int(row[0])
    cursor.close()
    return int(pageviews)

def scale_chart(total_counts):
    width = 50
    height = 50
    if total_counts > 75:
        width = 50
        height = 50
    elif total_counts >= 26 and total_counts <= 74 :
        width = 25
        height = 25
    elif total_counts <= 25:
        width = 15
        height = 15
    return width, height

def getSingleCourseEvents(course_id):
    course = Course.objects.get(pk=course_id)
    single_evt = CourseSingleEvent.objects.filter(course=course)
    evt_lst = ""
    for evt in single_evt:
        dte = evt.event_date
        evt_lst = evt_lst + "{x: Date.UTC(%d, %d, %d), title: '%s'}," % (dte.year, (dte.month - 1), dte.day, evt.title)
    evt = "{type: 'flags', name: 'Single Course Events', data:[%s], shape: 'squarepin'}" % (evt_lst)
    return evt

def getSubmissionCourseEvents(course_id):
    course = Course.objects.get(pk=course_id)
    sub_evt = CourseSubmissionEvent.objects.filter(course=course)
    evt_lst = ""
    for evt in sub_evt:
        sdte = evt.start_date
        edte = evt.end_date
        evt_lst = evt_lst + "{x: Date.UTC(%d, %d, %d), title: '%s: Start Date'}," % (sdte.year, (sdte.month - 1), sdte.day, evt.title)
        evt_lst = evt_lst + "{x: Date.UTC(%d, %d, %d), title: '%s: End Date'}," % (edte.year, (edte.month -1), edte.day, evt.title)
    evt = "{type: 'flags', name: 'Submission Events', data:[%s], onSeries: 'dataseries', shape: 'squarepin'}" % (evt_lst)
    return evt

def weekbegend(year, week):
    d = date(year, 1, 1)
    delta_days = d.isoweekday() - 1
    delta_weeks = week
    if year == d.isocalendar()[0]:
        delta_weeks -= 1
    # delta for the beginning of the week
    delta = timedelta(days=-delta_days, weeks=delta_weeks)
    weekbeg = d + delta
    # delta2 for the end of the week
    delta2 = timedelta(days=6-delta_days, weeks=delta_weeks)
    weekend = d + delta2

    begweek_unix = time.mktime(weekbeg.timetuple())
    endweek_unix = time.mktime(weekend.timetuple())

    begweek = weekbeg.strftime("%A %d %B %Y")
    endweek = weekend.strftime("%A %d %B %Y")
    return weekbeg, weekend, begweek_unix, endweek_unix

def get_userweekcount(week,pageid, course_id, content_type, order_no):
    cursor = connections['olap'].cursor()
    usercounts = 0
    sql = ""
    if content_type=="section":
        sql = "SELECT COUNT(DISTINCT F.user_pk) AS usercount FROM fact_coursevisits F INNER JOIN dim_dates D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.section_order=%d AND D.date_week=%d;" % (course_id, int(order_no), week)
    else:
        sql = "SELECT COUNT(DISTINCT F.user_pk) AS usercount FROM fact_coursevisits F INNER JOIN dim_dates D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.page_id=%d AND D.date_week=%d;" % (course_id, int(pageid), week)
    row_count = cursor.execute(sql);
    result = cursor.fetchall()
    for row in result:
           if row[0] is not None:
                usercounts = int(row[0])
    return int(usercounts)

def get_quizusercount(week,pageid, course_id):
    cursor = connections['olap'].cursor()
    usercounts = 0
    # convert week to datetimestamp
    start, end, startunix, endunix = weekbegend(2015, week)
    startUTC = 'Date.UTC(%d, %d, %d)' % (start.year, start.month-1, start.day)
    endUTC = 'Date.UTC(%d, %d, %d)' % (end.year, end.month-1, end.day)
    sql = ""
    sql = "SELECT COUNT(DISTINCT F.user_id) AS usercount FROM dim_submissionattempts F WHERE F.course_id=%d AND F.content_id=%d AND F.unixtimestamp BETWEEN %d AND %d;" % (course_id, int(pageid), startunix, endunix)
    row_count = cursor.execute(sql);
    result = cursor.fetchall()
    for row in result:
           if row[0] is not None:
                usercounts = int(row[0])
    return int(usercounts)

def get_quizusercoursecount(pageid, course_id):
    cursor = connections['olap'].cursor()
    usercounts = 0

    sql = ""
    sql = "SELECT COUNT(DISTINCT F.user_id) AS usercount FROM dim_submissionattempts F WHERE F.course_id=%d AND F.content_id=%d;" % (course_id, int(pageid))
    row_count = cursor.execute(sql);
    result = cursor.fetchall()
    for row in result:
           if row[0] is not None:
                usercounts = int(row[0])
    return int(usercounts)

def get_usercoursecount(pageid, course_id, content_type, order_no):
    cursor = connections['olap'].cursor()
    usercounts = 0
    sql = ""
    if content_type=="section":
        sql = "SELECT COUNT(DISTINCT F.user_pk) AS usercount FROM fact_coursevisits F INNER JOIN dim_dates D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.section_order=%d;" % (course_id, int(order_no))
    else:
        sql = "SELECT COUNT(DISTINCT F.user_pk) AS usercount FROM fact_coursevisits F INNER JOIN dim_dates D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.page_id=%d;" % (course_id, int(pageid))
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
           if row[0] is not None:
                usercounts = int(row[0])
    return int(usercounts)

def get_noforumposts(pageid, course_id, week_id):
    cursor = connections['olap'].cursor()
    no_posts = 0
    sql = ""
    sql = "SELECT COUNT(F.id) AS pageviews, D.date_week  FROM summary_posts F INNER JOIN dim_dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.forum_id=%d AND D.date_week=%d;" % (course_id, int(pageid), week_id)
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
           #print row
           if row[0] is not None:
                no_posts = int(row[0])
    return int(no_posts)

def get_usernoforumposts(pageid, course_id, user_id, unixstart=None, unixend=None):
    cursor = connections['olap'].cursor()
    no_posts = 0
    if unixstart is None:
        sql = "SELECT COUNT(F.id) AS pageviews, D.date_week  FROM summary_posts F INNER JOIN dim_dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.forum_id=%d AND F.user_id=%d;" % (course_id, int(pageid), user_id)
    else:
        sql = "SELECT COUNT(F.id) AS pageviews, D.date_week  FROM summary_posts F INNER JOIN dim_dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.forum_id=%d AND F.user_id=%d AND D.unixtimestamp BETWEEN %d AND %d;" % (course_id, int(pageid), user_id, unixstart, unixend)

    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
           if row[0] is not None:
                no_posts = int(row[0])
    return int(no_posts)

def get_nocourseforumposts(pageid, course_id, unixstart=None, unixend=None):
    cursor = connections['olap'].cursor()
    no_posts = 0
    sql = ""
    if unixstart is None:
        sql = "SELECT COUNT(F.id) AS pageviews, D.date_week  FROM summary_posts F INNER JOIN dim_dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.forum_id=%d" % (course_id, int(pageid))
    else:
        sql = "SELECT COUNT(F.id) AS pageviews, D.date_week  FROM summary_posts F INNER JOIN dim_dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.forum_id=%d AND D.unixtimestamp BETWEEN %d AND %d" % (course_id, int(pageid), unixstart, unixend)
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
           if row[0] is not None:
                no_posts = int(row[0])
    return int(no_posts)

def get_quizattemps(pageid, course_id, unixstart=None, unixend=None):
    cursor = connections['olap'].cursor()
    no_attempts = 0
    sql = ""
    if unixstart is None:
        sql = "SELECT count(id) AS attempts FROM dim_submissionattempts WHERE content_id=%s AND course_id=%s;" % (pageid, course_id)
    else:
        sql = "SELECT count(id) AS attempts FROM dim_submissionattempts WHERE content_id=%s AND course_id=%s AND unixtimestamp BETWEEN %d AND %d;" % (pageid, course_id, unixstart, unixend)
    print sql
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
           if row[0] is not None:
                no_attempts = int(row[0])
    return int(no_attempts)

def get_userquizattemps(pageid, course_id, user_id, unixstart=None, unixend=None):
    cursor = connections['olap'].cursor()
    no_attempts = 0
    if unixstart is None:
        sql = "SELECT count(id) AS attempts FROM dim_submissionattempts WHERE content_id=%s AND course_id=%s AND user_id=%s;" % (pageid, course_id, user_id)
    else:
        sql = "SELECT count(id) AS attempts FROM dim_submissionattempts WHERE content_id=%s AND course_id=%s AND user_id=%s AND unixtimestamp BETWEEN %d AND %d;" % (pageid, course_id, user_id, unixstart, unixend)

    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
           #print row
           if row[0] is not None:
                no_attempts = int(row[0])
    return int(no_attempts)

def get_avggrade(pageid, course_id, unixstart=None, unixend=None):
    cursor = connections['olap'].cursor()
    avg_grade = 0
    sql = ""
    if unixstart is None:
        sql = "SELECT avg(grade) AS avggrade FROM dim_submissionattempts WHERE content_id=%s AND course_id=%s;" % (pageid, course_id)
    else:
        sql = "SELECT avg(grade) AS avggrade FROM dim_submissionattempts WHERE content_id=%s AND course_id=%s AND unixtimestamp BETWEEN %d AND %d;" % (pageid, course_id, unixstart, unixend)
    print sql
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
           if row[0] is not None:
                avg_grade = int(row[0])
    return int(avg_grade)

def get_usergrade(pageid, course_id, user_id, unixstart=None, unixend=None):
    cursor = connections['olap'].cursor()
    avg_grade = 0
    if unixstart is None:
        sql = "SELECT max(grade) AS maxgrade FROM dim_submissionattempts WHERE content_id=%s AND course_id=%s AND user_id=%s;" % (pageid, course_id, user_id)
    else:
        sql = "SELECT max(grade) AS maxgrade FROM dim_submissionattempts WHERE content_id=%s AND course_id=%s AND user_id=%s AND unixtimestamp BETWEEN %d AND %d;" % (pageid, course_id, user_id, unixstart, unixend)
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
           #print row
           if row[0] is not None:
                avg_grade = row[0]
    rounded_grade = "%.2f" % round(float(avg_grade),2)
    return rounded_grade

def get_contentdetails(content_id, course_id):
    cursor = connections['olap'].cursor()
    sql = "SELECT title, content_type FROM dim_pages WHERE content_id=%d AND course_id=%s;" % (content_id, course_id)
    row_count = cursor.execute(sql)
    title = ""
    type = ""
    row = cursor.fetchone()
    if row[0] is not None:
        title = row[0]
    if row[1] is not None:
        type = row[1]
    return {'title': title, 'type': type}

def get_userdetails(user_id, course_id):
    cursor = connections['olap'].cursor()
    sql = "SELECT firstname, lastname, email, role FROM dim_users WHERE user_pk='%s' AND course_id=%s;" % (user_id, course_id)
    result = cursor.execute(sql);
    firstname = ""
    lastname = ""
    email = ""
    role = ""
    row = cursor.fetchone()
    if row[0] is not None:
        firstname = row[0]
    if row[1] is not None:
        lastname = row[1]
    if row[2] is not None:
        email = row[2]
    if row[3] is not None:
        role = row[3]
    return {'firstname': firstname, 'lastname': lastname, 'email':email, 'role': role }

def generate_userbyweek_histogram(week, course_id):
    cursor = connections['olap'].cursor()
    userbyweek_sql = "SELECT SUM(F.pageview) AS pageviews FROM dim_dates D LEFT JOIN fact_coursevisits F ON D.Id = F.Date_Id WHERE D.DATE_week=%d AND F.course_id=%d GROUP BY F.user_pk;" %(week, course_id)
    row_count = cursor.execute(userbyweek_sql);
    result = cursor.fetchall()
    userpageview_list = []
    for row in result:
        userpageview_list.append(row[0])
    max_pageviews = 0
    if len(userpageview_list)!=0:
        max_pageviews = max(userpageview_list)
    else:
        max_pageviews = 0
    histogram = create_bins(userpageview_list, max_pageviews, 25)
    return histogram

def generate_userbycourse_histogram(course_id):
    cursor = connections['olap'].cursor()
    userbyweek_sql = "SELECT SUM(F.pageview) AS pageviews FROM dim_dates D LEFT JOIN fact_coursevisits F ON D.Id = F.Date_Id WHERE F.course_id=%d GROUP BY F.user_pk;" %(course_id)
    row_count = cursor.execute(userbyweek_sql);
    result = cursor.fetchall()
    userpageview_list = []
    for row in result:
        userpageview_list.append(row[0])
    max_pageviews = 0
    if len(userpageview_list)!=0:
        max_pageviews = max(userpageview_list)
    else:
        max_pageviews = 0
    histogram = create_bins(userpageview_list, max_pageviews, 100)
    return histogram

def generate_usersforpage_histogram(course_id, page_id, section_order, unixstart=None, unixend=None):
    cursor = connections['olap'].cursor()
    userbyweek_sql = ""
    if unixstart is None:
        if (section_order==0):
            userbyweek_sql = "SELECT SUM(F.pageview) AS pageviews FROM fact_coursevisits F WHERE F.course_id=%d AND F.page_id=%d GROUP BY F.user_pk;" %(course_id, page_id)
        else:
            userbyweek_sql = "SELECT SUM(F.pageview) AS pageviews FROM fact_coursevisits F WHERE F.course_id=%d AND F.page_id=%d AND F.section_order=%d GROUP BY F.user_pk;" %(course_id, page_id, section_order)
    else:
        if (section_order==0):
            userbyweek_sql = "SELECT SUM(F.pageview) AS pageviews FROM fact_coursevisits F WHERE F.course_id=%d AND F.page_id=%d AND F.unixtimestamp BETWEEN %d AND %d GROUP BY F.user_pk;" %(course_id, page_id, unixstart, unixend)
        else:
            userbyweek_sql = "SELECT SUM(F.pageview) AS pageviews FROM fact_coursevisits F WHERE F.course_id=%d AND F.page_id=%d AND F.section_order=%d AND F.unixtimestamp BETWEEN %d AND %d GROUP BY F.user_pk;" %(course_id, page_id, section_order, unixstart, unixend)
    row_count = cursor.execute(userbyweek_sql);
    result = cursor.fetchall()
    userpageview_list = []
    for row in result:
        userpageview_list.append(row[0])
    max_pageviews = 0
    if len(userpageview_list)!=0:
        max_pageviews = max(userpageview_list)
    else:
        max_pageviews = 0
    histogram = create_bins(userpageview_list, max_pageviews, 5)
    return histogram

#Todo: If numpy is added as a dependency and deployable on openshift then there is a digitize and histogram method available
def create_bins(grouped_totals_list, max, bin_allocation):
    #print grouped_totals_list, bin_allocation
    number_of_bins = max//bin_allocation + 1
    allocations_list = []
    allocations_dict = {}
    for i in xrange(bin_allocation, (number_of_bins*bin_allocation)+1, bin_allocation):
        allocations_list.append(i)
        allocations_dict[i] = 0
    for val in grouped_totals_list:
        for (i, x) in enumerate(allocations_list):
            if ((val <= allocations_list[i]) and (val > (allocations_list[i]-bin_allocation-1))):
                allocations_dict[x] = allocations_dict[x] + 1
                break
    #Create bin labels
    label_list = []
    value_list = []
    for (i, x) in enumerate(allocations_list):
        label = ""
        if (i == 0):
            label = "<= %d" % (x)
        elif (i==(len(allocations_list))-1):
            label = ">= %d" % (x)
        else:
            label = "%d - %d" % (x-bin_allocation+1, x)
        label_list.append(label)
        value_list.append(allocations_dict[x])
    values = ','.join(map(str, value_list))
    labels = '"' + '","'.join(map(str, label_list)) + '"'
    return {'values': values, 'labels': labels}

def get_usersthatdidnotaccesscontent(content_id, course_id, section_order, unixstart=None, unixend=None ):
    cursor = connections['olap'].cursor()
    sql = ""
    if unixstart is None:
        if (section_order==0):
            sql = "SELECT U.firstname, U.lastname, U.email, U.Role FROM dim_users U WHERE U.course_id=%d AND U.lms_id NOT IN (SELECT DISTINCT F.user_id FROM fact_coursevisits F JOIN dim_users U ON F.user_id=U.lms_id WHERE F.page_id=%d AND F.course_id=%d)" %(course_id, content_id, course_id)
        else:
            sql = "SELECT U.firstname, U.lastname, U.email, U.Role FROM dim_users U WHERE U.course_id=%d AND U.lms_id NOT IN (SELECT DISTINCT F.user_id FROM fact_coursevisits F JOIN dim_users U ON F.user_id=U.lms_id WHERE F.page_id=%d AND F.course_id=%d AND F.section_order=%d)" %(course_id, content_id, course_id, section_order)
    else:
        if (section_order==0):
            sql = "SELECT U.firstname, U.lastname, U.email, U.Role FROM dim_users U WHERE U.course_id=%d AND U.lms_id NOT IN (SELECT DISTINCT F.user_id FROM fact_coursevisits F JOIN dim_users U ON F.user_id=U.lms_id WHERE F.page_id=%d AND F.course_id=%d AND F.unixtimestamp BETWEEN %d AND %d)" %(course_id, content_id, course_id, unixstart, unixend)
        else:
            sql = "SELECT U.firstname, U.lastname, U.email, U.Role FROM dim_users U WHERE U.course_id=%d AND U.lms_id NOT IN (SELECT DISTINCT F.user_id FROM fact_coursevisits F JOIN dim_users U ON F.user_id=U.lms_id WHERE F.page_id=%d AND F.course_id=%d AND F.section_order=%d AND F.unixtimestamp BETWEEN %d AND %d)" %(course_id, content_id, course_id, section_order, unixstart, unixend)
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    table = ""
    for row in result:
        table = table + '<tr><td class="center">%s</td><td class="center">%s</td><td class="center">%s</td><td class="center">%s</td></tr>' %(row[0], row[1], row[2], row[3])
    return table

def get_contentpageviews_dataset(content_id, course_id, weeks, section_order, course_type):
    communication_types = None
    assessment_types = None
    if course_type == "Moodle":
        communication_types = ['forum']
        assessment_types = ['quiz', 'assign']
    elif course_type == "MoodleMacquarie":
        communication_types = ['forum_discussions', 'oublog', 'forum', 'oublog_posts', 'forum_posts', 'dialogue', 'dialogue_conversations']
        assessment_types = ['quiz','quiz_attempts','grade_grades','turnitintool', 'assign']
    elif course_type == "Blackboard":
        communication_types = ['resource/x-bb-discussionboard', 'course/x-bb-collabsession', 'resource/x-bb-discussionfolder']
        assessment_types = ['assessment/x-bb-qti-test', 'course/x-bb-courseassessment', 'resource/x-turnitin-assignment']

    excluse_contentype_list = communication_types + assessment_types
    excluse_contentype_str = ','.join("'{0}'".format(x) for x in excluse_contentype_list)

    cursor = connections['olap'].cursor()
    sql = "SELECT content_type FROM dim_pages WHERE content_id=%d AND course_id=%s;" % (content_id, course_id)
    result = cursor.execute(sql);
    content_type = ""
    row = cursor.fetchone()
    if row[0] is not None:
        content_type = row[0]

    sql_clause = ""
    content_type_str = ""
    if content_type in communication_types:
        sql_clause = "IN"
        content_type_str = ','.join("'{0}'".format(x) for x in communication_types)
    elif content_type in assessment_types:
        sql_clause = "IN"
        content_type_str = ','.join("'{0}'".format(x) for x in assessment_types)
    else:
        sql_clause = "NOT IN"
        content_type_str = excluse_contentype_str

    sql = ""
    if (section_order==0):
        sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day,SUM(F.pageview) AS Pageviews FROM dim_dates D LEFT JOIN fact_coursevisits F ON D.Id = F.Date_Id WHERE D.DATE_week IN (%s) AND F.course_id=%d AND F.page_id='%s' AND F.module %s (%s) GROUP BY D.id ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks, course_id, content_id, sql_clause, content_type_str)
    else:
        sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day,SUM(F.pageview) AS Pageviews FROM dim_dates D LEFT JOIN fact_coursevisits F ON D.Id = F.Date_Id WHERE D.DATE_week IN (%s) AND F.course_id=%d AND F.page_id='%s' AND F.section_order=%d AND F.module %s (%s) GROUP BY D.id ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks, course_id, content_id, section_order, sql_clause, content_type_str)
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    pagepageviews_dict = {}
    dataset_list = []
    for row in result:
        pagepageviews_dict[row[0]] = row[4]
    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day FROM dim_dates D WHERE D.DATE_week IN (%s) ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks)
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
        if row[0] in pagepageviews_dict:
           pageviews = pagepageviews_dict[row[0]]
        else:
           pageviews = 0
        datapoint = "[Date.UTC(%s,%s,%s),%s]" % (row[1],str(int(row[2])-1),row[3],pageviews)
        dataset_list.append(datapoint)
    dataset = ','.join(map(str, dataset_list))
    return dataset

def get_userpageviews_dataset(user_id, course_id, weeks, course_type):
    communication_types = None
    assessment_types = None
    if course_type == "Moodle":
        communication_types = ['forum']
        assessment_types = ['quiz', 'assign']
    elif course_type == "MoodleMacquarie":
        communication_types = ['forum_discussions', 'oublog', 'forum', 'oublog_posts', 'forum_posts', 'dialogue', 'dialogue_conversations']
        assessment_types = ['quiz','quiz_attempts','grade_grades','turnitintool', 'assign']
    elif course_type == "Blackboard":
        communication_types = ['resource/x-bb-discussionboard', 'course/x-bb-collabsession', 'resource/x-bb-discussionfolder']
        assessment_types = ['assessment/x-bb-qti-test', 'course/x-bb-courseassessment', 'resource/x-turnitin-assignment']

    excluse_contentype_list = communication_types + assessment_types
    excluse_contentype_str = ','.join("'{0}'".format(x) for x in excluse_contentype_list)

    cursor = connections['olap'].cursor()
    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day,SUM(F.pageview) AS Pageviews FROM dim_dates D LEFT JOIN fact_coursevisits F ON D.Id = F.Date_Id WHERE D.DATE_week IN (%s) AND F.course_id=%d AND F.user_pk='%s' AND F.module NOT IN (%s) GROUP BY D.id ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks, course_id, user_id, excluse_contentype_str)
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    userpageviews_dict = {}
    dataset_list = []
    for row in result:
        userpageviews_dict[row[0]] = row[4]
    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day FROM dim_dates D WHERE D.DATE_week IN (%s) ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks)
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
        if row[0] in userpageviews_dict:
           pageviews = userpageviews_dict[row[0]]
        else:
           pageviews = 0
        datapoint = "[Date.UTC(%s,%s,%s),%s]" % (row[1],str(int(row[2])-1),row[3],pageviews)
        dataset_list.append(datapoint)
    dataset = ','.join(map(str, dataset_list))
    return dataset

def get_usercommunicationviews_dataset(user_id, course_id, weeks, course_type):
    communication_types = None
    assessment_types = None
    if course_type == "Moodle":
        communication_types = ['forum']
        assessment_types = ['quiz', 'assign']
    elif course_type == "MoodleMacquarie":
        communication_types = ['forum_discussions', 'oublog', 'forum', 'oublog_posts', 'forum_posts', 'dialogue', 'dialogue_conversations']
        assessment_types = ['quiz','quiz_attempts','grade_grades','turnitintool', 'assign']
    elif course_type == "Blackboard":
        communication_types = ['resource/x-bb-discussionboard', 'course/x-bb-collabsession', 'resource/x-bb-discussionfolder']
        assessment_types = ['assessment/x-bb-qti-test', 'course/x-bb-courseassessment', 'resource/x-turnitin-assignment']

    assessment_types_str = ','.join("'{0}'".format(x) for x in assessment_types)
    communication_types_str = ','.join("'{0}'".format(x) for x in communication_types)

    cursor = connections['olap'].cursor()
    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day,SUM(F.pageview) AS Pageviews FROM dim_dates D LEFT JOIN fact_coursevisits F ON D.Id = F.Date_Id WHERE D.DATE_week IN (%s) AND F.course_id=%d AND F.user_pk='%s' AND F.module IN (%s) GROUP BY D.id ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks, course_id, user_id, communication_types_str)
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    userpageviews_dict = {}
    dataset_list = []
    for row in result:
        userpageviews_dict[row[0]] = row[4]
    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day FROM dim_dates D WHERE D.DATE_week IN (%s) ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks)
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
        if row[0] in userpageviews_dict:
           pageviews = userpageviews_dict[row[0]]
        else:
           pageviews = 0
        datapoint = "[Date.UTC(%s,%s,%s),%s]" % (row[1],str(int(row[2])-1),row[3],pageviews)
        dataset_list.append(datapoint)
    dataset = ','.join(map(str, dataset_list))
    return dataset

def get_userassessmentviews_dataset(user_id, course_id, weeks, course_type):
    communication_types = None
    assessment_types = None
    if course_type == "Moodle":
        communication_types = ['forum']
        assessment_types = ['quiz', 'assign']
    elif course_type == "MoodleMacquarie":
        communication_types = ['forum_discussions', 'oublog', 'forum', 'oublog_posts', 'forum_posts', 'dialogue', 'dialogue_conversations']
        assessment_types = ['quiz','quiz_attempts','grade_grades','turnitintool', 'assign']
    elif course_type == "Blackboard":
        communication_types = ['resource/x-bb-discussionboard', 'course/x-bb-collabsession', 'resource/x-bb-discussionfolder']
        assessment_types = ['assessment/x-bb-qti-test', 'course/x-bb-courseassessment', 'resource/x-turnitin-assignment']

    assessment_types_str = ','.join("'{0}'".format(x) for x in assessment_types)
    communication_types_str = ','.join("'{0}'".format(x) for x in communication_types)

    cursor = connections['olap'].cursor()
    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day,SUM(F.pageview) AS Pageviews FROM dim_dates D LEFT JOIN fact_coursevisits F ON D.Id = F.Date_Id WHERE D.DATE_week IN (%s) AND F.course_id=%d AND F.user_pk='%s' AND F.module IN (%s) GROUP BY D.id ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks, course_id, user_id, assessment_types_str)
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    userpageviews_dict = {}
    dataset_list = []
    for row in result:
        userpageviews_dict[row[0]] = row[4]
    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day FROM dim_dates D WHERE D.DATE_week IN (%s) ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks)
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
        if row[0] in userpageviews_dict:
           pageviews = userpageviews_dict[row[0]]
        else:
           pageviews = 0
        datapoint = "[Date.UTC(%s,%s,%s),%s]" % (row[1],str(int(row[2])-1),row[3],pageviews)
        dataset_list.append(datapoint)
    dataset = ','.join(map(str, dataset_list))
    return dataset

def get_courseweeks(course_id):
    """
    Returns a list of course weeks
    Todo: Calculate from course table start and end date

    Args:
        course_id: course id

    Returns:
        list of course week numbers
    """
    return [31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44]

def get_indivuserweekcount_for_specificallcoursepage(week, course_id, module, action, user_id):
    cursor = connections['olap'].cursor()
    usercounts = 0
    sql = "SELECT COUNT(F.user_pk) AS usercount FROM fact_coursevisits F INNER JOIN dim_dates D ON F.Date_Id = D.Id WHERE F.course_id=%d AND D.date_week=%d AND F.module='%s' AND F.action='%s AND F.user_id=%d';" % (course_id, week, module, action, user_id)
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
           if row[0] is not None:
                usercounts = int(row[0])
    return int(usercounts)

def get_indivuser_weekcount(week,pageid, course_id, content_type, order_no, user_id):
    cursor = connections['olap'].cursor()
    usercounts = 0
    pageid2 = str(pageid)
    pageid2 = int(pageid2[0:(len(pageid2)-2)])
    sql = ""
    if content_type=="section":
        sql = "SELECT COUNT(F.user_pk) AS usercount FROM fact_coursevisits F INNER JOIN dim_dates D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.section_order=%d AND F.user_id=%d AND D.date_week=%d;" % (course_id, int(order_no), user_id, week)
    else:
        sql = "SELECT COUNT(F.user_pk) AS usercount FROM fact_coursevisits F INNER JOIN dim_dates D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.page_id=%d AND F.user_id=%d AND D.date_week=%d;" % (course_id, int(pageid2), user_id, week)
    row_count = cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
           if row[0] is not None:
                usercounts = int(row[0])
    return int(usercounts)
