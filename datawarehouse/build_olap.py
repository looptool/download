"""
    Loop Data Warehouse creation script

    This script processes the log (.csv) and course export file (.zip) for each course that is specified in config.json.
    Blackboard and Moodle course data is transformed into the same structure internally which means that the analytics and
    dashboard code is the same for both.
    Assumes that the zips are unzipped and that log files are called log.csv and that both are placed in the same folder for
    each course.
    OLAP and summary tables are then created in a mysql database. The cloop_olap.sql file contains the sql to create the database.
    Star schema is used to make queries across dimensions easy:
     - Pageviews are stored in a fact table (i.e., fact_coursevisits)
     - Dates, Pages and Users are stored in Dimension tables
    All HTML tables included in the dashboard are cached in the summary_courses table.

    Different code however has been written for extracting the data for Blackboard and Moodle. Blackboard uses the IMS-CP format while
    Moodle has its own format. These formats were reverse engineered for the project.
    Blackboard and MoodleMacquarie formats need a csv log file.
    Moodle from UniSA only needs the course export format.

    High level overview of processing from course export file:
    - Extract users and insert into fact table
    - Extract course structure from zip and insert into content dimension table
    - Extract forum posts
    - Extract quiz attempts and scores

    Todo:
    - Use Celery for task processing and cron
    - Update Pandas to use Dask for larger out of memory data processing
    - Clean up Blackboard manifest code processing
    - Import all users for processing - at the moment for the trial only students are imported
    - Move database to Postgres
"""

import sys
from sqlalchemy.engine import create_engine
import re
import datetime
import time
import csv
import random
import xml.etree.cElementTree as ET
import os.path
import urlparse
import unicodedata
import pandas as pd
import numpy
import json

cache = {}
connection = None

def main():
    global engine
    global connection
    global DayL
    global course_id
    global course_startdate
    global course_enddate
    global course_type
    global treetable
    global course_weeks
    global course_event
    global content_hidden_list
    global forum_hidden_list
    global submission_hidden_list
    global staff_list
    global tree_table_totals
    global communication_types
    global assessment_types
    global msg_to_forum_dict
    global df_treetable
    global sitetree

    DayL = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']

    # load config.json
    course_config = None
    with open('config.json') as data_file:
        course_config = json.load(data_file)

    staff_list = []
    sitetree = {}

    engine = create_engine('mysql://root:root@localhost/cloop_olap?unix_socket=/Applications/MAMP/tmp/mysql/mysql.sock')
    course_exports_path = '/Users/aneesha/cloop/olap/data/'

    connection = engine.connect()

    print str(datetime.datetime.now())

    print "Cleanup_database - delete all records"
    cleanup_database()

    print "Generate Dates"
    generate_dates()

    for config in course_config:
        course_id = config['course_id']
        course_type = config['course_type']
        week_startdate = config['start_date']
        week_enddate = config['end_date']
        course_weeks = get_weeks(week_startdate, week_enddate)

        tree_table_totals = [0] * len(course_weeks)

        #Create communication and assessment content type mappings
        if course_type == "Moodle":
            communication_types = ['forum']
            assessment_types = ['quiz', 'assign']
        elif course_type == "MoodleMacquarie":
            communication_types = ['forum_discussions', 'oublog', 'forum', 'oublog_posts', 'forum_posts', 'dialogue', 'dialogue_conversations']
            assessment_types = ['quiz','quiz_attempts','grade_grades','turnitintool', 'assign']
        elif course_type == "Blackboard":
            communication_types = ['resource/x-bb-discussionboard', 'course/x-bb-collabsession', 'resource/x-bb-discussionfolder']
            assessment_types = ['assessment/x-bb-qti-test', 'course/x-bb-courseassessment', 'resource/x-turnitin-assignment']

        add_coursesummarytable(course_id)

        print "Processing Course ID: ", course_id
        process_exportzips(course_id, course_type, course_exports_path)

    print str(datetime.datetime.now())

def cleanup_database():
    """
        Deletes all records in the database.
    """
    # First the OLAP Tables
    connection.execute("DELETE FROM dim_users");
    connection.execute("DELETE FROM fact_coursevisits");
    connection.execute("DELETE FROM dim_dates");
    connection.execute("DELETE FROM dim_sessions");
    connection.execute("DELETE FROM dim_session");
    connection.execute("DELETE FROM dim_pages");
    connection.execute("DELETE FROM dim_submissionattempts");
    connection.execute("DELETE FROM dim_submissiontypes");

    # Next cleanup the Summary Tables
    connection.execute("DELETE FROM Summary_Courses");
    connection.execute("DELETE FROM summary_forum");
    connection.execute("DELETE FROM summary_discussion");
    connection.execute("DELETE FROM summary_posts");
    connection.execute("DELETE FROM Summary_CourseVisitsByDayInWeek");
    connection.execute("DELETE FROM Summary_CourseCommunicationVisitsByDayInWeek");
    connection.execute("DELETE FROM Summary_CourseAssessmentVisitsByDayInWeek");
    connection.execute("DELETE FROM Summary_SessionsByDayInWeek");
    connection.execute("DELETE FROM Summary_SessionAverageLengthByDayInWeek");
    connection.execute("DELETE FROM Summary_SessionAveragePagesPerSessionByDayInWeek");
    connection.execute("DELETE FROM Summary_ParticipatingUsersByDayInWeek");
    connection.execute("DELETE FROM Summary_UniquePageViewsByDayInWeek");

def generate_dates():
    """
        Generates dates for the date dimension table
    """
    start_date =  datetime.datetime.strptime("1-JAN-14", "%d-%b-%y")
    end_date =  datetime.datetime.strptime("31-DEC-16", "%d-%b-%y")

    cur_date = start_date
    while (cur_date <= end_date):
        insert_date(cur_date.year, cur_date.month, cur_date.day, cur_date.weekday(), cur_date.isocalendar()[1])
        cur_date = cur_date + datetime.timedelta(days = +1)

def insert_date(year, month, day, dayinweek, week):
    """
    Inserts a date in the dim_dates dimension table

    Args:
        year: 4 digit year
        month: 1 - 12 month value
        day: day of the month
        dayinweek: 0 - 6 day in week
        week: week in year value

    Returns:
        id of the inserted date
    """
    row = { }
    prefix = "date"

    insert_date = datetime.date(int(year), int(month), int(day));

    if insert_date != None:
        row["id"] = sanitize(datetime.datetime.strftime(insert_date, "%d/%b/%y"))
        row[prefix + "_year"] = year
        row[prefix + "_month"] = month
        row[prefix + "_day"] = day
        row[prefix + "_dayinweek"] = dayinweek
        row[prefix + "_week"] = week

        if row[prefix + "_month"] == 12 and row[prefix + "_week"] <= 1:
            row[prefix + "_week"] = 52
        if row[prefix + "_month"] == 1 and row[prefix + "_week"] >= 52:
            row[prefix + "_week"] = 1

        unixtimestamp = time.mktime(datetime.datetime.strptime(row["id"], "%d-%b-%y").timetuple())
        row['unixtimestamp'] = unixtimestamp

    return save_object ("dim_dates", row)

def add_coursesummarytable(course_id):
    """
        Inserts a record for the course_id in the summary_courses table

        Args:
            course_id: course id
    """
    row = {"course_id": course_id}
    save_summaryobject ("summary_courses", row)

def process_exportzips(course_id, course_type, course_exports_path):
    """
        Determines if the course is from Moodle or Blackboard and runs the associated methods to process the course

        Args:
            course_id: course id
            course_type: Blackboard or Moodle
            course_exports_path: path to the export folder
    """
    global treetable
    global staff_list
    global df_treetable

    course_export_path = "%s%d" %(course_exports_path, course_id)
    course_export_normpath = os.path.normpath(course_export_path)

    if course_type=="Blackboard":
        print "Import Blackboard Manifest Tree"
        user_xmlfile, id_type_dict, usermsgstate_resourcefile, announcements_id, gradebook_id, content_link_id_to_content_id_dict = process_IMSCP_Manifest_Blackboard(course_export_normpath, course_id)

        print "Import Users"
        userxml_export_path = "%s%d/%s" %(course_exports_path, course_id, user_xmlfile)
        populate_dim_users_table(userxml_export_path, course_type, course_id)

        print "Process Access Log"
        log_export_path = "%s%d/%s" %(course_exports_path, course_id,"log.csv")
        process_accesslog(log_export_path, course_type, course_id, announcements_id, gradebook_id, content_link_id_to_content_id_dict, idtypemap=id_type_dict)
    else:
        print "Moodle"

        print "Import Users"
        populate_dim_users_table(course_export_normpath, course_type, course_id)

        print "Process Access Log"
        process_accesslog(course_export_normpath, course_type, course_id)

        course_export_acivitiespath = "%s%d/activities" %(course_exports_path, course_id)
        course_export_activitiesnormpath = os.path.normpath(course_export_acivitiespath)

        print "Process Access Log - Resources"
        process_courseresources(course_export_activitiesnormpath, course_id, course_type)

        course_export_sectionspath = "%s%d/sections" %(course_exports_path, course_id)
        course_export_sectionsnormpath = os.path.normpath(course_export_sectionspath)

        process_moodle_sections(course_export_sectionsnormpath, course_id, course_type)

    print "Processing Session Calculations"
    process_user_sessions(course_id)

    print "Processing Summary Tables"
    # Populate Summary Tables
    populate_summarytables(course_id)

    print "Processing Count and Vis Tables"
    # Populate Tables for Course Access Counts and Visualisations
    generate_studentbyweektable(course_id,"counts")
    generate_studentbyweektable(course_id,"vis")

    weeks_str_list = ','.join(map(str, course_weeks))
    contentsql = "SELECT F.pageview, D.date_week, F.module, F.action, F.page_id, F.section_order, F.course_id, F.user_pk FROM Fact_Coursevisits F INNER JOIN Dim_Dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d AND D.Date_week IN (%s);" % (course_id, weeks_str_list)
    newconnection = engine.raw_connection()
    # store as pandas dataframe; keep global to speed up processing
    df_treetable = pd.read_sql(contentsql, newconnection)

    print "ProcessingTreetables processing - Counts"
    treetable = ""
    generate_contentaccesstreetable(course_id,"counts")

    print "Processing Treetables processing - Users"
    treetable = ""
    generate_contentaccesstreetable(course_id,"user")

    print "Processing Communication tables"
    generate_contentaccesstable(course_id, "communication", "user", "communication_counts_table")
    generate_contentaccesstable(course_id, "communication", "count", "communication_user_table")

    print "Processing Forum Posts"
    generate_contentaccesstable(course_id, "forum", "posts", "forum_posts_table")

    print "Processing Assessment tables"
    generate_contentaccesstable(course_id, "submmision", "user", "assessment_counts_table")
    generate_contentaccesstable(course_id, "submmision", "count", "assessment_user_table")

    print "Processing Timelines Summary"
    gen_coursepageviews(course_id)

    print "Processing Assessment Grades for each student"
    generate_assessmentgrades(course_id, "assessmentgrades")

def populate_summarytables(course_id):
    """
        Produces summary tables Summary_CourseVisitsByDayInWeek, Summary_CourseCommunicationVisitsByDayInWeek
        Summary_CourseAssessmentVisitsByDayInWeek, Summary_SessionAverageLengthByDayInWeek, Summary_SessionAveragePagesPerSessionByDayInWeek
        Summary_ParticipatingUsersByDayInWeek, Summary_UniquePageViewsByDayInWeek

        Args:
            course_id: course id
    """
    global course_weeks
    global communication_types
    global assessment_types

    course_weeks_str = str(course_weeks).strip('[]')

    excluse_contentype_list = communication_types + assessment_types
    excluse_contentype_str =  ','.join("'{0}'".format(x) for x in excluse_contentype_list)

    communication_types_str = ','.join("'{0}'".format(x) for x in communication_types)
    assessment_types_str = ','.join("'{0}'".format(x) for x in assessment_types)

    # Populate Summary_CourseVisitsByDayInWeek - only contains content items
    trans = connection.begin()
    Summary_CourseVisitsByDayInWeek_sql = "SELECT D.Date_Year, D.Date_Day, D.Date_Week, D.Date_dayinweek, SUM(F.pageview) AS Pageviews, F.course_id FROM Dim_Dates  D LEFT JOIN Fact_Coursevisits F ON D.Id = F.Date_Id WHERE D.Date_dayinweek IN (0,1,2,3,4,5,6) AND D.DATE_week IN (%s) AND F.course_id=%d AND F.module NOT IN (%s) GROUP BY D.Date_Week, D.Date_dayinweek" %(course_weeks_str, course_id, excluse_contentype_str)
    result = connection.execute(Summary_CourseVisitsByDayInWeek_sql);
    for row in result:
            pageview = row[4] if row[4] is not None else 0
            record = {"Date_Year": row[0], "Date_Day": row[1], "Date_Week": row[2], "Date_dayinweek": row[3], "pageviews": pageview, "course_id": course_id}
            save_summaryobject ("Summary_CourseVisitsByDayInWeek", record)
    trans.commit()

    # Populate Summary_CourseCommunicationVisitsByDayInWeek - only contains forums
    trans = connection.begin()
    Summary_CourseCommunicationVisitsByDayInWeek_sql = "SELECT D.Date_Year, D.Date_Day, D.Date_Week, D.Date_dayinweek, SUM(F.pageview) AS Pageviews, F.course_id FROM Dim_Dates  D LEFT JOIN Fact_Coursevisits F ON D.Id = F.Date_Id WHERE D.Date_dayinweek IN (0,1,2,3,4,5,6) AND D.DATE_week IN (%s) AND F.course_id=%d AND F.module IN (%s) GROUP BY D.Date_Week, D.Date_dayinweek" %(course_weeks_str, course_id, communication_types_str)
    result = connection.execute(Summary_CourseCommunicationVisitsByDayInWeek_sql);
    for row in result:
            pageview = row[4] if row[4] is not None else 0
            record = {"Date_Year": row[0], "Date_Day": row[1], "Date_Week": row[2], "Date_dayinweek": row[3], "pageviews": pageview, "course_id": course_id}
            save_summaryobject ("Summary_CourseCommunicationVisitsByDayInWeek", record)
    trans.commit()

    # Populate Summary_CourseAssessmentVisitsByDayInWeek - only contains quiz and assign
    trans = connection.begin()
    Summary_CourseAssessmentVisitsByDayInWeek_sql = "SELECT D.Date_Year, D.Date_Day, D.Date_Week, D.Date_dayinweek, SUM(F.pageview) AS Pageviews, F.course_id FROM Dim_Dates  D LEFT JOIN Fact_Coursevisits F ON D.Id = F.Date_Id WHERE D.Date_dayinweek IN (0,1,2,3,4,5,6) AND D.DATE_week IN (%s) AND F.course_id=%d  AND F.module IN (%s) GROUP BY D.Date_Week, D.Date_dayinweek" %(course_weeks_str, course_id, assessment_types_str)
    result = connection.execute(Summary_CourseAssessmentVisitsByDayInWeek_sql);
    for row in result:
            pageview = row[4] if row[4] is not None else 0
            record = {"Date_Year": row[0], "Date_Day": row[1], "Date_Week": row[2], "Date_dayinweek": row[3], "pageviews": pageview, "course_id": course_id}
            save_summaryobject ("Summary_CourseAssessmentVisitsByDayInWeek", record)
    trans.commit()

    # Populate Summary_SessionAverageLengthByDayInWeek
    trans = connection.begin()
    Summary_SessionAverageLengthByDayInWeek_sql = "SELECT S.Date_Year, S.Date_Week, S.Date_dayinweek, AVG(S.session_length_in_mins), S.course_id FROM Dim_Session S WHERE S.Date_dayinweek IN (0,1,2,3,4,5,6) AND S.DATE_week IN (%s) AND S.course_id=%d GROUP BY S.Date_Week, S.Date_dayinweek" %(course_weeks_str, course_id)
    result = connection.execute(Summary_SessionAverageLengthByDayInWeek_sql);
    for row in result:
            session_average_in_minutes = row[3] if row[3] is not None else 0
            record = {"Date_Year": row[0], "Date_Week": row[1], "Date_dayinweek": row[2], "session_average_in_minutes": session_average_in_minutes, "course_id": course_id}
            save_summaryobject ("Summary_SessionAverageLengthByDayInWeek", record)
    trans.commit()

    # Populate Summary_SessionAveragePagesPerSessionByDayInWeek
    trans = connection.begin()
    Summary_SessionAveragePagesPerSessionByDayInWeek_sql = "SELECT S.Date_Year, S.Date_Week, S.Date_dayinweek, AVG(S.pageviews), S.course_id FROM Dim_Session S WHERE S.Date_dayinweek IN (0,1,2,3,4,5,6) AND S.DATE_week IN (%s) AND S.course_id=%d GROUP BY S.Date_Week, S.Date_dayinweek" %(course_weeks_str, course_id)
    result = connection.execute(Summary_SessionAveragePagesPerSessionByDayInWeek_sql);
    for row in result:
            pages_per_session = row[3] if row[3] is not None else 0
            record = {"Date_Year": row[0], "Date_Week": row[1], "Date_dayinweek": row[2], "pages_per_session": pages_per_session, "course_id": course_id}
            save_summaryobject ("Summary_SessionAveragePagesPerSessionByDayInWeek", record)
    trans.commit()

    # Populate Summary_SessionsByDayInWeek
    trans = connection.begin()
    Summary_SessionsByDayInWeek_sql = "SELECT S.Date_Year, S.Date_Week, S.Date_dayinweek, COUNT(DISTINCT S.session_id) AS Session, S.course_id FROM Dim_Session S WHERE S.Date_dayinweek IN (0,1,2,3,4,5,6) AND S.DATE_week IN (%s) AND S.course_id=%d GROUP BY S.Date_Week, S.Date_dayinweek" %(course_weeks_str, course_id)
    result = connection.execute(Summary_SessionsByDayInWeek_sql);
    for row in result:
            sessions = row[3] if row[3] is not None else 0
            record = {"Date_Year": row[0], "Date_Week": row[1], "Date_dayinweek": row[2], "sessions": sessions, "course_id": course_id}
            save_summaryobject ("Summary_SessionsByDayInWeek", record)
    trans.commit()

    # Populate Summary_ParticipatingUsersByDayInWeek
    trans = connection.begin()
    Summary_ParticipatingUsersByDayInWeek_sql = "SELECT D.Date_Year, D.Date_Day, D.Date_Week, D.Date_dayinweek, SUM(F.pageview) AS pageviews, F.course_id FROM Dim_Dates D LEFT JOIN Fact_Coursevisits F ON D.Id = F.Date_Id WHERE D.Date_dayinweek IN (0,1,2,3,4,5,6) AND D.DATE_week IN (%s) AND F.course_id=%d  GROUP BY D.Date_Week, D.Date_dayinweek;" % (course_weeks_str, course_id)
    result = connection.execute(Summary_ParticipatingUsersByDayInWeek_sql);
    for row in result:
            pageview = row[4] if row[4] is not None else 0
            record = {"Date_Year": row[0], "Date_Day": row[1], "Date_Week": row[2], "Date_dayinweek": row[3], "pageviews": pageview, "course_id": course_id}
            save_summaryobject ("Summary_ParticipatingUsersByDayInWeek", record)
    trans.commit()

    # Populate Summary_UniquePageViewsByDayInWeek
    trans = connection.begin()
    Summary_UniquePageViewsByDayInWeek_sql = "SELECT D.Date_Year, D.Date_Day, D.Date_Week, D.Date_dayinweek, COUNT(DISTINCT F.page_id) AS UniquePageViews, F.course_id FROM Fact_Coursevisits F INNER JOIN Dim_Dates D ON F.Date_Id = D.Id WHERE D.Date_dayinweek IN (0,1,2,3,4,5,6) AND D.DATE_week IN (%s) AND F.course_id=%d  GROUP BY D.Date_Week, D.Date_dayinweek;" % (course_weeks_str, course_id)
    result = connection.execute(Summary_UniquePageViewsByDayInWeek_sql);
    for row in result:
            pageview = row[4] if row[4] is not None else 0
            record = {"Date_Year": row[0], "Date_Day": row[1], "Date_Week": row[2], "Date_dayinweek": row[3], "pageviews": pageview,  "course_id": course_id}
            save_summaryobject ("Summary_UniquePageViewsByDayInWeek", record)
    trans.commit()

"""
    Session Processing
"""
def process_user_sessions(course_id):
    """
        Gets each user (i.e., student) and calls method to process to split visits into a session.
        A session includes all pageviews where the time access difference does not exceed 40 mins

        Args:
            course_id: course id
    """
    res_sql = "SELECT DISTINCT lms_id FROM dim_users WHERE course_id=%d;" %(course_id)
    result = connection.execute(res_sql);
    rows = result.fetchall()

    for row in rows:
        user_pk = int(row[0])
        determine_user_sessions(user_pk)

def determine_user_sessions(user_pk):
    """
        Split visits for a user into a session.
        A session includes all pageviews where the time access difference does not exceed 40 mins
        Algorithm to Determine Sessions:
            get all hits for a user in order from database (user_pk)
            sessionid = 1
            sessionupdatelist = []
            loop through (starting at i=0) (all hits for a user)
                sessionupdatelist append counter
                get date time stamp for i=counter
                get date time stamp for i=counter + 1
                timediff = (date time stamp for i=counter + 1) - (date time stamp for i=counter)
                if timediff > 40 mins
                    update all id's that match sessionupdatelist with sessionid in database
                    clear sessionupdatelist
                    update sessionid by 1
        Args:
            user_pk: user id in loop
    """
    trans = connection.begin()
    sessionid = getmax_sessionid()
    sessionupdatelist = []
    sessionupdatelist_i = []
    res_sql = "SELECT id, time_id, date_id, course_id, unixtimestamp, date_id FROM fact_coursevisits WHERE user_id=%d order by unixtimestamp ASC;" %(user_pk)
    result = connection.execute(res_sql);
    rows = result.fetchall()
    total_hits = len(rows)
    timediff_mins = 0

    for (i, row) in enumerate(rows):
        current_id = row[0]
        current_time = row[1]
        current_date = row[2]
        current_date_time = current_date + " " + current_time
        sessionupdatelist.append(current_id)
        sessionupdatelist_i.append(i)
        if i < (total_hits-1):
            next_id = rows[i+1][0]
            next_time = rows[i+1][1]
            next_date = rows[i+1][2]
            next_date_time = next_date + " " + next_time

            fmt = '%d-%b-%y %H:%M:%S'
            d1 = datetime.datetime.strptime(current_date_time, fmt)
            d2 = datetime.datetime.strptime(next_date_time, fmt)

            # convert to unix timestamp
            d1_ts = time.mktime(d1.timetuple())
            d2_ts = time.mktime(d2.timetuple())

            # they are now in seconds, subtract and then divide by 60 to get minutes.
            timediff_mins = int(d2_ts-d1_ts) / 60

        else:
            timediff_mins = 40.0
        if (timediff_mins >= 40.0):
            sessionupdatelist_string = ','.join(map(str, sessionupdatelist))
            session_start = int(float(rows[sessionupdatelist_i[0]][4]))
            session_end = int(float(rows[sessionupdatelist_i[len(sessionupdatelist_i)-1]][4]))
            session_length_in_mins = int(session_end-session_start) / 60
            pageviews = len(sessionupdatelist_i)
            dt = datetime.datetime.fromtimestamp(session_start)
            date_year = dt.year
            date_day = dt.day
            date_week = dt.isocalendar()[1]
            date_dayinweek = dt.weekday()
            updatesql = "UPDATE fact_coursevisits SET session_id=%d WHERE id IN (%s);" %(sessionid, sessionupdatelist_string)
            updateresult = connection.execute(updatesql);
            record = {"user_id": user_pk, "date_week": date_week, "date_year":date_year, "date_dayinweek": date_dayinweek, "pageviews": pageviews, "session_length_in_mins": session_length_in_mins, "session_id":sessionid, "course_id": row[3], "unixtimestamp": row[4], "date_id": row[5] }
            save_summaryobject ("dim_session", record)

            sessionupdatelist = []
            sessionupdatelist_i = []
            sessionid = sessionid + 1
    trans.commit()

def calculate_sessionlengthandnopageviews(session_id, course_id):
    """
        Calculates the session length in minutes and the no of pageviews in the session.
        Stores the result in the dim_sessions table
        Args:
            session_id: session id
            course_id: course id
    """
    res_sql = "SELECT F.unixtimestamp, D.Date_Week, D.Date_Year, D.Date_DayInWeek FROM fact_coursevisits F LEFT JOIN dim_dates D ON F.Date_Id = D.Id LEFT JOIN dim_session S ON F.ID = S.fact_coursevisits_id WHERE S.session_id=%s AND F.course_id=%d order by F.unixtimestamp ASC;" %(session_id, course_id)
    result = connection.execute(res_sql);
    rows = result.fetchall()
    total_pageviews_in_session = len(rows)
    timediff_mins = 0
    session_start = int(float(rows[0][0]))
    session_end = int(float(rows[total_pageviews_in_session-1][0]))
    session_length_in_mins = int(session_end-session_start) / 60
    date_week = rows[0][1]
    date_year = rows[0][2]
    date_dayinweek = rows[0][3]
    record = {"date_week": date_week, "date_year":date_year, "date_dayinweek": date_dayinweek, "session_id": session_id, "course_id": course_id, "session_length_in_mins": session_length_in_mins, "pageviews": total_pageviews_in_session}
    save_summaryobject ("dim_sessions", record)

def getmax_sessionid():
    """
        Gets the maximim session id + 1
        Returns:
            int with next session id to use
    """
    res_sql = "SELECT max(session_id) FROM dim_session;"
    result = connection.execute(res_sql);
    rows = result.fetchall()
    max_val = rows[0][0]
    if (max_val is None):
        return 1
    else:
        return int(max_val) + 1

"""
    Dashboard Timeseries Processing
"""

def get_contentpageviews_dataset(content_id, course_id, weeks):
    """
        Calculates timeseries data for each content item.
        Args:
            content_id: content id
            course_id: course id
            weeks: list of weeks to produce the timeseries for
        Returns:
            json string for javascript rendering of graphs in highchart
    """
    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day,SUM(F.pageview) AS Pageviews FROM Dim_Dates D LEFT JOIN Fact_Coursevisits F ON D.Id = F.Date_Id WHERE D.DATE_week IN (%s) AND F.course_id=%d AND F.page_id=%d GROUP BY D.id;" %(weeks, course_id, content_id)
    result = connection.execute(sql);
    pagepageviews_dict = {}
    dataset_list = []
    for row in result:
        pagepageviews_dict[row[0]] = row[4]
    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day FROM Dim_Dates D WHERE D.DATE_week IN (%s)" %(weeks)
    result = connection.execute(sql);
    for row in result:
        if row[0] in pagepageviews_dict:
           pageviews = pagepageviews_dict[row[0]]
        else:
           pageviews = 0
        datapoint = "[Date.UTC(%s,%s,%s),%s]" % (row[1],row[2],row[3],pageviews)
        dataset_list.append(datapoint)
    dataset = ','.join(map(str, dataset_list))
    return dataset

def get_userpageviews_dataset(user_id, course_id, weeks):
    """
        Calculates timeseries data for each user.
        Args:
            user_id: user id
            course_id: course id
            weeks: list of weeks to produce the timeseries for
        Returns:
            json string for javascript rendering of graphs in highchart
    """
    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day, SUM(F.pageview) AS Pageviews FROM Dim_Dates D LEFT JOIN Fact_Coursevisits F ON D.Id = F.Date_Id WHERE D.DATE_week IN (%s) AND F.course_id=%d AND F.user_pk='%s' GROUP BY D.id;" %(weeks, course_id, user_id)
    result = connection.execute(sql);
    userpageviews_dict = {}
    dataset_list = []
    for row in result:
        userpageviews_dict[row[0]] = row[4]
    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day FROM Dim_Dates D WHERE D.DATE_week IN (%s)" %(weeks)
    result = connection.execute(sql);
    for row in result:
        if row[0] in userpageviews_dict:
           pageviews = userpageviews_dict[row[0]]
        else:
           pageviews = 0
        datapoint = "[Date.UTC(%s,%s,%s),%s]" % (row[1],row[2],row[3],pageviews)
        dataset_list.append(datapoint)
    dataset = ','.join(map(str, dataset_list))
    return dataset

def gen_coursepageviews(course_id):
    """
        Generates and saves pageview timelines in a table column.
        Args:
            course_id: course id
    """
    global course_weeks
    weeks = ','.join(map(str, course_weeks))

    dataset = get_coursecontentpageviews_dataset(course_id, weeks)
    update_coursesummarytable(course_id, 'contentcoursepageviews', dataset)

    dataset = get_coursecommunicationpageviews_dataset(course_id, weeks)
    update_coursesummarytable(course_id, 'communicationcoursepageviews', dataset)

    dataset = get_courseassessmentpageviews_dataset(course_id, weeks)
    update_coursesummarytable(course_id, 'assessmentcoursepageviews', dataset)

def get_coursecontentpageviews_dataset(course_id, weeks):
    """
        Calculates timeseries data for content pageviews.
        Args:
            course_id: course id
            weeks: list of weeks to produce the timeseries for
        Returns:
            json string for javascript rendering of graphs in highchart
    """

    global communication_types
    global assessment_types

    excluse_contentype_list = communication_types + assessment_types
    excluse_contentype__str = ','.join("'{0}'".format(x) for x in excluse_contentype_list)

    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day,SUM(F.pageview) AS Pageviews FROM Dim_Dates D LEFT JOIN Fact_Coursevisits F ON D.Id = F.Date_Id WHERE D.DATE_week IN (%s) AND F.course_id=%d AND F.module NOT IN (%s) GROUP BY D.id ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks, course_id, excluse_contentype__str)

    result = connection.execute(sql);
    pagepageviews_dict = {}
    dataset_list = []
    for row in result:
        pagepageviews_dict[row[0]] = row[4]
    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day FROM Dim_Dates D WHERE D.DATE_week IN (%s) ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks)
    result = connection.execute(sql);
    for row in result:
        if row[0] in pagepageviews_dict:
           pageviews = pagepageviews_dict[row[0]]
        else:
           pageviews = 0
        datapoint = "[Date.UTC(%s,%s,%s),%s]" % (row[1],str(int(row[2])-1),row[3],pageviews)
        dataset_list.append(datapoint)
    dataset = ','.join(map(str, dataset_list))
    return dataset

def get_coursecommunicationpageviews_dataset(course_id, weeks):
    """
        Calculates timeseries data for communication pageviews.
        Args:
            course_id: course id
            weeks: list of weeks to produce the timeseries for
        Returns:
            json string for javascript rendering of graphs in highchart
    """
    global communication_types

    communication_types_str = ','.join("'{0}'".format(x) for x in communication_types)

    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day,SUM(F.pageview) AS Pageviews FROM Dim_Dates D LEFT JOIN Fact_Coursevisits F ON D.Id = F.Date_Id WHERE D.DATE_week IN (%s) AND F.course_id=%d AND F.module IN (%s) GROUP BY D.id ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks, course_id, communication_types_str)
    result = connection.execute(sql);
    pagepageviews_dict = {}
    dataset_list = []
    for row in result:
        pagepageviews_dict[row[0]] = row[4]
    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day FROM Dim_Dates D WHERE D.DATE_week IN (%s) ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks)
    result = connection.execute(sql);
    for row in result:
        if row[0] in pagepageviews_dict:
           pageviews = pagepageviews_dict[row[0]]
        else:
           pageviews = 0
        datapoint = "[Date.UTC(%s,%s,%s),%s]" % (row[1],str(int(row[2])-1),row[3],pageviews)
        dataset_list.append(datapoint)
    dataset = ','.join(map(str, dataset_list))
    return dataset

def get_courseassessmentpageviews_dataset(course_id,weeks):
    """
        Calculates timeseries data for assessment pageviews.
        Args:
            course_id: course id
            weeks: list of weeks to produce the timeseries for
        Returns:
            json string for javascript rendering of graphs in highchart
    """
    global assessment_types

    assessment_types_str = ','.join("'{0}'".format(x) for x in assessment_types)

    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day,SUM(F.pageview) AS Pageviews FROM Dim_Dates D LEFT JOIN Fact_Coursevisits F ON D.Id = F.Date_Id WHERE D.DATE_week IN (%s) AND F.course_id=%d AND F.module IN (%s) GROUP BY D.id ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks, course_id, assessment_types_str)
    result = connection.execute(sql);
    pagepageviews_dict = {}
    dataset_list = []
    for row in result:
        pagepageviews_dict[row[0]] = row[4]
    sql = "SELECT D.id, D.Date_Year, D.Date_Month, D.Date_Day FROM Dim_Dates D WHERE D.DATE_week IN (%s) ORDER BY D.date_year, D.date_month, D.date_day;" %(weeks)
    result = connection.execute(sql);
    for row in result:
        if row[0] in pagepageviews_dict:
           pageviews = pagepageviews_dict[row[0]]
        else:
           pageviews = 0
        datapoint = "[Date.UTC(%s,%s,%s),%s]" % (row[1],str(int(row[2])-1),row[3],pageviews)
        dataset_list.append(datapoint)
    dataset = ','.join(map(str, dataset_list))
    return dataset

"""
    Process Assessment Grades
"""

def generate_assessmentgrades(course_id, updatecol):
    """
        Creates the Grades HTML table. A row is included for each student  with columns for each assessment.
        Pandas dataframes are used to speed up processing.
        Args:
            course_id: course id
            updatecol: column to update in the course summary table
    """
    global assessment_types

    assessment_types_str = ','.join("'{0}'".format(x) for x in assessment_types)
    # get all quizzes and assignments
    res_sql = "SELECT title, content_type, content_id, parent_id FROM dim_pages WHERE content_type IN (%s) AND course_id=%d order by order_no;" %(assessment_types_str, course_id)
    result = connection.execute(res_sql);
    rows = result.fetchall()
    titles_list = []
    page_id_list = []
    for page in rows:
        titles_list.append(page[0])
        page_id_list.append(page[2])
    # print table header
    # assignments and quizzes as headings
    table = []
    table.append("<thead><tr>")
    table.append("<th>Name</th>")
    for x in titles_list:
        table.append("<th>%s</th>" % (x))
    table.append("</tr>")
    table.append("</thead>")
    table.append("<tbody>\n")

    # load all quiz scores into Pandas
    quizsql = "SELECT content_id, user_id, grade FROM dim_submissionattempts WHERE course_id=%s;" % (course_id)
    newconnection = engine.raw_connection()
    df = pd.read_sql(quizsql, newconnection)

    # loop over all students
    sql = "SELECT lms_id,firstname,lastname,role FROM dim_users WHERE role='Student' ORDER BY lastname;"  #need to make dynamic for each type of role
    result = connection.execute(sql);
    for row in result:
        table.append("<tr class='stats-row'><td class='stats-title'><span>%s %s</span></td>" % (row[1], row[2]))
        for i in page_id_list:
            filterd_df = df[(df.content_id == int(i)) & (df.user_id == int(row[0]))]
            quiz_score = filterd_df['grade'].max()
            table.append("<td>%s</td>" % (quiz_score))
    table.append("</tr>\n")
    table.append("</tbody>\n")
    table = ''.join(table)

    update_coursesummarytable (course_id, updatecol, table.replace("'", "''"))

def get_usergrade(pageid, course_id, user_id):
    """
    Returns the grade a user received in an assessement item

    Args:
        pageid: content id of the assessment item
        course_id: course id
        user_id: user id

    Returns:
        grade as a float
    """
    avg_grade = 0
    sql = "SELECT max(grade) AS maxgrade FROM dim_submissionattempts WHERE content_id=%s AND course_id=%s AND user_id=%s;" % (pageid, course_id, user_id)
    result = connection.execute(sql);
    for row in result:
           if row[0] is not None:
                avg_grade = row[0]
    rounded_grade = "%.2f" % round(float(avg_grade),2)
    return rounded_grade

"""
    Process Student Access Table
"""
def generate_studentbyweektable(course_id, displaytype):
    """
        Creates a student by weeks table. Uses pandas dataframes to speed up processing
        Args:
            course_id: course id
            displaytype: counts or vis
    """
    global course_weeks
    global week_totals

    week_totals = [0] * len(course_weeks)
    weeks_str_list = ','.join(map(str, course_weeks))

    total_users = get_totalusers(course_id)
    total_pageviews = get_totalpageviews(course_id)

    table = [] # need class="heat-map"
    table.append("<thead><tr>")
    table.append("<th>Firstname</th><th>Lastname</th><th>Account Type</th><th>View</th>")
    for x in range(len(course_weeks)):  #range(0, 10):
        table.append("<th>Week %d</th>" % (x+1))
    table.append("</tr>")
    table.append("</thead>")
    table.append("<tbody>\n")

    useraccesssql = "SELECT F.pageview, D.date_week, F.user_pk FROM Fact_Coursevisits F INNER JOIN Dim_Dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d AND D.Date_week IN (%s);" % (course_id, weeks_str_list)
    newconnection = engine.raw_connection()
    df = pd.read_sql(useraccesssql, newconnection)

    sql = "SELECT user_pk,firstname,lastname,role FROM dim_users WHERE course_id=%d ORDER BY lastname;" %(course_id) #need to make dynamic
    result = connection.execute(sql);
    for row in result:
       table.append("<tr class='stats-row'><td class='stats-title'>%s</td><td class='stats-title'>%s</td><td class='stats-title'>%s</td><td class='stats-title'><a href='/coursemember?user_id=%s&course_id=%s' class='btn btn-small btn-primary'>View</a></td>" % (row[1],row[2],row[3], row[0], course_id))
       totalcounts_dict = {}
       for wk in course_weeks:
           totalcounts_dict[wk] = 0

       filtered_df = df[(df.user_pk == row[0])]
       by_week = filtered_df.groupby(by=['date_week'])['pageview'].sum()
       for ind, dfrow in by_week.iteritems():
          totalcounts_dict[ind] = dfrow

       if displaytype=="vis":
           table.append('<td>&nbsp;</td>')
       else:
           table.append(makeweek_rows(course_id, course_weeks,totalcounts_dict,total_users,total_pageviews,userpercent=False ))

       table.append("</tr>\n")

    table.append("<tr class='stats-row'><td class='stats-title'></td><td class='stats-title'></td><td class='stats-title'></td><td class='stats-title'></td><td class='stats-title'>Total</td>")
    for week in week_totals:
        table.append('<td class="stats-title">%d</td>' % (week))

    table.append("</tbody>\n")

    table = ''.join(table)

    if displaytype == 'counts':
        update_coursesummarytable (course_id, 'users_counts_table', table.replace("'", "''"))
    else:
        update_coursesummarytable (course_id, 'users_vis_table', table.replace("'", "''"))

"""
    Process Content, Assessment and Communication Tables
"""

def generate_contentaccesstable(course_id, contentdisplaytype, displaytype, updatecol):
    """
        Generate and saves HTML tables for content assess tables.
        Args:
            course_id: course id
            contentdisplaytype: communication, submission or communication
            displaytype: counts or user
            updatecol: column to update in summary_courses table
    """
    global course_weeks
    global forum_hidden_list
    global submission_hidden_list
    global communication_types
    global assessment_types

    global week_totals

    forum_hidden_list = ""
    submission_hidden_list = ""

    weeks_str_list = ','.join(map(str, course_weeks))

    week_totals = [0] * len(course_weeks)

    total_users = get_totalusers(course_id)
    total_pageviews = get_totalpageviews(course_id)

    hidden_list = ""
    if (contentdisplaytype == "communication" or contentdisplaytype == "forum"):
        #contenttype = "'forum'"
        contenttype = ','.join("'{0}'".format(x) for x in communication_types)
        hidden_list = forum_hidden_list
    else:
        #contenttype = "'quiz','assign'"
        contenttype = ','.join("'{0}'".format(x) for x in assessment_types)
        hidden_list = submission_hidden_list

    percent_calc_text = ""
    if displaytype == "user":
        percent_calc_text = "Percentage of students"
    else:
        percent_calc_text = "Percentage in relation to total pageviews"
    table = []
    table.append("<thead><tr>")
    table.append("<th>Name</th><th>Type</th><th>View</th>")
    for x in range(len(course_weeks)):
        table.append("<th>Week %d</th>" % (x+1))
    table.append("<th>Total</th><th>&#37;<a href='#' class='btn btn-mini' data-rel='tooltip' data-original-title='%s'><i class='halflings-icon info-sign'></i></a></th>" % (percent_calc_text))
    table.append("</tr>")
    table.append("</thead>")
    table.append("<tbody>\n")

    # add rows for pages that show all quizzes, forums and assignments
    # code can be reduced in size by processing as a list
    if course_type == "Moodle":
        if (contentdisplaytype == "communication"):
            table.append("<tr class='stats-row'><td class='stats-title'><span>All Forums</span> <a href='#' class='btn btn-mini' data-rel='tooltip' data-original-title='All Forums refers to views of the page that has links to each discussion forum'><i class='halflings-icon info-sign'></i></a></td><td class='stats-title'>Forum</td><td class='stats-title'></td>")
            #for week in course_weeks:
            if displaytype == "user":
                user_counts_list = get_userweekcount_for_specificallcoursepage(weeks_str_list, course_id, "forum", "view all")
                table.append(makeweek_rows(course_id,course_weeks,user_counts_list,total_users,total_pageviews, userpercent=True))
            else:
                total_counts_list = get_weekcount_for_specificallcoursepage(weeks_str_list, course_id, "forum", "view all")
                table.append(makeweek_rows(course_id,course_weeks,total_counts_list,total_users,total_pageviews, userpercent=False))
            table.append("</tr>\n")

        if (contentdisplaytype == "submmision"):
            table.append("<tr class='stats-row'><td class='stats-title'><span>All Quizzes</span> <a href='#' class='btn btn-mini' data-rel='tooltip' data-original-title='All Quizzes refers to views of the page that has links to each quiz'><i class='halflings-icon info-sign'></i></a></td><td class='stats-title'>Quiz</td><td class='stats-title'></td>")

            if displaytype == "user":
                user_counts_dict = get_userweekcount_for_specificallcoursepage(weeks_str_list, course_id, "quiz", "view all")
                table.append(makeweek_rows(course_id,course_weeks,user_counts_dict,total_users,total_pageviews, userpercent=True))
            else:
                total_counts_dict = get_weekcount_for_specificallcoursepage(weeks_str_list, course_id, "quiz", "view all")
                table.append(makeweek_rows(course_id,course_weeks,total_counts_dict,total_users,total_pageviews, userpercent=False))
            table.append("</tr>\n")
            table.append("<tr class='stats-row'><td class='stats-title'><span>All Assignments</span> <a href='#' class='btn btn-mini' data-rel='tooltip' data-original-title='All Assignments refers to views of the page that has links to each assignment'><i class='halflings-icon info-sign'></i></a></td><td class='stats-title'>Assign</td><td class='stats-title'></td>")

            if displaytype == "user":
                user_counts_dict = get_userweekcount_for_specificallcoursepage(weeks_str_list, course_id, "assign", "view all")
                table.append(makeweek_rows(course_id,course_weeks,user_counts_dict,total_users,total_pageviews, userpercent=True))
            else:
                total_counts_dict = get_weekcount_for_specificallcoursepage(weeks_str_list, course_id, "assign", "view all")
                table.append(makeweek_rows(course_id, course_weeks,total_counts_dict,total_users,total_pageviews, userpercent=False))
            table.append("</tr>\n")

    res_sql = "SELECT title, content_type, content_id, parent_id FROM dim_pages WHERE content_type IN (%s) AND course_id=%d order by order_no;" %(contenttype, course_id)
    result = connection.execute(res_sql);
    rows = result.fetchall()

    for row in rows:
        pageid2 = int(row[2])

        table.append("<tr class='stats-row'><td class='stats-title'><span>%s</span></td><td class='stats-title'>%s</td><td class='stats-title'><a href='/coursepage?course_id=%d&page_id=%s' class='btn btn-small btn-primary'>View</a></td>" % (row[0], row[1], course_id, str(pageid2)))

        if displaytype == "user":
            user_counts_dict = get_userweekcount(weeks_str_list, row[2],course_id, "","")
            table.append(makeweek_rows(course_id,course_weeks,user_counts_dict,total_users,total_pageviews, userpercent=True))
        elif displaytype=='posts':
            posts_dict = get_posts(weeks_str_list, row[2],course_id)
            table.append(makeweek_rows(course_id,course_weeks,posts_dict,total_users,total_pageviews, userpercent=False))
        else:
            total_counts_dict = get_weekcount(weeks_str_list, row[2],course_id,"","")
            table.append(makeweek_rows(course_id,course_weeks,total_counts_dict,total_users,total_pageviews, userpercent=False))
        table.append("</tr>\n")

    table.append("<tr class='stats-row'><td class='stats-title'><span>Total</span></td><td class='stats-title'></td><td class='stats-title'></td>")
    for week in week_totals:
        table.append('<td>%d</td>' % (week))
    table.append('<td>&nbsp;</td><td>&nbsp;</td>')
    table.append("</tr>\n")
    table.append("</tbody>\n")
    table = ''.join(table)

    update_coursesummarytable (course_id, updatecol, table.replace("'", "''"))

def makeweek_rows(course_id,weeks,count_dict,total_users,total_pageviews, userpercent=False):
    global week_totals

    row = ""
    i = 0
    rowcount = 0
    total = 0
    for week in weeks:
        val = 0
        if week in count_dict:
            val = count_dict[week]
            rowcount = rowcount + val
        row = row + '<td>%d</td>' % (val)
        week_totals[i] = week_totals[i]  + val
        i = i + 1
    percent = 0.0
    if (userpercent==True):
        percent = float(rowcount)*(100.0/total_users)
    else:
        percent = float(rowcount)*(100.0/total_pageviews)
    row = row + "<td class='stats-title'>%d</td>" % (rowcount)
    row = row + '<td>%.4f</td>' % (round(percent,4))

    return row

def generate_contentaccesstreetable(course_id, displaytype):
    """
        Generate and saves HTML tree table for content assess. This is the expandable treetable
        Args:
            course_id: course id
            updatecol: column to update in summary_courses table
    """
    global treetable
    global course_weeks
    global week_totals
    global sitetree

    sitetree = []
    week_totals = [0] * len(course_weeks)

    weeks_str_list = ','.join(map(str, course_weeks))

    total_users = get_totalusers(course_id)
    total_pageviews = get_totalpageviews(course_id)

    treetable = []
    treetable.append("<thead><tr>")
    treetable.append("<th>Name</th><th>Type</th><th>View</th>")
    for x in range(len(course_weeks)):
        treetable.append("<th>Week %d</th>" % (x+1))
    treetable.append("<th>Total</th><th>&#37;<a href='#' class='btn btn-mini' data-rel='tooltip' data-original-title='Percentage in relation to total pageviews.'><i class='halflings-icon info-sign'></i></a></th>")
    treetable.append("</tr>")
    treetable.append("</thead>")
    treetable.append("<tbody>\n")

    # Add counts for main access page
    treetable.append("<tr class='stats-row'><td class='stats-title'>Main Course Homepage</td><td class='stats-title'>Course</td><td class='stats-title'></td>")

    #for week in course_weeks:
    if displaytype == "user":
        user_counts_dict = get_userweekcount_for_specificallcoursepage(weeks_str_list, course_id, "course", "view")
        treetable.append(makeweek_rows(course_id,course_weeks,user_counts_dict,total_users,total_pageviews, userpercent=True))
    else:
        total_counts_dict = get_weekcount_for_specificallcoursepage(weeks_str_list, course_id, "course", "view")
        treetable.append(makeweek_rows(course_id,course_weeks,total_counts_dict,total_users,total_pageviews, userpercent=False))

    treetable.append("</tr>\n")
    moodle_treetable(course_id, displaytype, 0)

    treetable.append("<tr class='stats-row'><td class='stats-title'><span>Total</span></td><td class='stats-title'></td><td class='stats-title'></td>")
    for week in week_totals:
        treetable.append('<td>%d</td>' % (week))

    treetable.append('<td>&nbsp;</td><td>&nbsp;</td>')
    treetable.append("</tr>\n")
    treetable.append("</tbody>\n")

    treetable = ''.join(treetable)
    sitetree_json = json.dumps(sitetree)

    if displaytype == 'counts':
        update_coursesummarytable (course_id, 'content_counts_table', treetable.replace("'", "''"))
        update_coursesummarytable (course_id, 'sitetree', sitetree_json.replace("'", "''"))
    else:
        update_coursesummarytable (course_id, 'content_user_table', treetable.replace("'", "''"))

def moodle_treetable(course_id, displaytype, parent_id=0):
    """
        Moodle course structure processing
    """
    global treetable
    global course_weeks
    global content_hidden_list
    global tree_table_totals
    global communication_types
    global assessment_types

    global sitetree

    total_users = get_totalusers(course_id)
    total_pageviews = get_totalpageviews(course_id)

    excluse_contentype_list = communication_types + assessment_types
    excluse_contentype_str =  ','.join("'{0}'".format(x) for x in excluse_contentype_list)

    weeks_str_list = ','.join(map(str, course_weeks))

    res_sql = "SELECT title, content_type, content_id, parent_id, order_no FROM dim_pages WHERE parent_id=%d AND content_type NOT IN (%s) AND course_id=%d order by order_no;" %(parent_id, excluse_contentype_str, course_id)

    result = connection.execute(res_sql);
    rows = result.fetchall()
    for row in rows:
        pageid2 = int(row[2])

        sitetree.append((pageid2,parent_id))
        order_no = row[4]
        content_type = row[1]
        classname = ""
        parentidstring =""

        if course_type=="Moodle":
            pagestoinclude = ['page', 'folder', 'section', 'course_modules'] #"label" 'assign', 'forum', 'quiz', 'resource', 'url', 'book', 'course_module_instance_list', 'user', 'course_resources_list', 'activity_report', 'report', 'recent_activity', 'readtracking', 'event'
        else:
            pagestoinclude = ['course/x-bb-coursetoc','resource/x-bb-folder', 'resource/x-bb-stafffolder', 'resource/x-bb-discussionfolder']

        if row[1] in pagestoinclude:
            classname = "folder"
        else:
            classname = "file"

        if row[3]==0:
            parentidstring = ""
        else:
            parentidstring = "data-tt-parent-id='%s'" % (str(row[3]))
        if (row[1]=='section'):
            treetable.append("<tr data-tt-id='%s' %s class='stats-row'><td class='stats-title'><span class='%s'>%s</span></td><td class='stats-title'>%s</td><td class='stats-title'><a href='/coursepage?course_id=%d&page_id=%s&section_order=%s' class='btn btn-small btn-primary'>View</a></td></td>" % (str(pageid2), parentidstring, classname, row[0], row[1], course_id, str(pageid2), str(row[4])))
        else:
            treetable.append("<tr data-tt-id='%s' %s class='stats-row'><td class='stats-title'><span class='%s'>%s</span></td><td class='stats-title'>%s</td><td class='stats-title'><a href='/coursepage?course_id=%d&page_id=%s' class='btn btn-small btn-primary'>View</a></td>" % (str(pageid2), parentidstring, classname, row[0], row[1], course_id, str(pageid2)))

        if displaytype == "user":
            user_counts_dict = get_userweekcount(weeks_str_list, row[2],course_id, content_type, order_no)
            treetable.append(makeweek_rows(course_id,course_weeks,user_counts_dict,total_users,total_pageviews, userpercent=True))
        else:
            total_counts_dict = get_weekcount(weeks_str_list, row[2],course_id, content_type, order_no)
            treetable.append(makeweek_rows(course_id,course_weeks,total_counts_dict,total_users,total_pageviews, userpercent=False))

        treetable.append("</tr>\n")
        moodle_treetable(course_id, displaytype, parent_id=int(row[2]))

def get_weekcount_for_specificallcoursepage(week, course_id,  module, action):
    global df_treetable
    pageviews = {}

    for wk in course_weeks:
        pageviews[wk] = 0

    filtered_df = df_treetable[(df_treetable.module == module) & (df_treetable.action == module) & (df_treetable.course_id == int(course_id))]

    by_week = filtered_df.groupby(by=['date_week'])['pageview'].sum()
    for ind, dfrow in by_week.iteritems():
          pageviews[ind] = dfrow
    return pageviews

def get_userweekcount_for_specificallcoursepage(week, course_id, module, action):
    global df_treetable
    usercounts = {}

    for wk in course_weeks:
        usercounts[wk] = 0

    filtered_df = df_treetable[(df_treetable.course_id == int(course_id)) & (df_treetable.module == module) & (df_treetable.action == action)]

    unique_by_week = filtered_df.groupby('date_week').user_pk.nunique() #filtered_df.groupby(by=['date_week'])['pageview'].sum()

    total_users = get_totalusers(course_id)
    for ind, dfrow in unique_by_week.iteritems():
        usercounts[ind] = dfrow #percent

    return usercounts

def get_posts(week, pageid, course_id):
    """
    Returns the number of forums posts made in a specified week

    Args:
        week: week no
        pageid: content id of the forum item
        course_id: course id

    Returns:
        posts: forum posts as an integer
    """
    posts = {}
    sql = "SELECT COUNT(F.id) AS pageviews, D.date_week  FROM summary_posts F INNER JOIN Dim_Dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.forum_id=%d AND D.date_week IN (%s) GROUP BY D.date_week ORDER BY D.date_week;" % (course_id, int(pageid), week)
    result = connection.execute(sql);
    for row in result:
        posts[int(row[1])] = int(row[0])
    return posts

def get_weekcount(week,pageid, course_id, content_type, order_no):
    global df_treetable
    pageviews = {}
    pageid2 = int(pageid)

    for wk in course_weeks:
        pageviews[wk] = 0

    if content_type=="section":
        filtered_df = df_treetable[(df_treetable.section_order == int(pageid)) & (df_treetable.course_id == int(course_id))]
    else:
        filtered_df = df_treetable[(df_treetable.page_id == int(pageid)) & (df_treetable.course_id == int(course_id))]

    by_week = filtered_df.groupby(by=['date_week'])['pageview'].sum()
    for ind, dfrow in by_week.iteritems():
          pageviews[ind] = dfrow

    return pageviews

def get_userweekcount(week,pageid, course_id, content_type, order_no):
    global df_treetable
    usercounts = {}
    pageid2 = int(pageid)

    for wk in course_weeks:
        usercounts[wk] = 0

    filtered_df = None

    if content_type=="section":
        filtered_df = df_treetable[(df_treetable.course_id == int(course_id)) & (df_treetable.section_order == order_no)]
    else:
        filtered_df = df_treetable[(df_treetable.course_id == int(course_id)) & (df_treetable.page_id == int(pageid))]

    unique_by_week = filtered_df.groupby('date_week').user_pk.nunique() #filtered_df.groupby(by=['date_week'])['pageview'].sum()
    total_users = get_totalusers(course_id)
    for ind, dfrow in unique_by_week.iteritems():
        usercounts[ind] = dfrow #percent
    return usercounts

def get_totalusers(course_id):
    """
    Returns the number of students in a course
    Todo: extend to take role as parameter

    Args:
        course_id: course id

    Returns:
        usercounts: no of users as an integer
    """
    sql = "SELECT COUNT(U.id) AS usercount FROM Dim_Users U WHERE U.course_id=%d AND U.role='Student';" % (course_id)
    result = connection.execute(sql);
    for row in result:
           if row[0] is not None:
                usercounts = int(row[0])
    return int(usercounts)

def get_totalpageviews(course_id):
    """
    Returns the total number of pageviews for a course

    Args:
        course_id: course id

    Returns:
        counts: no of pagevies as an integer
    """
    sql = "SELECT COUNT(id) AS views FROM fact_coursevisits U WHERE course_id=%d;" % (course_id)
    counts = 0
    result = connection.execute(sql);
    for row in result:
           if row[0] is not None:
                counts = int(row[0])
    return int(counts)

def get_eventpre_postcounts(week, pageid, course_id):
    """
        Calculate pageviews pre and post a day of the week
    """
    pageviews = 0
    pageid2 = int(pageid)
    sql = "SELECT SUM(F.pageview) AS pageviews FROM Fact_Coursevisits F INNER JOIN Dim_Dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.page_id=%d AND D.date_week=%d AND D.date_dayinweek IN (0,1);" % (course_id, int(pageid2), week)
    result = connection.execute(sql);
    for row in result:
           if row[0] is not None:
                pageviews = int(row[0])
    return int(pageviews)

def get_indivuserweekcount(week, userid, course_id):
    """
        Returns pageviews by week for the user
    """
    pageviews = {}
    sql = "SELECT SUM(F.pageview) AS pageviews, D.date_week, F.user_pk FROM Fact_Coursevisits F INNER JOIN Dim_Dates  D ON F.Date_Id = D.Id WHERE F.user_pk='%s' AND F.course_id=%d AND D.Date_week IN (%s) GROUP BY D.date_week ORDER BY D.date_week;" % (userid, course_id, week)
    result = connection.execute(sql);
    for row in result:
        pageviews[row[1]] = int(row[0])
    return pageviews

def get_usereventpre_postcounts(week,userid,course_id):
    """
        Returns pageviews for a user by week pre and post a day of the week
    """
    pageviews = 0
    sql = "SELECT SUM(F.pageview) AS pageviews FROM Fact_Coursevisits F INNER JOIN Dim_Dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d AND F.user_pk='%s' AND D.date_week=%d AND D.date_dayinweek IN (0,1);" % (course_id, str(userid), week)
    result = connection.execute(sql);
    for row in result:
           if row[0] is not None:
                pageviews = int(row[0])
    return int(pageviews)

"""
    Ingest Blackboard Data
"""

def populate_dim_users_table_blackboard(user_membership_resourcefile, course_id):
    """
    Extracts users from the course export files and inserts into the dim_users table
    Todo: Update to include all roles not only students

    Args:
        user_membership_resourcefile: file with the list of users
        course_id: course id

    """
    global coursemember_to_userid_dict
    coursemember_to_userid_dict = {}
    tree = ET.ElementTree(file=user_membership_resourcefile)
    root = tree.getroot()
    username = ""
    firstname = ""
    lastname = ""
    role = ""
    email = ""
    for child_of_root in root:
        userpk = str(child_of_root.attrib["id"])
        user_id = userpk[1:(len(userpk)-2)]
        for child_child_of_root in child_of_root:
            if child_child_of_root.tag == "EMAILADDRESS":
                email = child_child_of_root.attrib["value"]
            if child_child_of_root.tag == "USERNAME":
                username = child_child_of_root.attrib["value"]
            for child_child_child_of_root in child_child_of_root:
                if child_child_child_of_root.tag=="GIVEN":
                    firstname = child_child_child_of_root.attrib["value"]
                    firstname = firstname.replace("'", "''")
                elif child_child_child_of_root.tag=="FAMILY":
                    lastname = child_child_child_of_root.attrib["value"]
                    lastname = lastname.replace("'", "''")
                elif child_child_child_of_root.tag=="ROLEID":
                    role = child_child_child_of_root.attrib["value"]

        user_pk = str(course_id) + "_" + user_id
        if (firstname is None) or (len(firstname) == 0):
            firstname = "blank"
        else:
            firstname = firstname.replace("'", "''")
        if (lastname is None) or (len(lastname) == 0):
            lastname = "blank"
        else:
            lastname = lastname.replace("'", "''")
        if (email is None) or (len(email) == 0):
            email = "blank"
        else:
            email = email.replace("'", "''")
        if role != "STAFF":
            row = {"lms_id": user_id, "firstname": firstname, "lastname": lastname, "username": username, "email": email, "role": role, "user_pk": user_pk, "course_id": course_id}
            save_summaryobject ("dim_users", row)

def process_IMSCP_Manifest_Blackboard(path, course_id):
    """
        Traverses the Blackboard IMS Manifest XML to get the course structure
        Lots of things in the Blackboard XML Manifest don't make sense.
        The format was reverse engineered to get the LOOP trial running
        Todo: Refactor IMSCP Manifest processing code
    """
    manifest_file = os.path.normpath(path + "/imsmanifest.xml")
    tree = ET.ElementTree(file=manifest_file)
    root = tree.getroot()

    user_membership_resourcefile = ""
    usermsgstate_resourcefile = ""
    membership_file = ""

    # Get all resource no and the pk for each.
    # Store a dict to map resource no to pk and a dict of counts of content types
    # Make a dict to match resource no to pk (id)
    resource_pk_dict = {}
    resource_type_lookup_dict = {}
    resource_name_lookup_dict = {}
    resource_id_type_lookup_dict = {}
    internal_handles = []
    toc_dict ={'discussion_board_entry':0,'staff_information':0, 'announcements_entry':0, 'check_grade':0}

    # Make a dict to store counts of content types
    resource_type_dict = {}

    assessment_res_to_id_dict = {}

    for elem in tree.iter(tag='resource'):
        file = elem.attrib["{http://www.blackboard.com/content-packaging/}file"]
        title = elem.attrib["{http://www.blackboard.com/content-packaging/}title"]
        # http://www.peterbe.com/plog/unicode-to-ascii
        title = unicodedata.normalize('NFKD', unicode(title)).encode('ascii','ignore')
        type = elem.attrib["type"]
        if type=="resource/x-bb-document":
            type=get_actual_contenttype_blackboard(os.path.normpath(path + "/" + file))
        resource_no = elem.attrib["{http://www.w3.org/XML/1998/namespace}base"]
        # the type file has invalid xml course/x-bb-trackingevent so ignore
        real_id = "0"
        if type not in ["course/x-bb-trackingevent","assessment/x-bb-qti-attempt", "resource/x-bb-announcement"]:
             cur_id = getID_fromResourceFile(os.path.normpath(path + "/" + file), type)

             resource_pk_dict[resource_no] = cur_id
             resource_type_lookup_dict[resource_no] = type
             resource_name_lookup_dict[resource_no] = title
             if cur_id!='0':
                 resource_id_type_lookup_dict[cur_id[1:len(cur_id)-2]] = type
                 real_id = cur_id[1:-2]
             else:
                 resource_id_type_lookup_dict[cur_id] = type
             if type in resource_type_dict:
                 resource_type_dict[type] = resource_type_dict[type]  + 1
             else:
                 resource_type_dict[type] =1
             if type == "course/x-bb-user":
                 user_membership_resourcefile = file

             if type == "discussionboard/x-bb-usermsgstate":
                 usermsgstate_resourcefile = file

             if type == "resource/x-bb-discussionboard":
                 process_blackboard_forum(os.path.normpath(path + "/" + file), cur_id, title, course_id)
             elif type == "resource/x-bb-conference":
                 process_blackboard_conferences(os.path.normpath(path + "/" + file), cur_id, title, course_id)
             elif type == "course/x-bb-gradebook":
                 gradebook_file = file
             elif type == "assessment/x-bb-qti-test":
                 process_blackboard_test(os.path.normpath(path + "/" + file), cur_id, title, course_id, type)
                 assessment_res_to_id_dict[resource_no] = cur_id[1:-2]
             elif type == 'membership/x-bb-coursemembership':
                membership_file = file

             if type in ['assessment/x-bb-qti-test', 'resource/x-bb-discussionboard']:
                includedrow = {"course_id": course_id, "content_type": type, "content_id": real_id, "title": title.replace("'", "''"), "order_no": 0, "parent_id": 0}
                save_summaryobject ("dim_pages", includedrow)

    parent_map = {c:p for p in tree.iter() for c in p}
    order = 1
    for node in parent_map:
        if ((node.tag=="item") and ('identifierref' in node.attrib)):
            curr_node = node.attrib["identifierref"]
            parent_node = parent_map[node]
            if parent_node.tag=="organization":
                parent_resource = parent_node.attrib["identifier"]
                if parent_resource in resource_pk_dict:
                    parent_resource_no = resource_pk_dict[parent_resource]
                else:
                    parent_resource_no = 0
            elif 'identifierref' in parent_node.attrib:
                parent_resource = parent_node.attrib["identifierref"]
                if parent_resource in resource_pk_dict:
                    parent_resource_no = resource_pk_dict[parent_resource]
                else:
                    parent_resource_no = 0
            else:
                parent_resource_no = 0
            curr_node_name = resource_name_lookup_dict[curr_node] #node.attrib["{http://www.blackboard.com/content-packaging/}title"]
            curr_node_type = resource_type_lookup_dict[curr_node]
            curr_node_id = resource_pk_dict[curr_node]
            curr_node_id = curr_node_id[1:len(curr_node_id)-2]
            if parent_resource_no != 0:
                parent_resource_no = parent_resource_no[1:len(parent_resource_no)-2]

            #get actual content handle
            content_handle = get_contenthandle_blackboard(os.path.normpath(path + "/" + curr_node + ".dat"))

            if content_handle in toc_dict:
                toc_dict[content_handle] = curr_node_id
                if content_handle =="staff_information":
                    curr_node_type = "resource/x-bb-stafffolder"
                elif content_handle == "discussion_board_entry":
                    curr_node_type = "resource/x-bb-discussionfolder"
                elif content_handle == "check_grade":
                    curr_node_type = "course/x-bb-gradebook"

            if curr_node_type!="resource/x-bb-asmt-test-link":
                row = {"course_id": course_id, "content_type": curr_node_type, "content_id": curr_node_id, "title": curr_node_name.replace("'", "''"), "order_no": order, "parent_id": parent_resource_no}
                save_summaryobject ("dim_pages", row)
        else:
            x = 1
        order += 1

    # store single announcements item to match announcements coming from log
    announcements_id = getmax_pageid(course_id)
    row = {"course_id": course_id, "content_type": "resource/x-bb-announcement", "content_id": announcements_id, "title": "Announcements", "order_no": 0, "parent_id": 0}
    save_summaryobject ("dim_pages", row)

    # store single view gradebook to match check_gradebook coming from log
    gradebook_id = getmax_pageid(course_id)
    row = {"course_id": course_id, "content_type": "course/x-bb-gradebook", "content_id": gradebook_id, "title": "View Gradebook", "order_no": 0, "parent_id": 0}
    save_summaryobject ("dim_pages", row)

    # remap /x-bbstaffinfo and discussion boards
    # toc_dict ={'discussion_board_entry':0,'staff_information':0, 'announcements_entry':0, 'check_grade':0}
    if toc_dict['staff_information']!=0:
        update_dimpage(course_id, "resource/x-bb-staffinfo", toc_dict['staff_information'])

    # process memberships
    member_to_user_dict = process_blackboard_memberships(os.path.normpath(path + "/" + membership_file), course_id)

    # process quiz attempts in gradebook file
    content_link_id_to_content_id_dict = process_blackboard_attempt(os.path.normpath(path + "/" + gradebook_file), course_id, assessment_res_to_id_dict, member_to_user_dict, path)

    return user_membership_resourcefile, resource_id_type_lookup_dict, usermsgstate_resourcefile, announcements_id, gradebook_id, content_link_id_to_content_id_dict

def build_starschema_blackboard(course_log_file, course_id, announcements_id, gradebook_id, content_link_id_to_content_id_dict, idtypemap):
    """
        Extract entries from the csv log file from a Blackboard course and inserts each entry as a row in the fact_coursevisits table
    """
    count = 0
    header = None
    trans = connection.begin()
    with open(course_log_file, 'rU') as f:
        reader = csv.reader(f)

        for row in reader:
            count = count + 1

            if (header == None):
                header = row
                continue

            arow = {}
            for header_index in range (0,  len(header)):
                arow[(header[header_index])] = row[header_index]

            # Process row
            cur_visit_date = datetime.datetime.strptime(arow["TIMESTAMP"], "%d-%b-%y %H:%M:%S") #eg 01-SEP-14 09:46:52
            date_id = datetime.datetime.strftime(cur_visit_date, "%d-%b-%y") #time.strftime("%d-%b-%y", cur_visit_date)
            time_id = datetime.datetime.strftime(cur_visit_date, "%H:%M:%S") #time.strftime("%H:%M:%S", cur_visit_date)

            unixtimestamp = time.mktime(cur_visit_date.timetuple())

            user_id = arow["USER_PK1"]
            page_id = arow["CONTENT_PK1"]

            action = arow["EVENT_TYPE"]
            forum_id = arow["FORUM_PK1"]

            if forum_id!="":
                page_id = forum_id

            # defaul for module
            module = "blank"
            if page_id!="":
                if str(page_id) in idtypemap:
                    module = idtypemap[str(page_id)]
                else:
                    if arow["INTERNAL_HANDLE"]=="":
                        module = arow["DATA"]
                        # also add to dim_pages
                        if (not check_ifRecordExists('dim_pages', 'content_id', int(page_id))):
                            title = "blank"
                            if module=="/webapps/blackboard/execute/blti/launchLink":
                                title = "LTI Link"
                            elif module=="/webapps/blackboard/execute/manageCourseItem":
                                title = "Manage Course Item"
                            page_row = {"course_id": course_id, "content_type": module, "content_id": page_id, "title": "blank", "order_no": 0, "parent_id": 0}
                            save_summaryobject("dim_pages", page_row)
                    else:
                        module = "blank"
            elif arow["INTERNAL_HANDLE"]=="my_announcements":
                module = "resource/x-bb-announcement"
                page_id = str(announcements_id)
            elif arow["INTERNAL_HANDLE"]=="check_grade":
                module = "course/x-bb-gradebook"
                page_id = str(gradebook_id)

            user_pk = str(course_id) + "_" + user_id
            page_pk = str(course_id) + "_" + page_id

            pageview = 1

            #map all links in content to assessment to actual assessment id
            if page_id in content_link_id_to_content_id_dict:
                page_id = content_link_id_to_content_id_dict[page_id]
            # Need to exclude forum post creation as this is counted in summary_posts
            # Exclude admin actions such as entering an announcement
            if arow["INTERNAL_HANDLE"] not in ['discussion_board_entry', 'db_thread_list_entry', 'db_collection_entry', 'announcements_entry', 'cp_gradebook_needs_grading']:
                row = {"date_id": date_id, "time_id": time_id, "course_id": course_id, "datetime": unixtimestamp, "user_id": user_id, "module": module, "action": action, "page_id": page_id, "pageview":1, "user_pk": user_pk, "page_pk": page_pk, "unixtimestamp": unixtimestamp}
                connection.execute(return_summaryobjectsql("fact_coursevisits", row))

    trans.commit()
    print "Imported %d facts " % (count)

def update_dimpage(course_id, content_type, parent_id):
    sql = "UPDATE dim_pages SET parent_id=%s WHERE course_id =%d AND content_type='%s';" % (parent_id, course_id, content_type)
    print sql
    try:
        test = connection.execute(sql);
    except: # catch *all* exceptions
        e = sys.exc_info()[0]
        print "<p>Error: %s</p>" % e
        print sys.exc_info()

def process_blackboard_attempt(filepath, course_id, assessment_res_to_id_dict, member_to_user_dict, path):
    tree = ET.ElementTree(file=filepath)
    root = tree.getroot()
    content_id = None #content_id[1:-2]
    content_link_id = None
    title = None
    content_link_id_to_content_id_dict = {}
    for elem in tree.iter(tag='OUTCOMEDEFINITION'):
        for child_of_root in elem:
            # This is the OUTCOMEDEFINITION level
            if child_of_root.tag == "ASIDATAID":
                resid = child_of_root.attrib['value']
                if resid in assessment_res_to_id_dict:
                    content_id = assessment_res_to_id_dict[resid]
            if child_of_root.tag == "CONTENTID":
                linkres = child_of_root.attrib['value']
                if linkres=="":
                    content_link_id = None
                else:
                    linkid = getID_fromResourceFile(os.path.normpath(path + "/" + linkres + ".dat"))
                    content_link_id = linkid[1:-2]
            if child_of_root.tag == "EXTERNALREF":
                externalref = child_of_root.attrib['value']
                if content_id is None and externalref!="":
                    content_id = externalref[1:-2]
            if child_of_root.tag == "TITLE":
                #print child_of_root.attrib['value']
                title = child_of_root.attrib['value']
            for child_child_of_root in child_of_root:
                # This is the outcomes level
                user_id = None
                grade = None
                unixtimestamp = None
                attempt_id = None
                date_str = None
                for child_child_child_of_root in child_child_of_root:
                    if child_child_child_of_root.tag == "COURSEMEMBERSHIPID":
                        member_id = child_child_child_of_root.attrib["value"]
                        member_id = member_id[1:len(member_id)-2]
                        if member_id in member_to_user_dict:
                            user_id = member_to_user_dict[member_id]
                        else:
                            user_id = member_id

                    for child_child_child_child_of_root in child_child_child_of_root:

                            if child_child_child_of_root.tag == "ATTEMPT":
                                attempt_id = child_child_child_of_root.attrib["id"]
                                attempt_id = attempt_id[1:len(attempt_id)-2]
                            for child_child_child_child_child_of_root in child_child_child_child_of_root:

                                if child_child_child_child_child_of_root.tag == "SCORE":
                                    grade = child_child_child_child_child_of_root.attrib['value']
                                if child_child_child_child_child_of_root.tag == "DATEATTEMPTED":
                                    date_str = child_child_child_child_child_of_root.attrib['value']

                if (content_id is not None and grade is not None): # i.e. 0 attempts -
                    date_id = date_str[0:len(date_str)-4]
                    post_date = datetime.datetime.strptime(date_id, "%Y-%m-%d %H:%M:%S") #2014-07-11 16:52:53 EST

                    unixtimestamp = time.mktime(post_date.timetuple())

                    date_id_index = datetime.datetime.strftime(post_date, "%d-%b-%y") #time.strftime("%d-%b-%y", cur_visit_date)
                    time_id = datetime.datetime.strftime(post_date, "%H:%M:%S")
                    attempt_row = {"course_id": course_id,  "content_id": content_id, "grade": grade, "user_id":  user_id, "unixtimestamp": unixtimestamp}

                    save_summaryobject ("dim_submissionattempts", attempt_row)
                    if content_link_id is not None:
                        content_link_id_to_content_id_dict[content_link_id] = content_id
                    #save all attempts as pageviews in fact_coursevisits
                    user_pk = str(course_id) + "_" + user_id
                    page_pk = str(course_id) + "_" + content_id

                    fact_row = {"date_id": date_id_index, "time_id": time_id, "course_id": course_id, "datetime": unixtimestamp, "user_id": user_id, "module": 'assessment/x-bb-qti-test', "action": 'COURSE_ACCESS', "page_id": content_id, "pageview":1, "user_pk": user_pk, "page_pk": page_pk, "unixtimestamp": unixtimestamp}
                    save_summaryobject ("fact_coursevisits", fact_row)

    return content_link_id_to_content_id_dict

def get_contenthandle_blackboard(filepath):
    tree = ET.ElementTree(file=filepath)
    root = tree.getroot()
    content_handle = ""
    for elem in tree.iter(tag='INTERNALHANDLE'):
        content_handle =  elem.attrib["value"]
    return content_handle

def get_actual_contenttype_blackboard(filepath):
    tree = ET.ElementTree(file=filepath)
    root = tree.getroot()
    content_type = ""
    for elem in tree.iter(tag='CONTENTHANDLER'):
        content_type =  elem.attrib["value"]
    return content_type

def process_blackboard_memberships(filepath, course_id):
    tree = ET.ElementTree(file=filepath)
    root = tree.getroot()
    member_to_user_dict = {}
    for elem in tree.iter(tag='COURSEMEMBERSHIP'):
        member_id = elem.attrib["id"]
        member_id = member_id[1:-2]
        user_id = 0
        for usr in elem:
            if usr.tag == "USERID":
                user_id = usr.attrib["value"]
                user_id = user_id[1:-2]
        member_to_user_dict[member_id] = user_id
    return member_to_user_dict

def process_blackboard_test(filepath, content_id, title, course_id, content_type):
    timeopen = 0
    timeclose = 0
    grade = 0.0
    content_id = content_id[1:-2]
    tree = ET.ElementTree(file=filepath)
    root = tree.getroot()
    for elem in tree.iter(tag='qmd_absolutescore_max'):
        grade =  elem.text
    submissiontype_row = {"course_id": course_id,  "content_id": content_id, "content_type": content_type, "timeopen": timeopen, "timeclose": timeclose, "grade": grade}
    save_summaryobject ("dim_submissiontypes", submissiontype_row)

def process_blackboard_conferences(filepath, content_id, title, course_id):
    # Save Forum = Conferences entry
    forum_row = {"course_id": course_id,  "forum_id": content_id, "title": "Conferences", "no_discussions": 0}
    save_summaryobject ("summary_forum", forum_row)

    tree = ET.ElementTree(file=filepath)
    root = tree.getroot()

    conf_id = ""
    title = ""
    for elem in tree.iter(tag='CONFERENCE'):
        conf_id = elem.attrib["id"]
        conf_id = conf_id[1:len(conf_id)-2]
        for child_of_root in elem:
            if child_of_root.tag == "TITLE":
                title = child_of_root.attrib["value"]
        # Save Forum - Discussion Board
        discussion_row = {"course_id": course_id,  "forum_id": conf_id, "discussion_id": content_id, "title": title.replace("'", "''"), "no_posts": 0}
        save_summaryobject ("summary_discussion", discussion_row)

def process_blackboard_forum(filepath, content_id, title, course_id):
    global msg_to_forum_dict
    tree = ET.ElementTree(file=filepath)
    root = tree.getroot()
    forum_id = root.attrib['id']
    forum_id = forum_id[1:len(forum_id)-2]
    conf_id = ""
    title = ""
    for elem in root:
        if elem.tag == "CONFERENCEID":
            conf_id = elem.attrib["value"]
            conf_id = conf_id[1:len(conf_id)-2]
        if elem.tag == "TITLE":
            title = elem.attrib["value"]
    # Get all posts
    for msg in tree.iter(tag='MSG'):
        post_id = msg.attrib['id'][1:-2]
        post_title = ""
        date_id = ""
        user_id = ""
        for elem in msg:
            if elem.tag == "TITLE":
                post_title = elem.attrib["value"]
            if elem.tag == "USERID":
                user_id = elem.attrib["value"]
            for subelem in elem:
                if subelem.tag == "CREATED":
                    date_id = subelem.attrib["value"]
        if (date_id is not None) and (len(date_id)!=0) and (date_id!=" ") and (date_id!=""):
            date_id = date_id[0:len(date_id)-4]
            post_date = datetime.datetime.strptime(date_id, "%Y-%m-%d %H:%M:%S") #2014-07-11 16:52:53 EST
            date_id = datetime.datetime.strftime(post_date, "%d-%b-%y")
            user_id = user_id[1:len(user_id)-2]
            post_row = {"date_id": date_id, "user_id": user_id, "course_id": course_id,  "forum_id": forum_id, "discussion_id": conf_id}
            save_summaryobject ("summary_posts", post_row)

def populate_summary_contentaggregatesbycourse_table(resource_type_dict, course_id):
    for key in resource_type_dict:
        row = {"contenttype": key, "count": resource_type_dict[key], "course_id": course_id}
        save_summaryobject ("summary_contentaggregatesbycourse", row)

def getID_fromResourceFile(resource_file, type=None):
    id = '0'
    tree = ET.ElementTree(file=resource_file)
    root = tree.getroot()
    if type == "assessment/x-bb-qti-test":
        for elem in tree.iter(tag='assessmentmetadata'):
            for elem_elem in elem:
                if elem_elem.tag == "bbmd_asi_object_id":
                    id =  elem_elem.text
    else:
        if "id" in root.attrib:
            id = root.attrib["id"]
        elif root.tag == "questestinterop":
            for elem in tree.iter(tag='assessmentmetadata'):
                for elem_elem in elem:
                    if elem_elem.tag == "bbmd_asi_object_id":
                        id =  elem_elem.text
        else:
            id = '0'
    return id

"""
    Ingest Moodle Data
"""

def populate_dim_users_table_moodle(user_membership_resourcefile, course_id, course_type):
    """
    Extracts users from the course export files and inserts into the dim_users table
    Todo: Update to include all roles not only students

    Args:
        user_membership_resourcefile: file with the list of users
        course_id: course id
        course_type: Moodle or MoodleMacquarie

    """
    print "populate_dim_users_table_moodle" , course_type
    role_skip_list = []
    if course_type=="Moodle":
        role_skip_list = ["Staff","None", None]

    tree = ET.ElementTree(file=user_membership_resourcefile)
    root = tree.getroot()
    firstname = ""
    lastname = ""
    role = ""
    username = ""
    email = ""
    lms_id = ""

    global staff_list

    trans = connection.begin()

    for child_of_root in root:
        lms_id = child_of_root.attrib["id"]
        for child_child_of_root in child_of_root:
            if (child_child_of_root.tag== "firstname"): firstname = child_child_of_root.text
            if (child_child_of_root.tag== "lastname"): lastname = child_child_of_root.text
            if (child_child_of_root.tag== "department"): role = child_child_of_root.text
            if (child_child_of_root.tag== "username"): username = child_child_of_root.text
            if (child_child_of_root.tag== "email"): email = child_child_of_root.text
        user_pk = str(course_id) + "_" + lms_id

        if (firstname is None) or (len(firstname) == 0):
            firstname = "blank"
        else:
            firstname = firstname.replace("'", "''")
        if (lastname is None) or (len(lastname) == 0):
            lastname = "blank"
        else:
            lastname = lastname.replace("'", "''")
        if (email is None) or (len(email) == 0):
            email = "blank"

        if course_type=="Moodle":
            if (role not in ["Staff","None", None]):
                staff_list.append(int(lms_id))
                row = {"lms_id": lms_id, "firstname": firstname, "lastname": lastname, "username": username, "email": email, "role": role, "user_pk": user_pk, "course_id": course_id}
                save_summaryobject ("dim_users", row)
        elif course_type=="MoodleMacquarie":
            if "students" in email:
                staff_list.append(int(lms_id))
                role = "Student"
                row = {"lms_id": lms_id, "firstname": firstname, "lastname": lastname, "username": username, "email": email, "role": role, "user_pk": user_pk, "course_id": course_id}
                save_summaryobject ("dim_users", row)
    trans.commit()

def build_starschema_moodle(course_log_file, course_id, content_type, forum_id):
    """
        Extracts logs from the Moodle export format and inserts as a row in the fact_coursevisits table.
    """
    global staff_list
    tree = ET.ElementTree(file=course_log_file)
    root = tree.getroot()
    datetime_str = ""
    lms_user_id = ""
    module = ""
    action = ""
    url = ""
    info = ""
    section_order = 0
    count = 1
    access_sql = "BEGIN TRANSACTION;"
    trans = connection.begin()
    for child_of_root in root:
        count +=1
        for child_child_of_root in child_of_root:
            if (child_child_of_root.tag== "time"): datetime_str = child_child_of_root.text
            if (child_child_of_root.tag== "userid"): lms_user_id = child_child_of_root.text
            if (child_child_of_root.tag== "module"): module = child_child_of_root.text
            if (child_child_of_root.tag== "action"): action = child_child_of_root.text
            if (child_child_of_root.tag== "url"): url = child_child_of_root.text
            if (child_child_of_root.tag== "info"): info = child_child_of_root.text
        date_id = time.strftime("%d-%b-%y", time.gmtime(int(datetime_str))) #datetime.datetime.strptime(arow["TIMESTAMP"], "%d-%b-%y")
        time_id = time.strftime("%H:%M:%S", time.gmtime(int(datetime_str)))
        session_id = 0
        page_id = 0
        section_id = 0
        if (not((url is None) or (len(url) == 0))):
            parsed_url = urlparse.urlparse(url)
            query_as_dict = urlparse.parse_qs(parsed_url.query)
            if "id" in query_as_dict: page_id = query_as_dict["id"][0]
            if "sectionid" in query_as_dict: section_id = query_as_dict["sectionid"][0]
            if "section" in query_as_dict: section_order = int(query_as_dict["section"][0])-1 # I think the numbers are added by 1 here
        if (module in ["forum", "wiki"]):
            page_id = forum_id #info

        user_pk = str(course_id) + "_" + lms_user_id
        page_pk = str(course_id) + "_" + str(page_id)
        section_pk = str(course_id) + "_" + str(section_id)
        fmt = '%d-%b-%y %H:%M:%S'
        dt = datetime.datetime.strptime(date_id + " " + time_id, fmt)
        unixtimestamp = time.mktime(dt.timetuple())

        if (info is None) or (len(info) == 0):
            info = "-"
        else:
            info = info.replace("'", "''")
            info = info.replace("%", "\%") #escape % sign

        if (url is None) or (len(url) == 0):
            url = ""
        else:
            url = info.replace("%", "\%") #escape % sign

        row = {}
        if (lms_user_id not in staff_list):
            if (module not in ['label', 'role', 'unisa_module', 'wizard']):
                if ((action not in ['add mod', 'update mod', 'editsection', 'enrol', 'unenrol', 'report log', 'loginas'])):
                    if section_order > 0:
                        row = {"date_id": date_id, "time_id": time_id, "course_id": course_id, "datetime": datetime_str, "user_id": lms_user_id, "module": module, "action": action, "url": url, "page_id": page_id, "section_id": section_id,  "section_order":  section_order , "pageview": 1, "user_pk": user_pk, "page_pk": page_pk, "section_pk": section_pk, "unixtimestamp": unixtimestamp, "info": info}
                    else:
                        row = {"date_id": date_id, "time_id": time_id, "course_id": course_id, "datetime": datetime_str, "user_id": lms_user_id, "module": module, "action": action, "url": url, "page_id": page_id, "section_id": section_id, "pageview": 1, "user_pk": user_pk, "page_pk": page_pk, "section_pk": section_pk, "unixtimestamp": unixtimestamp, "info": info}

                    connection.execute(return_summaryobjectsql("fact_coursevisits", row))
    trans.commit()

def build_starschema_moodlemacquarie(filepath, course_id):
    """
        Extracts csv log from the Moodle Macquarie and inserts as a row in the fact_coursevisits table.
    """
    print "build_starschema_moodlemacquarie"
    global staff_list
    action_skip_list = ['updated', 'unassigned', 'assigned', 'deleted', 'updated', 'loggedinas']
    print action_skip_list
    trans = connection.begin()
    count = 0
    header = None
    with open(filepath, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:

            count = count + 1

            if (header == None):
                header = row
                continue

            arow = {}
            for header_index in range (0,  len(header)):
                arow[(header[header_index])] = row[header_index]

            # Process row

            # Headings id	eventname	component	action	target	objecttable	objectid	crud	edulevel	contextid
            # contextlevel	contextinstanceid	userid	courseid	relateduserid	anonymous	other
            # timecreated	origin	ip	realuserid
            log_id = arow["id"]
            eventname = arow["eventname"]
            component = arow["component"]
            action = arow["action"]
            target = arow["target"]
            objecttable = arow["objecttable"]
            #page_id = arow["objectid"]
            crud = arow["crud"]
            edulevel = arow["edulevel"]
            contextid = arow["contextid"]
            contextlevel = arow["contextlevel"]
            page_id = arow["contextinstanceid"]
            lms_user_id = arow["userid"]
            relateduserid  = arow["relateduserid"]
            anonymous = arow["anonymous"]
            other = arow["other"] # old info column - has section no a:1:{s:10:"sectionnum";i:17;}
            timecreated = arow["timecreated"]
            origin = arow["origin"]
            ip = arow["ip"]
            realuserid = arow["realuserid"]

            module = None
            if objecttable=='\N':
                module = target
            else:
                module = objecttable

            date_id = time.strftime("%d-%b-%y", time.gmtime(int(timecreated)))
            time_id = time.strftime("%H:%M:%S", time.gmtime(int(timecreated)))
            fmt = '%d-%b-%y %H:%M:%S'
            dt = datetime.datetime.strptime(date_id + " " + time_id, fmt)
            unixtimestamp = time.mktime(dt.timetuple())

            user_pk = str(course_id) + "_" + lms_user_id
            page_pk = str(course_id) + "_" + str(page_id)

            row = {}
            if (lms_user_id not in staff_list):
                if (action not in action_skip_list):
                    if (module not in ["user_enrolments","role"]):
                        row = {"date_id": date_id, "time_id": time_id, "course_id": course_id, "datetime": timecreated, "user_id": lms_user_id, "module": module, "action": action, "page_id": page_id, "pageview":1, "user_pk": user_pk, "page_pk": page_pk, "unixtimestamp": unixtimestamp}
                        connection.execute(return_summaryobjectsql("fact_coursevisits", row))
            else:
                print lms_user_id, action, page_id
                print "staff_list", staff_list
    trans.commit()
    print "imported rows:", count

def process_courseresources(course_resourcelog_folder, course_id, course_type):
    resource_count_dict = {}

    trans = connection.begin()

    for subdir, dirs, files in os.walk(course_resourcelog_folder):
        folder_name = os.path.basename(subdir)
        if ((folder_name!="activities") and (folder_name!="blocks") and (not "blocks" in subdir)):
            moodle_res_list = folder_name.split("_")
            content_type = moodle_res_list[0]
            content_id = moodle_res_list[1]
            content_type_file = subdir + "/" + content_type + ".xml"
            tree = ET.ElementTree(file=content_type_file)
            root = tree.getroot()
            title = "blank"
            timeopen = ""
            timeclose = ""
            grade = ""
            for child_of_root in root:
                for child_child_of_root in child_of_root:
                    if (child_child_of_root.tag== "name"): title = removeNonAscii(child_child_of_root.text)
                    if (child_child_of_root.tag== "timeopen"): timeopen = removeNonAscii(child_child_of_root.text)
                    if (child_child_of_root.tag== "timeclose"): timeclose = removeNonAscii(child_child_of_root.text)
                    if (child_child_of_root.tag== "grade"): grade = removeNonAscii(child_child_of_root.text)
                    if (child_child_of_root.tag== "allowsubmissionsfromdate"): timeopen = removeNonAscii(child_child_of_root.text) # for assignment
                    if (child_child_of_root.tag== "duedate"): timeclose = removeNonAscii(child_child_of_root.text)	# for assignment
            #get parent and get order
            parent_type_file = subdir + "/module.xml"
            parent_id, order_no = get_resource_parentandorder(parent_type_file)

            title = title.replace("%", " percent")

            if (content_type!='label'):
                row = {"course_id": course_id, "content_type": content_type, "content_id": content_id, "title": title.replace("'", "''"), "order_no": order_no, "parent_id": parent_id}
                save_summaryobject ("dim_pages", row)

                if content_type=="forum":
                    process_forum(content_type_file, content_id, title, course_id)
                elif content_type in ["quiz","assign"]:
                    process_submissiontype(content_type,subdir, content_id, title, course_id, timeopen, timeclose, grade)

                #Add to resource_count_dict
                if content_type in resource_count_dict:
                    resource_count_dict[content_type] = resource_count_dict[content_type]  + 1
                else:
                    resource_count_dict[content_type] =1
                #save log entries from log file in resource folder
                if course_type=="Moodle":
                    logfilename = os.path.normpath(subdir + "/" + "logs.xml")
                    #print logfilename
                    build_starschema_moodle(logfilename, course_id, content_type, content_id)
    trans.commit()

def process_submissiontype(content_type,content_type_file, content_id, title, course_id, timeopen, timeclose, grade):
    submissiontype_row = {"course_id": course_id,  "content_id": content_id, "content_type": content_type, "timeopen": timeopen, "timeclose": timeclose, "grade": grade}
    save_summaryobject ("dim_submissiontypes", submissiontype_row)

    content_type_file = content_type_file + "/grades.xml"
    tree = ET.ElementTree(file=content_type_file)

    for elem in tree.iter(tag='grade_grade'):
        user_id = 0
        grade = ""
        unixtimestamp = 0
        for child_of_root in elem:
            if child_of_root.tag == "userid": user_id =  child_of_root.text
            if child_of_root.tag == "rawgrademax": grade =  child_of_root.text
            if child_of_root.tag == "timecreated": unixtimestamp =  child_of_root.text
        attempt_row = {"course_id": course_id,  "content_id": content_id, "grade": grade, "user_id":  user_id, "unixtimestamp": unixtimestamp}
        save_summaryobject ("dim_submissionattempts", attempt_row)

def process_forum(content_type_file, content_id, title, course_id):
    discussion_count = 0
    tree = ET.ElementTree(file=content_type_file)
    for elem in tree.iter(tag='discussion'):
        discussion_id = 0
        discussion_name = ""
        discussion_user_id = 0
        post_count = 0
        if elem.tag == "discussion":
            discussion_id = elem.attrib["id"]
            discussion_count += 1
        for child_of_root in elem:
            if child_of_root.tag == "name": discussion_name =  child_of_root.text
            if child_of_root.tag == "userid": discussion_user_id =  child_of_root.text
            if child_of_root.tag == "posts":
                for child_child_of_root in child_of_root:
                    post_count += 1
                    post_user_id = ""
                    datetime_str = ""
                    for child_child_child_of_root in child_child_of_root:
                        if child_child_child_of_root.tag == "userid":
                            post_user_id =  child_child_child_of_root.text
                        if child_child_child_of_root.tag == "created":
                            datetime_str =  child_child_child_of_root.text
                    date_id = time.strftime("%d-%b-%y", time.gmtime(int(datetime_str)))
                    post_row = {"date_id": date_id, "user_id": post_user_id, "course_id": course_id,  "forum_id": content_id, "discussion_id": discussion_id}

                    save_summaryobject ("summary_posts", post_row)

        discussion_row = {"course_id": course_id,  "forum_id": content_id, "discussion_id": discussion_id, "title": discussion_name.replace("'", "''"), "no_posts": post_count}
        save_summaryobject ("summary_discussion", discussion_row)
    forum_row = {"course_id": course_id,  "forum_id": content_id, "title": title.replace("'", "''"), "no_discussions": discussion_count}
    save_summaryobject ("summary_forum", forum_row)

def process_moodle_sections(course_sections_folder, course_id, course_type):
    for subdir, dirs, files in os.walk(course_sections_folder):
        folder_name = os.path.basename(subdir)
        if (folder_name!="sections"):
            moodle_res_list = folder_name.split("_")
            content_type = moodle_res_list[0]
            content_id = moodle_res_list[1]
            section_file = subdir + "/section.xml"
            title, order_no = get_section_info(section_file)
            if title is None:
                title = "blank"
            row = {"course_id": course_id, "content_type": content_type, "content_id": content_id, "title": removeNonAscii(title.replace("'", "''")), "order_no": order_no, "parent_id": 0}
            save_summaryobject ("dim_pages", row)
            update_moodle_sectionid(int(order_no), int(content_id), course_id)

def update_moodle_sectionid(section_no, content_id, course_id):
    sql = "UPDATE fact_coursevisits SET page_id=%d WHERE course_id=%d AND section_order=%d;" % (content_id, course_id, section_no)
    try:
        test = connection.execute(sql);
    except: # catch *all* exceptions
        e = sys.exc_info()[0]
        print "<p>Error: %s</p>" % e
        print sys.exc_info()

def get_resource_parentandorder(modulefile):
    tree = ET.ElementTree(file=modulefile)
    root = tree .getroot()
    parent_id = ""
    order_no = ""
    for child_of_root in root:
        if (child_of_root.tag== "sectionid"): parent_id = child_of_root.text
        if (child_of_root.tag== "sectionnumber"): order_no = child_of_root.text
    return parent_id, order_no

def get_section_info(modulefile):
    tree = ET.ElementTree(file=modulefile)
    root = tree .getroot()
    title = ""
    order_no = ""
    for child_of_root in root:
        if (child_of_root.tag== "name"): title = child_of_root.text
        if (child_of_root.tag== "number"): order_no = child_of_root.text
    return title, order_no

"""
    Process Users and Access Log for Moodle and Blackboard
"""

def process_accesslog(course_log_file, course_type, course_id, announcements_id=None, gradebook_id=None, content_link_id_to_content_id_dict=None, idtypemap=None):
    if course_type == "Moodle":
        build_starschema_moodle(course_log_file + "/course/logs.xml", course_id, "", 0)
    elif course_type == "MoodleMacquarie":
        build_starschema_moodlemacquarie(course_log_file + "/log.csv", course_id)
    else:
        build_starschema_blackboard(course_log_file, course_id, announcements_id, gradebook_id, content_link_id_to_content_id_dict, idtypemap)

def populate_dim_users_table(user_membership_resourcefile, course_type, course_id):
    if course_type in ["Moodle", "MoodleMacquarie"]:
        populate_dim_users_table_moodle(os.path.normpath(user_membership_resourcefile + "/users.xml"), course_id, course_type)
    else:
        populate_dim_users_table_blackboard(user_membership_resourcefile, course_id)

"""
    Date Helper functions
"""

def get_weeks(start_date_str, end_date_str):
    start_week = get_week(start_date_str)
    end_week = get_week(end_date_str)
    weeks = range(start_week, end_week)
    return weeks

def get_week(date_str):
    date_obj = datetime.datetime.strptime(date_str, "%d/%b/%y") #eg #'01/SEP/14' and '30/SEP/14'
    week = date_obj.isocalendar()[1]
    return week

"""
    Database Helper functions
"""

def save_object (table, row):

    if (table in cache):
        if (row["id"] in cache[table]):
            return row["id"]
    else:
        cache[table] = {}

    keys = row.keys();
    sql = "INSERT INTO " + table + " ("
    sql = sql + ", ".join(keys)
    sql = sql + ") VALUES ("
    sql = sql + ", ".join([ ("'" + str(row[key]) + "'") for key in keys])
    sql = sql + ")"

    connection.execute(sql);
    cache[table][row["id"]] = row

    return row["id"]

def save_summaryobject (table, row):

    keys = row.keys();
    sql = "INSERT INTO " + table + " ("
    sql = sql + ", ".join(keys)
    sql = sql + ") VALUES ("
    sql = sql + ", ".join([ ("'" + str(row[key]) + "'") for key in keys])
    sql = sql + ")"
    id = connection.execute(sql);

    return id


def return_summaryobjectsql (table, row):

    keys = row.keys();
    sql = "INSERT INTO " + table + " ("
    sql = sql + ", ".join(keys)
    sql = sql + ") VALUES ("
    sql = sql + ", ".join([ ("'" + str(row[key]) + "'") for key in keys])
    sql = sql + ");"

    return sql

def update_coursesummarytable(course_id, colname, colvalue):
    sql = "UPDATE summary_courses SET %s='%s' WHERE course_id =%d;" % (colname, colvalue, course_id)
    try:
        test = connection.execute(sql);
    except: # catch *all* exceptions
        e = sys.exc_info()[0]
        print "<p>Error: %s</p>" % e
        print sys.exc_info()

def update_coursesummarytable_blob(course_id, colname, colvalue):
    sql = "UPDATE summary_courses SET %s=%%s WHERE course_id =%d;" % (colname, course_id)
    try:
        test = connection.execute(sql,colvalue);
    except: # catch *all* exceptions
        e = sys.exc_info()[0]
        print "<p>Error: %s</p>" % e
        print sys.exc_info()

def check_ifRecordExists(table, pk_name, pk_value):
    exists = False
    sql = "SELECT count(%s) as cnts FROM %s WHERE %s=%d" % (pk_name, table, pk_name,  pk_value)
    result = connection.execute(sql);
    one_row = result.fetchone()
    if int(one_row[0])>0:
        exists = True
    return exists

def scale_chart(total_counts):
    width = 75
    height = 75
    if total_counts > 75:
        width = 75
        height = 75
    elif total_counts >= 26 and total_counts <= 74 :
        width = 50
        height = 50
    elif total_counts <= 25:
        width = 25
        height = 25
    return width, height

def sanitize (value):
    if (value == ""):
        value = "(BLANK)"
    elif (value == None):
        value = "(NULL)"
    else:
        value = re.sub('[^\w]', "-", value.strip())
    return value

def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

def insert_coursevisit(fact):
    return save_object ("fact_coursevisits", fact)

def insert_course (id, name):
    row = {
           "id": id,
           "name": name
    }
    return save_object ("dim_courses", row)

def insert_user (id, name, type):
    row = {
           "id": id,
           "name": name,
           "type": type
    }
    return save_object ("dim_users", row)

def insert_session (id):
    row = {
           "id": id
    }
    return save_object ("dim_sessions", row)

def insert_page (id, event_type, content_type, page_url):
    row = {
           "id": id,
           "event_type": event_type,
           "content_type": content_type,
           "page_url": page_url,
    }
    return save_object ("dim_pages", row)

def getmax_pageid(course_id):
    res_sql = "SELECT max(content_id) FROM dim_pages WHERE course_id=%d;" % (course_id)
    result = connection.execute(res_sql);
    rows = result.fetchall()
    max_val = rows[0][0]
    if (max_val is None):
        return 1
    else:
        return int(max_val) + 1

if __name__ == "__main__":
    main()
