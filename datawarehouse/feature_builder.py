"""
    Loop Feature Matrix Builder

    Experimental code to:
    - extract data from the Datawarehouse database
    - create a pandas dataframe to extract student features (access by week, attempts, no forum posts)
    - load features in a numpy matrix
    - apply scikit-learn machine learning algorithms
"""

from sklearn import preprocessing
from mpl_toolkits.mplot3d import Axes3D
from sklearn.cluster import AffinityPropagation
from itertools import cycle
from sklearn import decomposition
import numpy as np
from scipy.spatial import ConvexHull
import cPickle as pickle
from sklearn.cluster import KMeans

import sys
from sqlalchemy.engine import create_engine
import datetime
import time
import csv
import random
import os.path
import urlparse
import unicodedata
import pandas as pd
import json

cache = {}
connection = None

def main():
    global engine
    global connection
    global course_id
    global course_weeks
    global communication_types
    global assessment_types

    # load config.json
    course_config = None
    with open('config.json') as data_file:
        course_config = json.load(data_file)

    engine = create_engine('mysql://root:root@localhost/cloop_olap?unix_socket=/Applications/MAMP/tmp/mysql/mysql.sock')

    connection = engine.connect()

    print str(datetime.datetime.now())

    for config in course_config:
        course_id = config['course_id']
        course_type = config['course_type']
        week_startdate = config['start_date']
        week_enddate = config['end_date']
        course_weeks = get_weeks(week_startdate, week_enddate)

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

        feature_builder(course_id)

    print str(datetime.datetime.now())

def feature_builder(course_id):
    """
    Creates a student by feature matrix for applying machine learning algorithms.

    Args:
        course_id: course id

    """
    weeks_str_list = ','.join(map(str, course_weeks))
    weekly_features = ["Content Views", "Assessment Views", "Communication Views", "Forum Posts", "Sessions", "Session Time"]

    # build student id list
    student_id_list = []
    student_details_dict = {}
    student_sql = "SELECT DISTINCT U.lms_id, U.firstname, U.lastname, U.email FROM Dim_Users U WHERE U.course_id=%d AND U.role='Student';" % (course_id)
    result = connection.execute(student_sql);
    students = result.fetchall()

    for student in students:
        student_id_list.append(int(student[0]))
        student_details_dict[int(student[0])] = {'firstname': student[1], 'lastname': student[2], 'email': student[3]}

    # get all quizzes
    quiz_id_list = []
    quiz_sql = "SELECT DISTINCT S.content_id  FROM dim_submissiontypes S WHERE S.course_id=%d;" % (course_id)
    result = connection.execute(quiz_sql);
    quizzes = result.fetchall()
    for quiz in quizzes:
        quiz_id_list.append(int(quiz[0]))

    no_students = get_totalusers(course_id)
    no_features = (len(weekly_features) * len(course_weeks)) + (len(quiz_id_list) * 2)
    print no_students, no_features

    feature_matrix = numpy.zeros(shape=(no_students,no_features))

    weekly_feature_labels = []
    # build feature labels
    for feature in weekly_features:
        for i, week in enumerate(course_weeks):
            weekly_feature_labels.append(feature + ": Week " + str(i+1))
    #add quiz labels
    for q in quiz_id_list:
        weekly_feature_labels.append("Quiz: " + str(q))
    for q in quiz_id_list:
        weekly_feature_labels.append("Quiz Attempts: " + str(q))

    # populate the dataframe for fast querying

    useraccesssql = "SELECT F.pageview, D.date_week, F.module, F.action, F.page_id, F.section_order, F.course_id, F.user_id FROM Fact_Coursevisits F INNER JOIN Dim_Dates  D ON F.Date_Id = D.Id WHERE F.course_id=%d AND D.Date_week IN (%s);" % (course_id, weeks_str_list)
    newconnection = engine.raw_connection()
    df = pd.read_sql(useraccesssql, newconnection)

    userforumsql = "SELECT D.date_week, S.course_id, S.user_id FROM summary_posts S INNER JOIN Dim_Dates  D ON S.Date_Id = D.Id WHERE S.course_id=%d AND D.Date_week IN (%s);" % (course_id, weeks_str_list)
    forum_df = pd.read_sql(userforumsql, newconnection)

    sessionsql = "SELECT S.date_week, S.course_id, S.user_id, S.session_id, S.pageviews, S.session_length_in_mins FROM dim_session S WHERE S.course_id=%d AND S.Date_week IN (%s);" % (course_id, weeks_str_list)
    session_df = pd.read_sql(sessionsql, newconnection)

    quizsql = "SELECT S.user_id, S.content_id, S.grade FROM dim_submissionattempts S WHERE S.course_id=%d;" % (course_id)
    quiz_df = pd.read_sql(quizsql, newconnection)

    #populate features matrix for each student
    for id, student_id in enumerate(student_id_list):
        for fid, feature in enumerate(weekly_features):

            if feature in ["Content Views", "Assessment Views", "Communication Views"]:

                feature_index_offset = (fid) * len(course_weeks)
                filtered_df = None

                if feature == "Content Views":
                    excluse_contentype_list = communication_types + assessment_types
                    #print "excluse_contentype_list", excluse_contentype_list
                    filtered_df = df[(df.user_id == student_id) & (~df["module"].isin(excluse_contentype_list))]
                    #print filtered_df.head()
                elif feature == "Communication Views":
                    filtered_df = df[(df.user_id == student_id) & (df["module"].isin(communication_types))]
                elif feature == "Assessment Views":
                    filtered_df = df[(df.user_id == student_id) & (df["module"].isin(assessment_types))]

                by_week = filtered_df.groupby(by=['date_week'])['pageview'].sum()

                for ind, dfrow in by_week.iteritems():
                    feature_matrix[id,course_weeks.index(ind)+feature_index_offset] = dfrow

            elif feature in ["Forum Posts"]:
                feature_index_offset = (fid) * len(course_weeks)
                filtered_df = None
                filtered_df = forum_df[(forum_df.user_id == student_id)]
                by_week = filtered_df.groupby(by=['date_week'])['user_id'].count()
                for ind, dfrow in by_week.iteritems():
                    feature_matrix[id,course_weeks.index(ind)+feature_index_offset] = dfrow
            elif feature in ["Sessions"]:
                feature_index_offset = (fid) * len(course_weeks)
                filtered_df = None
                filtered_df = session_df[(session_df.user_id == student_id)]
                by_week = filtered_df.groupby(by=['date_week'])['user_id'].count()
                for ind, dfrow in by_week.iteritems():
                    feature_matrix[id,course_weeks.index(ind)+feature_index_offset] = dfrow
            elif feature in ["Session Time"]:
                feature_index_offset = (fid) * len(course_weeks)
                filtered_df = None
                filtered_df = session_df[(session_df.user_id == student_id)]
                by_week = filtered_df.groupby(by=['date_week'])['session_length_in_mins'].mean()
                for ind, dfrow in by_week.iteritems():
                    feature_matrix[id,course_weeks.index(ind)+feature_index_offset] = dfrow
            elif feature in ["Avg Pageviews in Sessions"]:
                feature_index_offset = (fid) * len(course_weeks)
                filtered_df = None
                filtered_df = session_df[(session_df.user_id == student_id)]
                by_week = filtered_df.groupby(by=['date_week'])['pageviews'].mean()
                for ind, dfrow in by_week.iteritems():
                    feature_matrix[id,course_weeks.index(ind)+feature_index_offset] = dfrow
            elif feature in ["Avg Pageviews in Sessions"]:
                feature_index_offset = (fid) * len(course_weeks)
                filtered_df = None
                filtered_df = session_df[(session_df.user_id == student_id)]
                by_week = filtered_df.groupby(by=['date_week'])['pageviews'].mean()
                for ind, dfrow in by_week.iteritems():
                    feature_matrix[id,course_weeks.index(ind)+feature_index_offset] = dfrow
        feature_index_offset = len(weekly_features) * len(course_weeks)
        filtered_df = None
        filtered_df = quiz_df[(quiz_df.user_id == student_id)]
        by_quiz = filtered_df.groupby(by=['content_id'])['grade'].max()
        for ind, dfrow in by_quiz.iteritems():
            feature_matrix[id,quiz_id_list.index(ind)+feature_index_offset] = dfrow
        feature_index_offset = (len(weekly_features) * len(course_weeks)) + len(quiz_id_list)
        by_quiz = filtered_df.groupby(by=['content_id'])['grade'].count()
        for ind, dfrow in by_quiz.iteritems():
            feature_matrix[id,quiz_id_list.index(ind)+feature_index_offset] = dfrow

    # save using cpickle
    pickle.dump( feature_matrix, open( "featurematrix_%d.p" % (course_id), "wb" ) )
    pickle.dump( student_id_list, open( "studentidlist_%d.p" % (course_id), "wb" ) )
    pickle.dump( weekly_feature_labels, open( "weeklyfeaturelabels_%d.p" % (course_id), "wb" ) )
    pickle.dump( student_details_dict, open( "studentdetailsdict%d.p" % (course_id), "wb" ) )

def findgroups(course_id):
    """
    Find student groups from Features matrix. Uses k-means clustering.
    Displays plot using matplotlib graph

    Args:
        course_id: course id

    """
    # load from cpickle
    feature_matrix = pickle.load( open( "featurematrix_%d.p" % (course_id), "rb" ) )
    student_id_list = pickle.load( open( "studentidlist_%d.p" % (course_id), "rb" ) )

    min_max_scaler = preprocessing.MinMaxScaler()
    X_normalized = min_max_scaler.fit_transform(feature_matrix)

    X_tsne = TSNE(learning_rate=100, n_components=2).fit_transform(X_normalized)

    reduced_data = X_tsne
    kmeans = KMeans(init='k-means++', n_clusters=8, n_init=10)
    kmeans.fit(reduced_data)

    # Step size of the mesh. Decrease to increase the quality of the VQ.
    h = .02     # point in the mesh [x_min, m_max]x[y_min, y_max].

    # Plot the decision boundary. For that, we will assign a color to each
    x_min, x_max = reduced_data[:, 0].min() + 1, reduced_data[:, 0].max() - 1
    y_min, y_max = reduced_data[:, 1].min() + 1, reduced_data[:, 1].max() - 1
    xx, yy = np.meshgrid(np.arange(x_min, x_max, h), np.arange(y_min, y_max, h))

    # Obtain labels for each point in mesh. Use last trained model.
    Z = kmeans.predict(np.c_[xx.ravel(), yy.ravel()])

    # Put the result into a color plot
    Z = Z.reshape(xx.shape)
    plt.figure(1)
    plt.clf()
    plt.imshow(Z, interpolation='nearest',
               extent=(xx.min(), xx.max(), yy.min(), yy.max()),
               cmap=plt.cm.Paired,
               aspect='auto', origin='lower')

    plt.plot(reduced_data[:, 0], reduced_data[:, 1], 'k.', markersize=2)

    # Plot the centroids as a white X
    centroids = kmeans.cluster_centers_
    plt.scatter(centroids[:, 0], centroids[:, 1],
                marker='x', s=169, linewidths=3,
                color='w', zorder=10)
    plt.title('K-means clustering (TSNE-reduced data)\n'
              'Centroids are marked with white cross')
    plt.xlim(x_min, x_max)
    plt.ylim(y_min, y_max)
    plt.xticks(())
    plt.yticks(())
    plt.show()

if __name__ == "__main__":
    main()
