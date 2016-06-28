-- phpMyAdmin SQL Dump
-- version 4.4.1.1
-- http://www.phpmyadmin.net
--
-- Host: localhost:8889
-- Generation Time: Nov 10, 2015 at 01:45 AM
-- Server version: 5.5.42
-- PHP Version: 5.6.7

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `cloop_olap`
--

-- --------------------------------------------------------

--
-- Table structure for table `dim_dates`
--

CREATE TABLE `dim_dates` (
  `id` varchar(11) NOT NULL,
  `date_day` int(11) NOT NULL,
  `date_year` int(11) NOT NULL,
  `date_month` int(11) NOT NULL,
  `date_dayinweek` int(11) NOT NULL,
  `date_week` int(11) NOT NULL,
  `unixtimestamp` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `dim_pages`
--

CREATE TABLE `dim_pages` (
  `id` int(11) NOT NULL,
  `course_id` int(11) NOT NULL,
  `content_type` varchar(200) NOT NULL,
  `content_id` int(11) NOT NULL,
  `parent_id` int(11) NOT NULL,
  `order_no` int(11) NOT NULL,
  `title` text NOT NULL,
  `page_pk` varchar(1000) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `dim_session`
--

CREATE TABLE `dim_session` (
  `id` int(11) NOT NULL,
  `fact_coursevisits_id` int(11) NOT NULL,
  `session_id` int(11) NOT NULL,
  `course_id` int(11) NOT NULL,
  `unixtimestamp` varchar(1000) NOT NULL,
  `date_id` varchar(11) NOT NULL,
  `session_length_in_mins` double NOT NULL,
  `pageviews` int(11) NOT NULL,
  `date_year` int(11) NOT NULL,
  `date_week` int(11) NOT NULL,
  `date_dayinweek` int(11) NOT NULL,
  `user_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `dim_sessions`
--

CREATE TABLE `dim_sessions` (
  `id` int(11) NOT NULL,
  `session_id` int(11) NOT NULL,
  `Date_Year` int(11) NOT NULL,
  `Date_Week` int(11) NOT NULL,
  `Date_dayinweek` int(11) NOT NULL,
  `Pageviews` int(11) NOT NULL DEFAULT '0',
  `session_length_in_mins` int(11) NOT NULL,
  `course_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `dim_submissionattempts`
--

CREATE TABLE `dim_submissionattempts` (
  `id` int(11) NOT NULL,
  `course_id` int(11) NOT NULL,
  `content_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `grade` varchar(50) NOT NULL,
  `unixtimestamp` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `dim_submissiontypes`
--

CREATE TABLE `dim_submissiontypes` (
  `id` int(11) NOT NULL,
  `content_type` varchar(1000) NOT NULL,
  `course_id` int(11) NOT NULL,
  `content_id` int(11) NOT NULL,
  `timeopen` varchar(1000) NOT NULL,
  `timeclose` varchar(1000) NOT NULL,
  `grade` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `dim_users`
--

CREATE TABLE `dim_users` (
  `id` int(11) NOT NULL,
  `lms_id` varchar(1000) NOT NULL,
  `firstname` varchar(1000) DEFAULT NULL,
  `lastname` varchar(1000) DEFAULT NULL,
  `username` varchar(1000) NOT NULL,
  `role` varchar(1000) DEFAULT NULL,
  `email` varchar(1000) DEFAULT NULL,
  `user_pk` varchar(1000) NOT NULL,
  `course_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `fact_coursevisits`
--

CREATE TABLE `fact_coursevisits` (
  `id` int(11) NOT NULL,
  `date_id` varchar(1000) NOT NULL,
  `course_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `page_id` int(11) NOT NULL,
  `pageview` int(11) NOT NULL DEFAULT '1',
  `time_id` varchar(1000) NOT NULL,
  `module` varchar(1000) DEFAULT NULL,
  `action` varchar(1000) DEFAULT NULL,
  `url` varchar(5000) DEFAULT NULL,
  `section_id` int(11) DEFAULT NULL,
  `datetime` varchar(1000) NOT NULL,
  `user_pk` varchar(1000) DEFAULT NULL,
  `page_pk` varchar(1000) DEFAULT NULL,
  `section_pk` varchar(1000) DEFAULT NULL,
  `unixtimestamp` varchar(5000) NOT NULL,
  `section_order` int(11) DEFAULT NULL,
  `info` varchar(5000) DEFAULT NULL,
  `session_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `Summary_CourseAssessmentVisitsByDayInWeek`
--

CREATE TABLE `Summary_CourseAssessmentVisitsByDayInWeek` (
  `id` int(11) NOT NULL,
  `Date_Year` int(11) NOT NULL,
  `Date_Day` int(11) NOT NULL,
  `Date_Week` int(11) NOT NULL,
  `Date_dayinweek` int(11) NOT NULL,
  `Pageviews` int(11) NOT NULL DEFAULT '0',
  `course_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `Summary_CourseCommunicationVisitsByDayInWeek`
--

CREATE TABLE `Summary_CourseCommunicationVisitsByDayInWeek` (
  `id` int(11) NOT NULL,
  `Date_Year` int(11) NOT NULL,
  `Date_Day` int(11) NOT NULL,
  `Date_Week` int(11) NOT NULL,
  `Date_dayinweek` int(11) NOT NULL,
  `Pageviews` int(11) NOT NULL DEFAULT '0',
  `course_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `summary_courses`
--

CREATE TABLE `summary_courses` (
  `id` int(11) NOT NULL,
  `course_id` int(11) NOT NULL,
  `weekly_json` longtext,
  `users_counts_table` longtext,
  `users_vis_table` longtext,
  `access_counts_table` longtext,
  `access_vis_table` longtext,
  `content_counts_table` longtext,
  `content_user_table` longtext,
  `communication_counts_table` longtext,
  `communication_user_table` longtext,
  `assessment_counts_table` longtext,
  `assessment_user_table` longtext,
  `content_hiddenlist` longtext,
  `communication_hiddenlist` longtext,
  `assessment_hiddenlist` longtext,
  `forum_posts_table` longtext,
  `contentcoursepageviews` longtext,
  `communicationcoursepageviews` longtext,
  `assessmentcoursepageviews` longtext,
  `assessmentgrades` longtext,
  `sitetree` longtext
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `Summary_CourseVisitsByDayInWeek`
--

CREATE TABLE `Summary_CourseVisitsByDayInWeek` (
  `id` int(11) NOT NULL,
  `Date_Year` int(11) NOT NULL,
  `Date_Day` int(11) NOT NULL,
  `Date_Week` int(11) NOT NULL,
  `Date_dayinweek` int(11) NOT NULL,
  `Pageviews` int(11) NOT NULL DEFAULT '0',
  `course_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `summary_discussion`
--

CREATE TABLE `summary_discussion` (
  `id` int(11) NOT NULL,
  `course_id` int(11) NOT NULL,
  `forum_id` int(11) NOT NULL,
  `title` text NOT NULL,
  `no_posts` int(11) NOT NULL,
  `sna_json` text NOT NULL,
  `discussion_id` int(11) NOT NULL,
  `forum_pk` text NOT NULL,
  `discussion_pk` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `summary_forum`
--

CREATE TABLE `summary_forum` (
  `id` int(11) NOT NULL,
  `course_id` int(11) NOT NULL,
  `forum_id` int(11) NOT NULL,
  `title` text NOT NULL,
  `no_discussions` int(11) NOT NULL,
  `forum_pk` text NOT NULL,
  `all_content` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `Summary_ParticipatingUsersByDayInWeek`
--

CREATE TABLE `Summary_ParticipatingUsersByDayInWeek` (
  `id` int(11) NOT NULL,
  `Date_Year` int(11) NOT NULL,
  `Date_Day` int(11) NOT NULL,
  `Date_Week` int(11) NOT NULL,
  `Date_dayinweek` int(11) NOT NULL,
  `Pageviews` int(11) NOT NULL DEFAULT '0',
  `course_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `summary_posts`
--

CREATE TABLE `summary_posts` (
  `id` int(11) NOT NULL,
  `date_id` varchar(11) NOT NULL,
  `course_id` int(11) NOT NULL,
  `forum_id` int(11) NOT NULL,
  `discussion_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `Summary_SessionAverageLengthByDayInWeek`
--

CREATE TABLE `Summary_SessionAverageLengthByDayInWeek` (
  `id` int(11) NOT NULL,
  `Date_Year` int(11) NOT NULL,
  `Date_Day` int(11) NOT NULL,
  `Date_Week` int(11) NOT NULL,
  `Date_dayinweek` int(11) NOT NULL,
  `session_average_in_minutes` int(11) NOT NULL DEFAULT '0',
  `course_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `Summary_SessionAveragePagesPerSessionByDayInWeek`
--

CREATE TABLE `Summary_SessionAveragePagesPerSessionByDayInWeek` (
  `id` int(11) NOT NULL,
  `Date_Year` int(11) NOT NULL,
  `Date_Day` int(11) NOT NULL,
  `Date_Week` int(11) NOT NULL,
  `Date_dayinweek` int(11) NOT NULL,
  `pages_per_session` int(11) NOT NULL DEFAULT '0',
  `course_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `Summary_SessionsByDayInWeek`
--

CREATE TABLE `Summary_SessionsByDayInWeek` (
  `id` int(11) NOT NULL,
  `Date_Year` int(11) NOT NULL,
  `Date_Day` int(11) NOT NULL,
  `Date_Week` int(11) NOT NULL,
  `Date_dayinweek` int(11) NOT NULL,
  `Sessions` int(11) NOT NULL DEFAULT '0',
  `course_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `Summary_UniquePageViewsByDayInWeek`
--

CREATE TABLE `Summary_UniquePageViewsByDayInWeek` (
  `id` int(11) NOT NULL,
  `Date_Year` int(11) NOT NULL,
  `Date_Day` int(11) NOT NULL,
  `Date_Week` int(11) NOT NULL,
  `Date_dayinweek` int(11) NOT NULL,
  `Pageviews` int(11) NOT NULL DEFAULT '0',
  `course_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `dim_dates`
--
ALTER TABLE `dim_dates`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `id` (`id`);

--
-- Indexes for table `dim_pages`
--
ALTER TABLE `dim_pages`
  ADD PRIMARY KEY (`id`),
  ADD KEY `course_id` (`course_id`),
  ADD KEY `content_id` (`content_id`);

--
-- Indexes for table `dim_session`
--
ALTER TABLE `dim_session`
  ADD PRIMARY KEY (`id`),
  ADD KEY `date_id` (`date_id`),
  ADD KEY `course_id` (`course_id`),
  ADD KEY `session_id` (`session_id`);

--
-- Indexes for table `dim_sessions`
--
ALTER TABLE `dim_sessions`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `dim_submissionattempts`
--
ALTER TABLE `dim_submissionattempts`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `dim_submissiontypes`
--
ALTER TABLE `dim_submissiontypes`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `dim_users`
--
ALTER TABLE `dim_users`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `fact_coursevisits`
--
ALTER TABLE `fact_coursevisits`
  ADD PRIMARY KEY (`id`),
  ADD KEY `date_id` (`date_id`(767)),
  ADD KEY `user_id` (`user_id`),
  ADD KEY `course_id` (`course_id`),
  ADD KEY `date_id_2` (`date_id`(767),`course_id`,`user_id`),
  ADD KEY `course_id_2` (`course_id`,`user_id`);

--
-- Indexes for table `Summary_CourseAssessmentVisitsByDayInWeek`
--
ALTER TABLE `Summary_CourseAssessmentVisitsByDayInWeek`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `id` (`id`),
  ADD KEY `course_id` (`course_id`);

--
-- Indexes for table `Summary_CourseCommunicationVisitsByDayInWeek`
--
ALTER TABLE `Summary_CourseCommunicationVisitsByDayInWeek`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `id` (`id`),
  ADD KEY `course_id` (`course_id`);

--
-- Indexes for table `summary_courses`
--
ALTER TABLE `summary_courses`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `Summary_CourseVisitsByDayInWeek`
--
ALTER TABLE `Summary_CourseVisitsByDayInWeek`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `id` (`id`),
  ADD KEY `course_id` (`course_id`);

--
-- Indexes for table `summary_discussion`
--
ALTER TABLE `summary_discussion`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `summary_forum`
--
ALTER TABLE `summary_forum`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `Summary_ParticipatingUsersByDayInWeek`
--
ALTER TABLE `Summary_ParticipatingUsersByDayInWeek`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `id` (`id`),
  ADD UNIQUE KEY `id_2` (`id`),
  ADD KEY `course_id` (`course_id`);

--
-- Indexes for table `summary_posts`
--
ALTER TABLE `summary_posts`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `Summary_SessionAverageLengthByDayInWeek`
--
ALTER TABLE `Summary_SessionAverageLengthByDayInWeek`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `id` (`id`),
  ADD KEY `course_id` (`course_id`);

--
-- Indexes for table `Summary_SessionAveragePagesPerSessionByDayInWeek`
--
ALTER TABLE `Summary_SessionAveragePagesPerSessionByDayInWeek`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `id` (`id`),
  ADD KEY `course_id` (`course_id`);

--
-- Indexes for table `Summary_SessionsByDayInWeek`
--
ALTER TABLE `Summary_SessionsByDayInWeek`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `id` (`id`),
  ADD KEY `course_id` (`course_id`);

--
-- Indexes for table `Summary_UniquePageViewsByDayInWeek`
--
ALTER TABLE `Summary_UniquePageViewsByDayInWeek`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `id` (`id`),
  ADD KEY `course_id` (`course_id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `dim_pages`
--
ALTER TABLE `dim_pages`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `dim_session`
--
ALTER TABLE `dim_session`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `dim_sessions`
--
ALTER TABLE `dim_sessions`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `dim_submissionattempts`
--
ALTER TABLE `dim_submissionattempts`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `dim_submissiontypes`
--
ALTER TABLE `dim_submissiontypes`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `dim_users`
--
ALTER TABLE `dim_users`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `fact_coursevisits`
--
ALTER TABLE `fact_coursevisits`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `Summary_CourseAssessmentVisitsByDayInWeek`
--
ALTER TABLE `Summary_CourseAssessmentVisitsByDayInWeek`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `Summary_CourseCommunicationVisitsByDayInWeek`
--
ALTER TABLE `Summary_CourseCommunicationVisitsByDayInWeek`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `summary_courses`
--
ALTER TABLE `summary_courses`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `Summary_CourseVisitsByDayInWeek`
--
ALTER TABLE `Summary_CourseVisitsByDayInWeek`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `summary_discussion`
--
ALTER TABLE `summary_discussion`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `summary_forum`
--
ALTER TABLE `summary_forum`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `Summary_ParticipatingUsersByDayInWeek`
--
ALTER TABLE `Summary_ParticipatingUsersByDayInWeek`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `summary_posts`
--
ALTER TABLE `summary_posts`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `Summary_SessionAverageLengthByDayInWeek`
--
ALTER TABLE `Summary_SessionAverageLengthByDayInWeek`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `Summary_SessionAveragePagesPerSessionByDayInWeek`
--
ALTER TABLE `Summary_SessionAveragePagesPerSessionByDayInWeek`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `Summary_SessionsByDayInWeek`
--
ALTER TABLE `Summary_SessionsByDayInWeek`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `Summary_UniquePageViewsByDayInWeek`
--
ALTER TABLE `Summary_UniquePageViewsByDayInWeek`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
