create database project3;
show databases;
use project3;

#CASE STUDY 1
create table job_data(
ds varchar(50),
job_id int not null,
actor_id int not null,
event varchar(50),
language varchar(50),
time_spent int not null,
org char(2)
);
#know about the save file path
show variables like 'secure_file_priv';
#uplode exel csv file	

LOAD DATA INFILE 
"C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/job_data.csv"
INTO TABLE job_data
FIELDS TERMINATED BY','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from job_data;

#A Jobs Reviewed Over Time:
select avg(t) as 'Average jobs examined in an hour, each day',
avg(p) as 'Average jobs reviewed in one second, each day'
from ( select 
ds,
((count(job_id)*3600)/sum(time_spent)) as t,
((count(job_id))/sum(time_spent))  as p 
from job_data where month(ds)=11 group by ds) a;

SELECT DATE_FORMAT(STR_TO_DATE(ds, '%Y-%m-%d %H:%i:%s'), '%Y-%m-%d') AS date,
HOUR(STR_TO_DATE(ds, '%Y-%m-%d %H:%i:%s')) AS hour,
COUNT(job_id) AS jobs_reviewed
FROM job_data
WHERE DATE_FORMAT(STR_TO_DATE(ds, '%Y-%m-%d %H:%i:%s'), '%Y-%m') = '2020-11'
GROUP BY  date, hour
ORDER BY date, hour;


#B Throughput Analysis:
select round(count(event)/sum(time_spent),2) 
as "Weekly Day Output" from job_data;
select ds as dates,round(count(event)/sum(time_spent),2) 
as "Every Day Output" from job_data 
group by ds order by ds;

#C Language Share Analysis:
SELECT language AS Languages, 
ROUND(100 * COUNT(*)/total, 2) 
AS Percentage, sub.total
FROM job_data
CROSS JOIN (SELECT COUNT(*) AS total FROM job_data) AS sub
GROUP BY language, sub.total;

#D Duplicate Rows Detection:
SELECT actor_id, COUNT(*) AS Duplicates FROM job_data
GROUP BY actor_id HAVING COUNT(*) > 1;


create table users(
user_id int,
created_at varchar(100),
company_id int,
language varchar(100),
activated_at varchar(100),
state varchar(100));

show variables like 'secure_file_priv';

LOAD DATA INFILE 
"C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
INTO TABLE users
FIELDS TERMINATED BY','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from users;

create table events(
user_id int,
occurred_at varchar(100),
event_type varchar(100),
event_name varchar(100),
location varchar(100),
device varchar(50),
user_type int
);

LOAD DATA INFILE 
"C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
INTO TABLE events
FIELDS TERMINATED BY','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from events;

create table email_events(
user_id int,
occurred_at varchar(50),
action varchar(50),
user_type int
);
LOAD DATA INFILE 
"C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
INTO TABLE email_events
FIELDS TERMINATED BY','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from email_events;

#CASE STUDY 2
# A Weekly User Engagement:
SELECT user_id,
WEEK(STR_TO_DATE(occurred_at, '%Y-%m-%d'), 1) AS week_number,
COUNT(*) AS weekly_engagement
FROM events GROUP BY user_id, week_number
ORDER BY user_id, week_number
LIMIT 10;

#B User Growth Analysis:
SELECT DATE_FORMAT(STR_TO_DATE(created_at, '%Y-%m-%d'),
 '%Y-%m-%d') AS registration_month,
COUNT(DISTINCT user_id) AS new_users
FROM users
GROUP BY registration_month
ORDER BY registration_month
LIMIT 10;

#C Weekly Retention Analysis:
WITH user_signups AS (
SELECT user_id,
WEEK(STR_TO_DATE(created_at, '%Y-%m-%d')) AS signup_week
FROM users
),
user_engagement AS (
SELECT user_id,
WEEK(STR_TO_DATE(occurred_at, '%Y-%m-%d')) AS engagement_week
FROM events)
SELECT
    us.signup_week AS signup_week,
    ue.engagement_week AS engagement_week,
    COUNT(DISTINCT ue.user_id) AS retained_users
FROM user_signups us
JOIN user_engagement ue ON us.user_id = ue.user_id
WHERE ue.engagement_week >= us.signup_week
GROUP BY signup_week, engagement_week
ORDER BY signup_week, engagement_week
LIMIT 10;

#D Weekly Engagement Per Device:
SELECT WEEK(STR_TO_DATE(occurred_at, '%Y-%m-%d')) AS week_number,
device,
COUNT(*) AS weekly_engagement
FROM events
GROUP BY week_number, device
ORDER BY week_number, device
LIMIT 10;

#E Email Engagement Analysis:
SELECT DATE_FORMAT(STR_TO_DATE(occurred_at, '%Y-%m-%d'), '%Y-%m') AS month,
action,
COUNT(*) AS action_count,
COUNT(DISTINCT user_id) AS unique_users
FROM email_events
GROUP BY month, action
ORDER BY month, action
LIMIT 10;







