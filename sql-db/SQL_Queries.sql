----------------------
-- Selection
----------------------

-- Query 1
SELECT p.* 
FROM `careervillage`.professionals p 
JOIN `careervillage`.tag_users tu ON p.professionals_id = tu.tag_users_user_id 
JOIN `careervillage`.tags t ON tu.tag_users_tag_id = t.tags_tag_id
WHERE tags_tag_name = 'college';

-- Query 2
SELECT * 
FROM `careervillage`.students s 
JOIN `careervillage`.group_memberships gm ON students_id = gm.group_memberships_user_id 
JOIN `careervillage`.groups_ g ON g.groups_id = gm.group_memberships_group_id 
JOIN `careervillage`.tag_users tu ON tu.tag_users_user_id = s.students_id 
JOIN `careervillage`.tags t ON t.tags_tag_id = tu.tag_users_tag_id
WHERE t.tags_tag_name = 'college' AND g.groups_group_type = 'youth program';

-- Query 3
SELECT * 
FROM `careervillage`.professionals p 
JOIN `careervillage`.emails e ON p.professionals_id = e.emails_recipient_id 
WHERE p.professionals_id = '0079e89bf1544926b98310e81315b9f1';

----------------------
-- Recursion
----------------------
-- Query 4
WITH RECURSIVE answer_replies AS(
SELECT answers_id, answers_author_id, answers_question_id, answers_date_added, answers_body 
FROM `careervillage`.answers 
WHERE answers_question_id IS not null UNION all 
SELECT a.answers_id, a.answers_author_id, a.answers_question_id, a.answers_date_added, a.answers_body
FROM `careervillage`.answers a 
INNER JOIN answer_replies ar ON ar.answers_id = a.answers_question_id ) 
SELECT * FROM answer_replies ar 
LEFT JOIN `careervillage`.questions q 
ON ar.answers_question_id = q.questions_id;


-- Query 5
WITH RECURSIVE answer_replies AS(
SELECT 1 as level,answers_id, answers_author_id, answers_question_id, answers_date_added, answers_body 
FROM `careervillage`.answers 
WHERE answers_question_id IS not null UNION all SELECT level+1,
a.answers_id, a.answers_author_id,
a.answers_question_id,
a.answers_date_added, a.answers_body
FROM `careervillage`.answers a 
INNER JOIN answer_replies ar ON ar.answers_id = a.answers_question_id 
WHERE level <=2) 
SELECT * FROM answer_replies ar LEFT JOIN `careervillage`.questions q ON ar.answers_question_id = q.questions_id;

-- Query 6
WITH RECURSIVE answer_replies AS(
SELECT 1 as level,answers_id, answers_author_id, answers_question_id, answers_date_added, answers_body 
FROM `careervillage`.answers 
WHERE answers_question_id IS not null UNION all SELECT level+1,
a.answers_id, a.answers_author_id, a.answers_question_id, a.answers_date_added, a.answers_body
FROM `careervillage`.answers a 
INNER JOIN answer_replies ar ON ar.answers_id = a.answers_question_id 
WHERE level <=3) 
SELECT * 
FROM answer_replies ar 
LEFT JOIN `careervillage`.questions q ON ar.answers_question_id = q.questions_id;

----------------------
-- Aggregation
----------------------

-- Query 7
SELECT count(professionals_id) 
FROM `careervillage`.professionals p 
JOIN `careervillage`.answers a ON p.professionals_id = a.answers_author_id;

-- Query 8
SELECT count(*) FROM 
(SELECT DISTINCT p.* 
FROM `careervillage`.professionals p 
JOIN `careervillage`.tag_users tu ON p.professionals_id = tu.tag_users_user_id 
JOIN `careervillage`.tags t ON tu.tag_users_tag_id = t.tags_tag_id
WHERE t.tags_tag_name = 'college') 
AS temp;

-- Query 9
SELECT tags.tags_tag_id, tags_tag_name, COUNT(p.professionals_id) AS number_of_professionals 
FROM `careervillage`.tags
JOIN `careervillage`.tag_users tu ON tags.tags_tag_id = tu.tag_users_tag_id 
JOIN `careervillage`.professionals p ON p.professionals_id = tu.tag_users_user_id 
GROUP BY tags.tags_tag_id, tags_tag_name 
ORDER BY COUNT(p.professionals_id) 
DESC LIMIT 1;


----------------------
-- Pattern Match
----------------------

-- Query 10
SELECT q.questions_id, t.tags_tag_id, a.answers_id 
FROM `careervillage`.tags t 
JOIN `careervillage`.tag_questions tq ON t.tags_tag_id = tq.tag_questions_tag_id 
JOIN `careervillage`.questions q ON tq.tag_questions_question_id = q.questions_id 
JOIN `careervillage`.answers a ON a.answers_question_id = questions_id;

-- Query 11
SELECT g.groups_id, professionals_id, students_id 
FROM `careervillage`.groups_ g 
JOIN `careervillage`.group_memberships gm ON g.groups_id = gm.group_memberships_group_id 
JOIN (SELECT group_memberships_group_id AS group_id, professionals_id 
FROM `careervillage`.professionals p 
JOIN `careervillage`.group_memberships gm1 ON gm1.group_memberships_user_id = p.professionals_id) pg 
ON pg.group_id = gm.group_memberships_group_id 
JOIN (SELECT group_memberships_group_id AS group_id, students_id 
FROM `careervillage`.students s
JOIN `careervillage`.group_memberships gm2 ON s.students_id = gm2.group_memberships_user_id) sg 
ON sg.group_id = gm.group_memberships_group_id;

-- Query 12
SELECT pt.tags_id, st.students_id, pt.professionals_id 
FROM `careervillage`.tags t 
JOIN `careervillage`.tag_users tu ON t.tags_tag_id = tu.tag_users_tag_id 
JOIN (SELECT u.tag_users_tag_id AS tags_id, professionals_id 
FROM `careervillage`.professionals p
JOIN `careervillage`.tag_users u ON p.professionals_id = u.tag_users_user_id) pt 
ON pt.tags_id= t.tags_tag_id 
JOIN (SELECT u.tag_users_tag_id AS tags_id, students_id 
FROM `careervillage`.students s
JOIN `careervillage`.tag_users u ON s.students_id = u.tag_users_user_id) st 
ON st.tags_id = t.tags_tag_id LIMIT 100000;



