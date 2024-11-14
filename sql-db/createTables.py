import os
from common.constants import *
from common.utils import *

import mysql.connector
from dotenv import load_dotenv
load_dotenv()


TABLES = {}
TABLES[PROFESSIONALS] = (""" 
CREATE TABLE professionals (
    professionals_id VARCHAR(50) PRIMARY KEY , 
    professionals_location VARCHAR(255), 
    professionals_industry VARCHAR(255), 
    professionals_headline VARCHAR(255), 
    professionals_date_joined DATETIME)
""")

TABLES[STUDENTS] = (""" 
CREATE TABLE students (
    students_id VARCHAR(50) PRIMARY KEY , 
    students_location VARCHAR(255), 
    students_date_joined DATETIME)
""")

TABLES[TAGS] = (""" 
CREATE TABLE tags (
    tags_tag_id INT PRIMARY KEY , 
    tags_tag_name VARCHAR(255)
    )
""")

TABLES[GROUPS] = (""" 
CREATE TABLE groups_ (
    groups_id VARCHAR(50) PRIMARY KEY , 
    groups_group_type VARCHAR(255)
    )
""")

TABLES[QUESTIONS] = (""" 
CREATE TABLE questions (
    questions_id VARCHAR(50) PRIMARY KEY , 
    questions_author_id VARCHAR (50),
    questions_date_added DATETIME,
    questions_title TEXT,
    questions_body TEXT
    )
""")

# FOREIGN KEY(questions_author_id) REFERENCES students(students_id)),
#     FOREIGN KEY(questions_author_id) REFERENCES professionals(professionals_id))

TABLES[TAG_QUESTIONS] = (""" 
CREATE TABLE tag_questions (
    tag_questions_tag_id INT, 
    tag_questions_question_id VARCHAR(50),
    FOREIGN KEY (tag_questions_tag_id) REFERENCES tags (tags_tag_id),
    FOREIGN KEY (tag_questions_question_id) REFERENCES questions (questions_id)
    )
""")

TABLES[TAG_USERS] = (""" 
CREATE TABLE tag_users (
    tag_users_tag_id INT,
    tag_users_user_id VARCHAR(50), 
    FOREIGN KEY (tag_users_tag_id) REFERENCES tags (tags_tag_id)
    )
""")

# FOREIGN KEY (tag_users_user_id) REFERENCES students (students_id),
#     FOREIGN KEY (tag_users_user_id) REFERENCES professionals (professionals_id)

TABLES[GROUP_MEMBERSHIPS] = (""" 
CREATE TABLE group_memberships (
    group_memberships_group_id VARCHAR(50), 
    group_memberships_user_id VARCHAR(50), 
    FOREIGN KEY (group_memberships_group_id) REFERENCES groups_ (groups_id)
    )
""")

# FOREIGN KEY (group_memberships_user_id) REFERENCES students (students_id),
#     FOREIGN KEY (group_memberships_user_id) REFERENCES professionals (professionals_id),

TABLES[SCHOOL_MEMBERSHIPS] = (""" 
CREATE TABLE school_memberships (
    school_memberships_school_id INT, 
    school_memberships_user_id VARCHAR(50)
    )
""")

# ,
#     FOREIGN KEY (school_memberships_user_id) REFERENCES students (students_id),
#     FOREIGN KEY (school_memberships_user_id) REFERENCES professionals (professionals_id)

TABLES[ANSWERS] = (""" 
CREATE TABLE answers (
    answers_id VARCHAR (50) PRIMARY KEY,
    answers_author_id VARCHAR (50), 
    answers_question_id VARCHAR (50),
    answers_date_added DATETIME,
    answers_body TEXT, 
    FOREIGN KEY (answers_question_id) REFERENCES questions (questions_id)
    )
""")

# FOREIGN KEY (answers_author_id) REFERENCES professionals (professionals_id),

TABLES[COMMENTS] = (""" 
CREATE TABLE comments (
    comments_id VARCHAR (50),
    comments_author_id VARCHAR (50), 
    comments_parent_content_id VARCHAR(50),
    comments_date_added DATETIME,
    comments_body TEXT
    )
""")

# FOREIGN KEY (comments_author_id) REFERENCES students (students_id),
#     FOREIGN KEY (comments_author_id) REFERENCES professionals (professionals_id),
#     FOREIGN KEY (comments_parent_content_id) REFERENCES questions (questions_id),
#     FOREIGN KEY (comments_parent_content_id) REFERENCES answers (answers_id)

TABLES[EMAILS] = (""" 
CREATE TABLE emails (
    emails_id INT PRIMARY KEY, 
    emails_recipient_id VARCHAR (50), 
    emails_date_sent DATETIME,
    emails_frequency_level VARCHAR(255)
    )
""")

# FOREIGN KEY (emails_recipient_id) REFERENCES students (students_id),
#     FOREIGN KEY (emails_recipient_id) REFERENCES professionals (professionals_id)

TABLES[MATCHES] = (""" 
CREATE TABLE matches (
    matches_email_id INT, 
    matches_question_id VARCHAR(50), 
    FOREIGN KEY (matches_email_id) REFERENCES emails (emails_id),
    FOREIGN KEY (matches_question_id) REFERENCES questions (questions_id)
    )
""")

TABLES[QUESTIONS_SCORES] = (""" 
CREATE TABLE question_scores (
    id VARCHAR(50) PRIMARY KEY , 
    score INT,
    FOREIGN KEY (id) REFERENCES questions (questions_id))
""")

TABLES[ANSWERS_SCORES] = (""" 
CREATE TABLE answer_scores (
    id VARCHAR(50) PRIMARY KEY , 
    score INT,
    FOREIGN KEY (id) REFERENCES answers (answers_id))
""")

db_user = os.environ.get(DB_USER)
db_pwd = os.environ.get(DB_PASSWORD)
db_name = os.environ.get(DB_NAME)

mydb = mysql.connector.connect(
    host="localhost",
    user=db_user,
    password=db_pwd,
    database=db_name
)

mycursor = mydb.cursor()


for table in TABLES:

    query = TABLES[table]
    print("Creating table: ", table)
    print("Executing query: ", query)
    mycursor.execute(query)
    for x in mycursor:
        print(x)

mydb.commit()
