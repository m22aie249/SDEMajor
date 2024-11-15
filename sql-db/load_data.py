import csv
from common.constants import *
from common.utils import *

import mysql.connector
from dotenv import load_dotenv
load_dotenv()

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

dataset_path = "../dataset"


# --------------------------------------------------
# professionals
# --------------------------------------------------
print("Loading professionals.csv")
insertQ = "INSERT INTO professionals (professionals_id, professionals_location, professionals_industry, professionals_headline, professionals_date_joined) VALUES "
numQueries = -1

with open(f"{dataset_path}/{PROFESSIONALS}.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        # print(f"row-num: {row}")
        if numQueries==-1:
            numQueries +=1
            continue
        insert = ''+insertQ
        # print(row[4])
        datetimeVals = row[4].split(' ')
        datetime = datetimeVals[0]+' '+datetimeVals[1]
        for i in range(1,len(row)-1):
            # row[i]=row[i].replace('"','\\"')
            row[i]=row[i].replace("'","\\'")

        # print(datetime)
        insert += """('{}','{}','{}','{}','{}');""".format(row[0],row[1],row[2],row[3],datetime)
        # print("Executing Query: ",insert)
        mycursor.execute(insert)
        numQueries +=1
mydb.commit()
print("No. of rows inserted = ",numQueries)


# --------------------------------------------------
# students
# --------------------------------------------------
print("Loading students.csv")
insertQ = "INSERT INTO students (students_id,students_location,students_date_joined) VALUES "
numQueries = -1

with open(f"{dataset_path}/{STUDENTS}.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        if numQueries==-1:
            numQueries +=1
            continue

        insert = ''+insertQ
        datetimeVals = row[2].split(' ')
        datetime = datetimeVals[0]+' '+datetimeVals[1]
        for i in range(1,len(row)-1):
            # row[i]=row[i].replace('"','\\"')
            row[i]=row[i].replace("'","\\'")

        # print(datetime)
        insert += """('{}','{}','{}');""".format(row[0],row[1],datetime)
        # print("Executing Query: ",insert)
        mycursor.execute(insert)
        numQueries +=1
mydb.commit()
print("No. of rows inserted = ",numQueries)


# --------------------------------------------------
# tags
# --------------------------------------------------
print("Loading tags.csv")
insertQ = "INSERT INTO tags (tags_tag_id,tags_tag_name) VALUES "
numQueries = -1

with open(f"{dataset_path}/tags.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        if numQueries==-1:
            numQueries +=1
            continue
        insert = ''+insertQ
        for i in range(1,len(row)):
            # row[i]=row[i].replace('"','\\"')
            row[i]=row[i].replace("'","\\'")

        # print(datetime)
        insert += """({},'{}');""".format(row[0],row[1])
        # print("Executing Query: ",insert)
        mycursor.execute(insert)
        numQueries +=1
mydb.commit()
print("No. of rows inserted = ",numQueries)


# --------------------------------------------------
# groups_
# --------------------------------------------------
print("Loading groups.csv")
insertQ = "INSERT INTO groups_ (groups_id,groups_group_type) VALUES "
numQueries = -1

with open(f"{dataset_path}/{GROUPS}.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        if numQueries==-1:
            numQueries +=1
            continue

        insert = ''+insertQ
        for i in range(1,len(row)):
            # row[i]=row[i].replace('"','\\"')
            row[i]=row[i].replace("'","\\'")

        # print(datetime)
        insert += """('{}','{}');""".format(row[0],row[1])
        # print("Executing Query: ",insert)
        mycursor.execute(insert)
        numQueries +=1
mydb.commit()
print("No. of rows inserted = ",numQueries)


# --------------------------------------------------
# questions
# --------------------------------------------------
print("Loading questions.csv")
insertQ = "INSERT INTO questions (questions_id,questions_author_id,questions_date_added,questions_title,questions_body) VALUES "
numQueries = -1

with open(f"{dataset_path}/{QUESTIONS}.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        if numQueries==-1:
            numQueries +=1
            continue
        insert = ''+insertQ
        # print(row[4])
        datetimeVals = row[2].split(' ')
        datetime = datetimeVals[0]+' '+datetimeVals[1]
        for i in range(1,len(row)):
            # row[i]=row[i].replace('"','\\"')
            row[i]=row[i].replace("\\","\\\\'")
            row[i]=row[i].replace("'","\\'")


        # print(datetime)
        insert += """('{}','{}','{}','{}','{}');""".format(row[0],row[1],datetime,row[3],row[4])
        # print("Executing Query: ",insert)
        mycursor.execute(insert)
        numQueries +=1
mydb.commit()
print("No. of rows inserted = ",numQueries)


# --------------------------------------------------
# tag_questions
# --------------------------------------------------
print("Loading tag_questions.csv")
insertQ = "INSERT INTO tag_questions (tag_questions_tag_id,tag_questions_question_id) VALUES "
numQueries = -1

with open(f"{dataset_path}/{TAG_QUESTIONS}.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        if numQueries==-1:
            numQueries +=1
            continue
        insert = ''+insertQ
        for i in range(1,len(row)-1):
            # row[i]=row[i].replace('"','\\"')
            row[i]=row[i].replace("'","\\'")

        # print(datetime)
        insert += """({},'{}');""".format(row[0],row[1])
        # print("Executing Query: ",insert)
        mycursor.execute(insert)
        numQueries +=1
mydb.commit()
print("No. of rows inserted = ",numQueries)


# --------------------------------------------------
# tag_users
# --------------------------------------------------
print("Loading tag_users.csv")
insertQ = "INSERT INTO tag_users (tag_users_tag_id,tag_users_user_id) VALUES "
numQueries = -1

with open(f"{dataset_path}/{TAG_USERS}.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        if numQueries==-1:
            numQueries +=1
            continue
        insert = ''+insertQ
        for i in range(1,len(row)-1):
            # row[i]=row[i].replace('"','\\"')
            row[i]=row[i].replace("'","\\'")

        # print(datetime)
        insert += """({},'{}');""".format(row[0],row[1])
        # print("Executing Query: ",insert)
        mycursor.execute(insert)
        numQueries +=1
mydb.commit()
print("No. of rows inserted = ",numQueries)


# --------------------------------------------------
# group_memberships
# --------------------------------------------------
print("Loading group_memberships.csv")
insertQ = "INSERT INTO group_memberships (group_memberships_group_id,group_memberships_user_id) VALUES "
numQueries = -1

with open(f"{dataset_path}/{GROUP_MEMBERSHIPS}.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        if numQueries==-1:
            numQueries +=1
            continue
        insert = ''+insertQ
        for i in range(1,len(row)-1):
            # row[i]=row[i].replace('"','\\"')
            row[i]=row[i].replace("'","\\'")

        # print(datetime)
        insert += """('{}','{}');""".format(row[0],row[1])
        # print("Executing Query: ",insert)
        mycursor.execute(insert)
        numQueries +=1
mydb.commit()
print("No. of rows inserted = ",numQueries)


# --------------------------------------------------
# school_memberships
# --------------------------------------------------
print("Loading school_memberships.csv")
insertQ = "INSERT INTO school_memberships (school_memberships_school_id,school_memberships_user_id) VALUES "
numQueries = -1

with open(f"{dataset_path}/{SCHOOL_MEMBERSHIPS}.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        if numQueries==-1:
            numQueries +=1
            continue
        insert = ''+insertQ
        for i in range(1,len(row)-1):
            # row[i]=row[i].replace('"','\\"')
            row[i]=row[i].replace("'","\\'")

        # print(datetime)
        insert += """({},'{}');""".format(row[0],row[1])
        # print("Executing Query: ",insert)
        mycursor.execute(insert)
        numQueries +=1
mydb.commit()
print("No. of rows inserted = ",numQueries)


# --------------------------------------------------
# answers
# --------------------------------------------------
print("Loading answers.csv")
insertQ = "INSERT INTO answers (answers_id,answers_author_id,answers_question_id,answers_date_added,answers_body) VALUES "
numQueries = -1

with open(f"{dataset_path}/{ANSWERS}.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        if numQueries==-1:
            numQueries +=1
            continue
        insert = ''+insertQ
        # print(row[4])
        datetimeVals = row[3].split(' ')
        datetime = datetimeVals[0]+' '+datetimeVals[1]
        for i in range(1,len(row)):
            # row[i]=row[i].replace('"','\\"')
            row[i]=row[i].replace("\\","\\\\'")
            row[i]=row[i].replace("'","\\'")

        # print(datetime)
        insert += """('{}','{}','{}','{}','{}');""".format(row[0],row[1],row[2],datetime,row[4])
        # print("Executing Query: ",insert)
        mycursor.execute(insert)
        numQueries +=1
mydb.commit()
print("No. of rows inserted = ",numQueries)


# --------------------------------------------------
# comments
# --------------------------------------------------
print("Loading comments.csv")
insertQ = "INSERT INTO comments (comments_id,comments_author_id,comments_parent_content_id,comments_date_added,comments_body) VALUES "
numQueries = -1

with open(f"{dataset_path}/{COMMENTS}.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        if numQueries==-1:
            numQueries +=1
            continue
        insert = ''+insertQ
        # print(row[4])
        datetimeVals = row[3].split(' ')
        datetime = datetimeVals[0]+' '+datetimeVals[1]
        for i in range(1,len(row)):
            # row[i]=row[i].replace('"','\\"')
            row[i]=row[i].replace("\\","\\\\'")
            row[i]=row[i].replace("'","\\'")

        # print(datetime)
        insert += """('{}','{}','{}','{}','{}');""".format(row[0],row[1],row[2],datetime,row[4])
        # print("Executing Query: ",insert)
        mycursor.execute(insert)
        numQueries +=1
mydb.commit()
print("No. of rows inserted = ",numQueries)


# --------------------------------------------------
# emails
# --------------------------------------------------
print("Loading emails.csv")
insertQ = "INSERT INTO emails (emails_id,emails_recipient_id,emails_date_sent,emails_frequency_level) VALUES "
numQueries = -1

with open(f"{dataset_path}/{EMAILS}.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        if numQueries==-1:
            numQueries +=1
            continue
        insert = ''+insertQ
        # print(row[4])
        datetimeVals = row[2].split(' ')
        datetime = datetimeVals[0]+' '+datetimeVals[1]
        for i in range(1,len(row)):
            # row[i]=row[i].replace('"','\\"')
            row[i]=row[i].replace("'","\\'")

        # print(datetime)
        insert += """({},'{}','{}','{}');""".format(row[0],row[1],datetime,row[3])
        # print("Executing Query: ",insert)
        mycursor.execute(insert)
        numQueries +=1
mydb.commit()
print("No. of rows inserted = ",numQueries)


# --------------------------------------------------
# matches
# --------------------------------------------------
print("Loading matches.csv")
insertQ = "INSERT INTO matches (matches_email_id,matches_question_id) VALUES "
numQueries = -1

with open(f"{dataset_path}/{MATCHES}.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        if numQueries==-1:
            numQueries +=1
            continue
        insert = ''+insertQ
        for i in range(1,len(row)-1):
            # row[i]=row[i].replace('"','\\"')
            row[i]=row[i].replace("'","\\'")

        # print(datetime)
        insert += """({},'{}');""".format(row[0],row[1])
        # print("Executing Query: ",insert)
        mycursor.execute(insert)
        numQueries +=1
mydb.commit()
print("No. of rows inserted = ",numQueries)


# --------------------------------------------------
# question_scores
# --------------------------------------------------
print("Loading question_scores.csv")
insertQ = "INSERT INTO question_scores (id, score) VALUES "
numQueries = -1
ignored=0
import csv
with open(f"{dataset_path}/{QUESTIONS_SCORES}.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        if numQueries==-1:
            numQueries +=1
            continue
        insert = ''+insertQ
        insert += """('{}',{});""".format(row[0],row[1])
        try:
            mycursor.execute(insert)
        except:
            ignored+=1
            continue
        numQueries +=1
mydb.commit()
print("No. of rows inserted = ",numQueries)


# --------------------------------------------------
# answer_scores
# --------------------------------------------------
print("Loading answer_scores.csv")
insertQ = "INSERT INTO answer_scores (id, score) VALUES "
numQueries = -1
ignored=0

with open(f"{dataset_path}/{ANSWERS_SCORES}.csv", newline='', encoding=ISO_8859_1) as csvfile:
    readerObj = csv.reader(csvfile, delimiter=',')
    for row in readerObj:
        if numQueries==-1:
            numQueries +=1
            continue
        insert = ''+insertQ
        insert += """('{}',{});""".format(row[0],row[1])
        try:
            mycursor.execute(insert)
        except Exception as e:
            print(f"Error: {e}")
            print(f"Insert Statement: {insert}")
            ignored+=1
            print("Row ignored: ", numQueries)
            # continue
        numQueries +=1
        # print("Row inserted: ",numQueries)
mydb.commit()
print("No. of rows inserted = ",numQueries)
