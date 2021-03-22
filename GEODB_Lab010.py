# Import Libraries
import psycopg2
import os
import csv

#Connect with postgres database
conn = psycopg2.connect(dbname='pkyrdb',
    user='pkyr',
    password='!pkyr123!',
    host='147.102.40.25',
    options ='-c search_path=pkrydb.exercise1')

cursor = conn.cursor()
cursor.execute('''CREATE TYPE exercise1.person_category
	           AS ENUM('student', 'teacher');''')

#Create tables

cursor.execute('''CREATE TABLE exercise1.person (
                idperson INTEGER NOT NULL PRIMARY KEY,
                name VARCHAR(50) DEFAULT NULL,
                givenname VARCHAR(50) DEFAULT NULL,
                register_num INTEGER DEFAULT NULL,
                department VARCHAR(50) DEFAULT NULL,
                university VARCHAR(50) DEFAULT NULL,
                ak_year INTEGER DEFAULT 2020,
                category exercise1.person_category DEFAULT 'student',
                sdb Boolean,
                comp_methods Boolean);''')


conn.commit()
conn.rollback()

cursor.execute(''' create table exercise1.course (
                idcourse INTEGER NOT NULL PRIMARY KEY,
                code_name VARCHAR(15),
                name varchar(80),
                semester INTEGER NOT NULL,
                roomid INTEGER NOT NULL);''')
conn.commit()

cursor.execute(''' create table exercise1.personcourse (
                personid INTEGER NOT NULL,
                courseid INTEGER NOT NULL,
                PRIMARY KEY (personid, courseid));''')
conn.commit()

cursor.execute(''' alter table exercise1.personcourse
                ADD CONSTRAINT FK_personcourse_person
                    FOREIGN KEY (personid)
                    REFERENCES exercise1.person (idperson)
                    ON DELETE CASCADE;''')
conn.commit()

cursor.execute(''' alter table exercise1.personcourse
                ADD CONSTRAINT FK_personcourse_course
                    FOREIGN KEY (courseid)
                    REFERENCES exercise1.course (idcourse)
                    ON DELETE CASCADE;''')
conn.commit()

cursor.execute('''create type exercise1.room_use AS ENUM ('office', 'class');''')
conn.commit()

cursor.execute('''create table exercise1.room (
                idroom INTEGER NOT NULL PRIMARY KEY,
                label VARCHAR(50) DEFAULT NULL,
                use exercise1.room_use,
                seats INTEGER);''')
conn.commit()


cursor.execute('''create table exercise1.roomuni (
                roomid INTEGER NOT NULL PRIMARY KEY,
                uni VARCHAR(50) DEFAULT NULL);''')
conn.commit()

cursor.execute('''create table exercise1.students (
                "Α/Α"	INTEGER PRIMARY KEY,
                ΕΠΩΝΥΜΟ VARCHAR(50) DEFAULT NULL,
                ΟΝΟΜΑ VARCHAR(50) DEFAULT NULL,
                ΠΑΤΡΩΝΥΜΟ VARCHAR(50) DEFAULT NULL,
                "ΤΜΗΜΑ ΠΡΟΕΛΕΥΣΗΣ" VARCHAR(50) DEFAULT NULL,
                "ΙΔΡΥΜΑ ΠΡΟΕΛΕΥΣΗΣ" VARCHAR(50) DEFAULT NULL);''')

conn.commit()


#Insert Our Data

with open('/home/petros/MSC/GeoDB/2021/1/person-2020.csv', 'r', encoding='iso-8859-7') as f:
    # Notice that we don't need the `csv` module.
    next(f) # Skip the header row.
    cursor.copy_from(f, 'exercise1.person', sep=';',null='')

conn.commit()


with open('/home/petros/MSC/GeoDB/2021/1/example2-course-2020.csv', 'r', encoding='iso-8859-7') as f:
    # Notice that we don't need the `csv` module.
    next(f) # Skip the header row.
    cursor.copy_from(f, 'exercise1.course', sep=';',null='')

conn.commit()

with open('/home/petros/MSC/GeoDB/2021/1/room-2020.csv', 'r', encoding='iso-8859-7') as f:
    # Notice that we don't need the `csv` module.
    next(f) # Skip the header row.
    cursor.copy_from(f, 'exercise1.room', sep=';',null='')

conn.commit()

with open('/home/petros/MSC/GeoDB/2021/1/roomuniversity-2021.csv', 'r', encoding='iso-8859-7') as f:
    # Notice that we don't need the `csv` module.
    next(f) # Skip the header row.
    cursor.copy_from(f, 'exercise1.roomuni', sep=';',null='')

conn.commit()

with open('/home/petros/MSC/GeoDB/2021/1/students.csv', 'r', encoding='iso-8859-7') as f:
    # Notice that we don't need the `csv` module.
    next(f) # Skip the header row.
    cursor.copy_from(f, 'exercise1.students', sep=';',null='')

conn.rollback()
conn.commit()



####

cursor.execute("SELECT * FROM exercise1.students LIMIT 10")
print(cursor.fetchall())
