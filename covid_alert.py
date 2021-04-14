# Covid app
import psycopg2
import pandas as pd
import numpy as np
import os

# Connecting to the DB source
connection = psycopg2.connect(
    database="postgres",
    user = os.environ.get('username'),
    password = os.environ.get('password'),
    host="data-interview-db.ce7oyzeskgrt.eu-west-1.rds.amazonaws.com",
    port='5432'
)
cursor = connection.cursor()
table_name= "u_0493617.infected_user_result"
sql_create = "Create table " +table_name+" (user_name int, location int, presence_date timestamp, report_date timestamp, positive_date timestamp, date1 timestamp);"
cursor.execute(sql_create)
connection.commit()
sql = """
SELECT *
FROM attendance
"""
df_attendance = pd.read_sql(sql, con=connection)
print(df_attendance)

df_attendance.to_csv('attendance.csv')
sql2 = """
SELECT * from
positives
"""
df_positives = pd.read_sql(sql2, con=connection)

df_join = pd.merge(df_attendance, df_positives, on='user', how='inner')
# Retrieve events (location and dates) that have had an attendee who got a positive result. 
# taking into account that they might infect users whom they meet within 15 days prior to the date of positive report of covid
df_join['date1'] = df_join['positive_date'] - pd.to_timedelta(15, unit='d')
df = df_join

# since we don't know the for how long the person was present at that location so we use date and not timestamp for presence_date

df['alert'] = np.where((df['presence_date'] >= df['date1']) & (df['presence_date'] <= df['positive_date']), 'Danger', 'safe')

df['presence_normalised_date'] = df['presence_date'].dt.date

df_new = df.loc[df['alert'] == 'Danger']
print(df_new)
df_infected_users=df_new.drop(columns=['index_y'])
# CSV of infected users with their Id, location and dates of events
df_infected_users.to_csv("infected_users.csv")
df_infected_users.rename(columns={"user": "users_name"})
df_csv = pd.read_csv("infected_users.csv")

# Inserting data into the Postgresql Table 
sql3 = """
with temp as (SELECT c.user as users,*
    FROM attendance w, positives c
    WHERE w.user = c.user),

temp2 as
(select users, location,	presence_date,	report_date,	positive_date,
((positive_date) - INTERVAL '15 DAYS') AS date1
 from temp),

temp3 as(
select  users as user_name, location,	presence_date,	report_date, positive_date, date1 from temp2 where presence_date >= date1  and presence_date <= positive_date
)

insert into u_0493617.infected_user_result (user_name, location, presence_date,	report_date, positive_date, date1)
select user_name, location,	presence_date,	report_date, positive_date, date1 from temp3;

"""
cursor.execute(sql3)
connection.commit()

# since we don't know the for how long the person was present at that location so we use date and not timestamp for presence_date
df_attendance['presence_normalised_date'] = df_attendance['presence_date'].dt.date

df_infected_comb = df_infected_users[['presence_normalised_date', 'location']].copy()
print(df_infected_comb)


# joining based on infected ppl location and presence date in order to find the users who might be infected
df_join_2 = pd.merge(df_attendance, df_infected_comb, on=('location', 'presence_normalised_date'), how='inner')
print(df_join_2)
df_maybe_infected = df_join_2['user'].drop_duplicates()
print(df_maybe_infected)
df_maybe_infected.to_csv("send_alert_to_users.csv")
