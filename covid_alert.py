#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 14 01:51:42 2021

@author: akarshika
"""

import psycopg2
import pandas as pd
import numpy as np

# data-interview-db.ce7oyzeskgrt.eu-west-1.rds.amazonaws.com:5432
connection = psycopg2.connect(
    database="postgres",
    user="u_0493617",
    password="p_0397412",
    host="data-interview-db.ce7oyzeskgrt.eu-west-1.rds.amazonaws.com",
    port='5432'
)
cursor = connection.cursor()

sql = """
SELECT *
FROM attendance
"""

df_attendance = pd.read_sql(sql, con=connection)

sql2 = """
SELECT * from
positives
"""
df_positives = pd.read_sql(sql2, con=connection)

df_join = pd.merge(df_attendance, df_positives, on='user', how='inner')

# taking into account that they might infect users whom they meet within 15 days prior to the date of positive report of covid
df_join['date1'] = df_join['positive_date'] - pd.to_timedelta(15, unit='d')
df = df_join

# since we don't know the for how long the person was present at that location so we use date and not timestamp for presence_date

df['alert'] = np.where((df['presence_date'] >= df['date1']) & (df['presence_date'] <= df['positive_date']), 'Danger', 'safe')

df['presence_normalised_date'] = df['presence_date'].dt.date

df_infected_users = df.loc[df['alert'] == 'Danger']
print(df_infected_users)

# CSV of infected users with their Id, location and dates of events
df_infected_users.to_csv("infected_users.csv")

# since we don't know the for how long the person was present at that location so we use date and not timestamp for presence_date
df_attendance['presence_normalised_date'] = df_attendance['presence_date'].dt.date

# joining based on infected ppl location and presence date in order to find the users who might be infected
df_join_2 = pd.merge(df_attendance, df_infected_users, on=('location', 'presence_normalised_date'), how='inner')

df_maybe_infected = df_join_2['user_x'].drop_duplicates()
print(df_maybe_infected)
df_maybe_infected.to_csv("send_alert_to_users.csv")
