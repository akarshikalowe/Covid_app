#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 14 11:14:05 2021

@author: akarshika
"""
import psycopg2
import pandas as pd
import numpy as np

# data-interview-db.ce7oyzeskgrt.eu-west-1.rds.amazonaws.com:5432
connection = psycopg2.connect(
    database = "postgres",
    user = "u_0493617",
    password = "p_0397412",
    host="data-interview-db.ce7oyzeskgrt.eu-west-1.rds.amazonaws.com",
    port='5432'
)
cursor = connection.cursor()

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

sql = """
SELECT *
FROM u_0493617.infected_user_result 
"""
df_table_postgre = pd.read_sql(sql, con=connection)
print(df_table_postgre)

