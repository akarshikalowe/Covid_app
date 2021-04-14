# Covid contact tracing app

## Data Source

RDS Postgres connection
Table names
```attendance
   positives
 ``` 
   
## Business Problem
#### 1. Retrieve events (location and dates) that have had an attendee who got a positive result. (possibly some days after the event)
#### 2. Given a person id, retrieve the possible events at which that person might have contracted covid. 

Created transformations on python to retrieve events location and dates that have been affected by covid positive attendee. This is important in order to alert all the people who were available at that location during that time. Since we do not know for how long that person was at that location so I have assumed to consider dates of the location and use the dates of that location to alert all the users that were present at that location on that date. The result are stored in csv files in order to visualize


#### 3. Retrieve a list of people who should be warned that they were in contact with a person who tested positive.
Took a list of all the users who might have get in contact with the covid patient. People who might have come in contact with the covid patient in the past 15 days should be sent an alert.

## Python libraries installation
```pip install psycopg2-binary
```

## Credentials
Set environmental variables for username and passdwords

## Visualization

Can be found on Tableau Public under link

https://public.tableau.com/profile/akarshika.lowe#!/vizhome/CovidApp/Dashboard1?publish=yes

