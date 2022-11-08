import psycopg2
  
conn = psycopg2.connect(database="hadoop_demo",
                        user='postgres', password='1', 
                        host='127.0.0.1', port='5432'
)
  
conn.autocommit = True
cursor = conn.cursor()

drop_sql='''
 DROP TABLE IF EXISTS tbl_Country 
 ''' 
cursor.execute(drop_sql)
sql = '''


CREATE TABLE tbl_Country( \
    lng_Country_Id_Nmbr int PRIMARY KEY NOT NULL,\
str_Country_Ident varchar(7),\
    str_Country_Desc varchar(50),\
str_Deleted_Flag char(1),\
dtm_Creation_Date TIMESTAMP, \
lng_Creation_User_Id_Nmbr int, \
dtm_Last_Mod_Date TIMESTAMP, \
    lng_Last_Mod_User_Id_Nmbr int , \
        tsp_TimeStamp varchar(100) );\

'''
  
  
cursor.execute(sql)
  
sql2 = '''COPY tbl_Country 
FROM 'D:/Hadoop_Demo/data/tbl_Country.csv' 
DELIMITER ','
CSV HEADER;'''
  
cursor.execute(sql2)
  
sql3 = '''select * from tbl_Country;'''
cursor.execute(sql3)
for i in cursor.fetchall():
    print(i)
  
conn.commit()
conn.close()