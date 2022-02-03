import pyodbc
import snowflake.connector as sf
import pandas as pd


#Manually adjust this, The table we want to test
Table='FACT_TABLE'

#Import the sql queries we want to test as a text string
path = (r'D:\Users\jhiu\Desktop\Queries')
query1=open(path+'\SSMS_'+Table+'.txt','r')
query2=open(path+'\SNOW_'+Table+'.txt','r')
Ssms_query=query1.read()
Snow_query=query2.read()

#Query SSMS for results and place it into a dataframe 
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=SERVER;"
                      "Database=DB;"
                      "Trusted_Connection=yes;")
cursor = cnxn.cursor()
Ssms = pd.read_sql_query(Ssms_query, cnxn)
Ssms = Ssms.fillna('')

#Query Snowflake for results and place it into a dataframe
conn = sf.connect(user="USER",
   password="PASSWORD",
   account="ACCOUNT")
cur = conn.cursor() 
cur.execute("USE ROLE SYSADMIN")
cur.execute("USE WAREHOUSE WH_COMPUTE")
output=cur.execute(Snow_query)
Snow = pd.DataFrame.from_records(iter(output), columns=[x[0] for x in output.description])
Snow = Snow.fillna('')

#Compare reported values SSMS output vs SNOWFLAKE
result=Ssms.compare(Snow, align_axis=1, keep_shape=False, keep_equal=False)

#Output to Excel
Writer = pd.ExcelWriter(Table+'.xlsx')
Snow.to_excel(Writer, sheet_name = 'Snow')
Ssms.to_excel(Writer, sheet_name = 'Ssms')
result.to_excel(Writer, sheet_name = 'Differences')
Writer.save()