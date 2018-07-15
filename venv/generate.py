from sql import *
from google.cloud import spanner
from sql.aggregate import *
from sql.conditionals import *
import sqlite3
import pandas as pd


def generate_table_statements(tb,tb_name):
    schema = """CREATE TABLE """ + tb_name + """("""
    c = 0
    id = ""
    for ob in tb:
        oblist = list(ob)
        if "Id" in oblist[1]: id = oblist[1]
        if c == len(tb) - 1:
            schema += oblist[1] + "     " + oblist[2]
        else:
            schema += oblist[1] + "     " + oblist[2] +","
        c+=1
    schema+= """)"""
    if id != "": schema += """ PRIMARY KEY (""" + id + """)"""
    ddl_statements.append(schema)


def insert_data(instance_id,database_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    with database.batch() as batch:
       batch.insert(
            
       )






spanner_client = spanner.Client()
instance_id = 'my-instance-id'
instance = spanner_client.instance(instance_id)
database_id = 'my-database-id'
database = instance.database(database_id)

ddl_statements = []

conn = sqlite3.connect('chinook.db')
c = conn.cursor()
names = list(c.execute("SELECT name FROM sqlite_master WHERE type='table';"))

for name in names:
    table_list = list(c.execute("PRAGMA table_info(" + name[0] + ")").fetchall())
    generate_table_statements(table_list,name[0])

print(ddl_statements)