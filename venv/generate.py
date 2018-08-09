from sql import *
from google.cloud import spanner
from sql.aggregate import *
from sql.conditionals import *
import sqlite3
import pandas as pd


spanner_client = spanner.Client()
instance_id = 'my-instance-id'
instance = spanner_client.instance(instance_id)
database_id = 'my-database-id'
database = instance.database(database_id)

ddl_statements = []

conn = sqlite3.connect('chinook.db')
c = conn.cursor()


def get_statement_datatype(txt):
    if "CHARACTER" in txt or "VARCHAR" in txt or "CHARACTER VARYING" in txt:
        return "STRING"
    if "INTEGER" in txt or "SMALLINT" in txt or "BIGINT" in txt:
        return "INT64"
    if "FLOAT" in txt or "REAL" in txt or "DOUBLE PRECISION" in txt or "NUMERIC" in txt or "DECIMAL" in txt:
        return "FLOAT64"
    if "DATE" in txt:
        return "DATE"
    if "TIME" in txt or "TIMESTAMP" in txt:
        return "TIMESTAMP"
    if "BOOLEAN" in txt:
        return "BOOL"
    if "ARRAY" in txt or "MULTISET" in txt:
        return "ARRAY"
    return ""


def get_real_datatype(d):
    return 0


def get_values(tab):
    cstr = "SELECT COUNT(*) FROM " + tab + ";"
    n = c.execute(cstr)
    rows = []
    for i in range(1,n):
        nstr = "SELECT * FROM " + table + " ORDER BY ID LIMIT " + str(i) + "-1,1"
        row = ()
        row_vals = list(c.execute(nstr))
        for j in range(0,len(row_vals)):
            if(isinstance(row_vals(j),str)):
                row += unicode(row_vals[j])
            else:
                row += row_vals[j]
        rows.append(row)
    return row



def generate_spanner_table(tb,tb_name):
    schema = """CREATE TABLE """ + tb_name + """("""
    cn = 0
    id = ""
    for ob in tb:
        oblist = list(ob)
        if "Id" in oblist[1]: id = oblist[1]
        if cn == len(tb) - 1:
            schema += oblist[1] + "     " + get_statement_datatype(oblist[2])
        else:
            schema += oblist[1] + "     " + get_statement_datatype(oblist[2]) +","
        cn+=1
    schema+= """)"""
    if id != "": schema += """ PRIMARY KEY (""" + id + """)"""
    ddl_statements.append(schema)


def insert_spanner_data(instance_id,database_id,tables):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    with database.batch() as batch:
       for tab in tables:
           #todo figure out how to specify all columns
           #Create a datastructure that maps columns to values
           cquer = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='" + tab + "'"
           cols = list(c.execute(cquer))
           tupcols = tuple(cols)
           batch.insert(
                table=tab,
                columns = tupcols,
                values = get_values(tab,cols)
            )


names = list(c.execute("SELECT name FROM sqlite_master WHERE type='table';"))

for name in names:
    table_list = list(c.execute("PRAGMA table_info(" + name[0] + ")").fetchall())
    generate_spanner_table(table_list,name[0])

for st in ddl_statements:
    print(st)
    print("\n")

