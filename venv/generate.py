from sql import *
from google.cloud import spanner
from sql.aggregate import *
from sql.conditionals import *
import sqlite3
import pandas as pd
import re

spanner_client = spanner.Client()
instance_id = input("Enter your spanner instance id: ")
instance = spanner_client.instance(instance_id)
database_id = input("Enter your spanner database id: ")

dd = []
db_file = input("Enter Database File: ")

conn=sqlite3.connect(db_file)
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



def get_values(name):
    c.execute("SELECT * FROM " + name[0] + " LIMIT 0")
    all_cols = []
    for i in list(c.description):
        all_cols.append(i[0])
    print(all_cols)
    id = all_cols[0]
    cstr = "SELECT COUNT(*) FROM " + name[0] + ";"
    n = c.execute(cstr).fetchone()[0]
    rows = []
    for i in range(1, n):
        nstr = "SELECT * FROM " + name[0] + " ORDER BY " + id + " LIMIT " + str(i) + "-1,1"
        vals = list(c.execute(nstr))
        row = ()
        for val in vals:
            if(isinstance(val,str)):
                row += unicode(val)
            else:
                row += val
        rows.append(row)
    return rows


def generate_spanner_table(tb,tb_name):
    c.execute("select sql from sqlite_master where sql not NULL")
    fetch = c.fetchall()
    schema = """CREATE TABLE """ + tb_name + """("""
    cn = 0
    id = None
    fors = []
    ref = None
    for ob,row in zip(tb,fetch):
        print(row[0])
        res = re.sub("`\s*,\s*`",",",row[0])
        #print(ob)
        pattern = "FOREIGN\s+KEY\s*\(*(.*?)*\)"

        matches = re.search(pattern,res)
        if matches:
            matches = str(matches.group())
            match2 = re.search("\[(.*?)\]",matches)
            for m in match2.groups():
                fors.append(m)
        oblist = list(ob)
        if "Id" in oblist[1]:
            id = oblist[1]
        if cn == len(tb) - 1:
            schema += oblist[1] + "     " + get_statement_datatype(oblist[2])
        else:
            schema += oblist[1] + "     " + get_statement_datatype(oblist[2]) +","
        cn+=1
    schema+= """)"""
    if id:
        if len(fors) < 0:
            schema += """ PRIMARY KEY (""" + id + """)"""

    return schema


def insert_spanner_data(instance_id,database_id,tables):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    with database.batch() as batch:
       for tab in tables:
           #Get all col names, and all vals from all rows
           c.execute("SELECT * FROM " + name[0] + " LIMIT 0")
           all_cols = []
           for i in list(c.description):
               all_cols.append(i[0])
           tupcols = tuple(all_cols)
           batch.insert(
                table=tab,
                columns = tupcols,
                values = get_values(tab)
            )


names = list(c.execute("SELECT name FROM sqlite_master WHERE type='table';"))

c.execute("select sql from sqlite_master where sql not NULL")
#for row in c.fetchall():
       # print(row)

for name in names:
    table_list = list(c.execute("PRAGMA table_info(" + name[0] + ")").fetchall())
    schem = generate_spanner_table(table_list,name[0])
    dd.append(schem)



"""database = instance.database(database_id, ddl_statements=dd)
operation = database.create()
insert_spanner_data(instance_id,database_id,names)"""



"""for name in names:
    print(get_values(name))
    print("\n")
    print("\n")"""

for st in dd:
    print(st)
    print("\n")

