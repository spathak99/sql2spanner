from sql import *
from google.cloud import spanner, storage
from sql.aggregate import *
from sql.conditionals import *
import sqlite3
import pandas as pd
import json


def store(data):
    #TODO store json in google cloud storage

def make_json(res):
    dump = {}
    for r in res:
        #TODO append to json for data backup
    store(dump)

def backup_data(tables):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    counter = 0
    rows = []
    for tab in tables:
        counter = counter+1
        with database.snapshot() as snapshot:
            keyset = spanner.KeySet(all_=True)
            # todo figure out how to specify all columns
            results = snapshot.read(
                table=tab,
                columns=(),
                keyset=keyset
            )
            rows.append(results)
        if counter == 15:
            counter = 0
            make_json(results);



