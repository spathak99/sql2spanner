from sql import *
from google.cloud import spanner, storage
from sql.aggregate import *
from sql.conditionals import *
import sqlite3
import pandas as pd
import json



def backup_data(tables):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    for tab in tables:
        with database.snapshot() as snapshot:
            keyset = spanner.KeySet(all_=True)
            results = snapshot.read(
                table=tab,
                columns=(),
                keyset=keyset
            )