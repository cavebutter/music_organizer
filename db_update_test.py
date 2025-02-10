# Description: This script is used to test the database update functions.
# It duplicates the functionality of db_test.py, but with the addition update functions.
# We run this after we are satisfied with the results of db_test.py.

import subprocess
import db.db_functions as dbf
import db.db_update as dbu
from loguru import logger
import sys


# Configure logging
logger.remove()
logger.add(sys.stdout, level="DEBUG")
logger.add("logs/db_update_test.log", rotation="10 MB", level="DEBUG")


# Run setup_test_env to create fresh test db
subprocess.run(['python3', 'db/setup_test_env.py'])

database = dbf.database
database.connect()

# Create tables
database.create_all_tables()


# Populate test db
dbf.insert_tracks(dbf.database, 'output/test_track_data2.csv')
# after this export, we can run bpm analysis on a different machine
dbf.populate_artists_table(dbf.database)
dbf.populate_artist_id_column(dbf.database)

# db_update functions to populate the genres table and the track_genre table
genre_list = dbu.populate_genres_table_from_track_data(database)
dbu.insert_genres_if_not_exists(database, genre_list)
dbu.populate_track_genre_table(database)