from . import DB_PATH, DB_USER, DB_PASSWORD, DB_DATABASE, TEST_DB, DB_PORT
from .database import Database
import csv
import os
from loguru import logger
import datetime

# database = Database(DB_PATH, DB_USER, DB_PASSWORD, DB_DATABASE)
database = Database(DB_PATH, DB_USER, DB_PASSWORD, TEST_DB)


def insert_tracks(database: Database, csv_file):
    database.connect()
    query = """
    INSERT INTO track_data (title, artist, album, genre, added_date, filepath, location, woodstock_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            values = (row['title'], row['artist'], row['album'], row['genre'],
                      row['added_date'], row['filepath'], row['location'],
                      row['woodstock_id'])
            database.execute_query(query, values)
            logger.info(f"Inserted track record for {row['woodstock_id']}")




def get_id_location(database: Database, cutoff=None):
    """
    Query the database for the id and location of each track. Replace the beginning of the location
    Args:
        database: Database object
        cutoff: String representing the date to use as a cutoff for the query in 'mmddyyyy' format

    Returns:
        list: List of tuples containing id, woodstock_id, and updated location
    """
    database.connect()
    query_wo_cutoff = "SELECT id, woodstock_id, location FROM track_data"
    query_w_cutoff = "SELECT id, woodstock_id, location FROM track_data WHERE added_date > %s"

    if cutoff is None:
        results = database.execute_select_query(query_wo_cutoff)
        logger.info("Queried db without cutoff")
    else:
        try:
            # Convert cutoff from 'mmddyyyy' to 'yyyy-mm-dd'
            cutoff_date = datetime.datetime.strptime(cutoff, '%m%d%Y').strftime('%Y-%m-%d')
            results = database.execute_select_query(query_w_cutoff, (cutoff_date,))
            logger.info("Queried db with cutoff")
        except ValueError:
            logger.error("Invalid date format. Please use 'mmddyyyy'")
            results = []
    return results


def export_results(results: list, file_path: str = 'output/id_location.csv'):
    """
    Export the results of a query to a CSV file. 'results' is a list of tuples.
    :param results: List of tuples containing the data to be written to CSV
    :param file_path: Path to the CSV file
    :return: None
    """
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'woodstock_id', 'location'])
        writer.writerows(results)
    logger.info(f"id_location results exported to {file_path}")
    return None


def populate_artists_table(database: Database):
    """

    :param database:
    :return:
    """
    database.connect()
    query = """
    SELECT DISTINCT artist FROM track_data
    """
    artists = database.execute_select_query(query)
    for artist in artists:
        database.execute_query("INSERT INTO artists (artist) VALUES (%s)", (artist[0],))
        logger.info(f"Inserted {artist[0]} into artists table; {artists.index(artist) + 1} of {len(artists)}")
    logger.debug("Populated artists table")


def add_artist_id_column(database: Database):
    """
    Replaces the artist column in the track_data table with the artist id from the artists table.
    Should only be called once at the beginning of the program.
    Returns:

    """
    database.connect()
    query = """
    ALTER TABLE track_data
    ADD COLUMN artist_id INTEGER,
    ADD FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE
    """
    result = database.execute_query(query)
    logger.debug("Replaced artist column in track_data table")
    return result


def populate_artist_id_column(database: Database):
    """
    Populates the artist_id column in the track_data table with the artist id from the artists table.
    Should only be called once at the beginning of the program.
    Returns:

    """
    database.connect()
    query = """
    SELECT id, artist
    FROM artists
    """
    artists = database.execute_select_query(query) # fetchall()
    logger.debug("Queried DB for id and artist")
    update_query = "UPDATE track_data SET artist_id = %s WHERE artist = %s"

    for artist in artists:
        params = (artist[0], artist[1])
        database.execute_query(update_query, params)
        logger.info(f"Updated {artist[1]} in track_data table; {artists.index(artist) + 1} of {len(artists)}")
    logger.debug("Updated artist_id column in track_data table")


def get_last_update_date(database: Database):
    database.connect()
    query = "SELECT MAX(date) FROM history"
    result = database.execute_select_query(query)
    result = result[0][0]
    return result