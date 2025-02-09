from . import DB_PATH, DB_USER, DB_PASSWORD, DB_DATABASE, DB_PORT, TEST_DB
from .database import Database
from analysis import bpm
from loguru import logger
import csv
import json
import re

# TODO change databsae for production
database = Database(DB_PATH, DB_USER, DB_PASSWORD, TEST_DB)


def process_bpm(database: Database, track_list: csv):
    """
    Process the BPM for each track in the track list and update the 'bpm' field in the database.

    Parameters:
    database (Database): The database connection object.
    track_list (csv): The list of tracks to process BPM for.

    Returns:
    None
    """
    database.connect()
    with open(track_list, 'r') as f:
        reader = csv.DictReader(f)
        lib_size = sum(1 for _ in reader)
        logger.debug(f"Library size: {lib_size}")


    with open(track_list, 'r') as f:
        reader = csv.DictReader(f)
        i = 1
        for row in reader:
            file_location = '/mnt/triton/' + row['location'].replace('Music/', 'music/')
            # Above only applies to Neptune server. Change as needed.
            track_bpm = bpm.get_bpm(file_location)
            database.execute_query("UPDATE track_data SET bpm = %s WHERE id = %s", (track_bpm, row['id']))
            logger.info(f"Processed BPM for {row['woodstock_id']}; {i} of {lib_size}")
            i += 1
    database.close()


import re
from loguru import logger

def populate_genres_table_from_track_data(database: Database):
    """
    Ensure that the genres table is populated with all genres from the track_data table.
    Select all genre lists from the track_data table, unpack them, and insert them into the genres table.

    Parameters:
    database (Database): The database connection object.

    Returns:
    list: A list of genres.
    """
    database.connect()
    query = "SELECT genre FROM track_data"
    results = database.execute_select_query(query)
    genre_list = []

    for result in results:
        genre_str = result[0]
        if genre_str != '[]':
            try:
                # Remove the enclosing brackets and single quotes
                genre_str = genre_str.strip("[]").replace("'", "")
                # Split the string by commas to get individual genres
                genres = [genre.strip() for genre in genre_str.split(",")]
                genre_list.extend(genres)
            except Exception as e:
                logger.error(f"Error processing genre string: {e} - genre_str: {genre_str}")

    genre_list = list(set(genre_list))  # Remove duplicates

    database.close()
    return genre_list


def insert_genres_if_not_exists(database: Database, genre_list: list):
    """
    Insert each genre into the genres table if it does not already exist.

    Parameters:
    database (Database): The database connection object.
    genre_list (list): The list of genres to insert.

    Returns:
    None
    """
    database.connect()

    # Get existing genres from the database
    existing_genres_query = "SELECT genre FROM genres"
    existing_genres = database.execute_select_query(existing_genres_query)
    existing_genres_set = {genre[0] for genre in existing_genres}

    # Filter out genres that already exist
    new_genres = [genre for genre in genre_list if genre not in existing_genres_set]

    # Insert new genres into the database
    for genre in new_genres:
        database.execute_query("INSERT INTO genres (genre) VALUES (%s)", (genre,))

    database.close()
    return new_genres