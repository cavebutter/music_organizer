from . import DB_PATH, DB_USER, DB_PASSWORD, DB_DATABASE, DB_PORT, TEST_DB
from .database import Database
from analysis import bpm
from loguru import logger
import csv
import json
import re
from time import sleep
import analysis.lastfm as lastfm

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


def populate_genres_table_from_track_data(database: Database):
    logger.debug("Starting to populate genres table from track data.")
    database.connect()
    query = "SELECT genre FROM track_data"
    results = database.execute_select_query(query)
    genre_list = []

    for result in results:
        genre_str = result[0]
        if genre_str != '[]':
            try:
                genre_str = genre_str.strip("[]").replace("'", "")
                genres = [genre.strip() for genre in genre_str.split(",")]
                genre_list.extend(genres)
            except Exception as e:
                logger.error(f"Error processing genre string: {e} - genre_str: {genre_str}")

    genre_list = list(set(genre_list))
    logger.info(f"Extracted genres: {genre_list}")
    database.close()
    logger.debug("Finished populating genres table from track data.")
    return genre_list


def insert_genres_if_not_exists(database: Database, genre_list: list):
    logger.debug("Starting to insert genres if not exists.")
    database.connect()

    existing_genres_query = "SELECT genre FROM genres"
    existing_genres = database.execute_select_query(existing_genres_query)
    existing_genres_set = {genre[0] for genre in existing_genres}

    new_genres = [genre for genre in genre_list if genre not in existing_genres_set]

    for genre in new_genres:
        database.execute_query("INSERT INTO genres (genre) VALUES (%s)", (genre,))
        logger.info(f"Inserted new genre: {genre}")

    database.close()
    logger.debug("Finished inserting genres if not exists.")
    return new_genres


def populate_track_genre_table(database: Database):
    logger.debug("Starting to populate track genre table.")
    database.connect()
    query = "SELECT id, genre FROM track_data"
    results = database.execute_select_query(query)

    for result in results:
        track_id = result[0]
        genre_str = result[1]
        if genre_str != '[]':
            try:
                genre_str = genre_str.strip("[]").replace("'", "")
                genres = [genre.strip() for genre in genre_str.split(",")]
                for genre in genres:
                    genre_id_query = "SELECT id FROM genres WHERE genre = %s"
                    genre_id_result = database.execute_select_query(genre_id_query, (genre,))
                    if genre_id_result:
                        genre_id = genre_id_result[0][0]
                        database.execute_query("INSERT INTO track_genres (track_id, genre_id) VALUES (%s, %s)", (track_id, genre_id))
                        logger.info(f"Inserted track-genre pair: track_id={track_id}, genre_id={genre_id}")
            except Exception as e:
                logger.error(f"Error processing genre string: {e} - genre_str: {genre_str}")

    database.close()
    logger.debug("Finished populating track genre table.")
    return None


def update_track_genre_table(database: Database, cutoff: str = None):
    logger.debug("Starting to update track genre table.")
    database.connect()
    query_wo_cutoff = "SELECT id, genre FROM track_data"
    query_w_cutoff = "SELECT id, genre FROM track_data WHERE added_date > %s"

    if cutoff is None:
        results = database.execute_select_query(query_wo_cutoff)
    else:
        try:
            cutoff_date = re.sub(r'(\d{2})(\d{2})(\d{4})', r'\3-\1-\2', cutoff)
            results = database.execute_select_query(query_w_cutoff, (cutoff_date,))
        except Exception as e:
            logger.error(f"There was an error querying db with cutoff: {e}")
            results = []

    for result in results:
        track_id = result[0]
        genre_str = result[1]
        if genre_str != '[]':
            try:
                genre_str = genre_str.strip("[]").replace("'", "")
                genres = [genre.strip() for genre in genre_str.split(",")]
                for genre in genres:
                    genre_id_query = "SELECT id FROM genres WHERE genre = %s"
                    genre_id_result = database.execute_select_query(genre_id_query, (genre,))
                    if genre_id_result:
                        genre_id = genre_id_result[0][0]
                        database.execute_query("INSERT INTO track_genres (track_id, genre_id) VALUES (%s, %s)", (track_id, genre_id))
                        logger.info(f"Inserted track-genre pair: track_id={track_id}, genre_id={genre_id}")
            except Exception as e:
                logger.error(f"Error processing genre string: {e} - genre_str: {genre_str}")

    database.close()
    logger.debug("Finished updating track genre table.")
    return None


def get_artists_from_db(database: Database):
    """
    Get all artists from artists table in the database. Return a list of artist names.
    :param database:
    :return:
    """
    logger.debug("Starting to get artists from db.")
    database.connect()
    query = "SELECT artist FROM artists"
    results = database.execute_select_query(query)
    artist_list = [result[0] for result in results]
    database.close()
    logger.debug("Finished getting artists from db.")
    return artist_list


def check_mbid_and_insert(database: Database, lastfm_json: json, mbid_list: list):
    """
    Check if the MBID is in the database and insert it in artists.musicbrainz_id if it is not.
    :param database:
    :param lastfm_json:
    :param mbid_list:
    :return:
    """
    database.connect()
    mbid = lastfm.get_mbid(lastfm_json)
    if mbid not in mbid_list:
        artist = lastfm_json['artist']['name']
        database.execute_query("UPDATE artists SET musicbrainz_id = %s WHERE artist = %s", (mbid, artist))
        logger.info(f"Inserted MBID for {artist}: {mbid}")


def check_tags_and_insert(database: Database, lastfm_json: json, genre_list: list):
    """
    Check if the tags are in the database and insert them in genres if they are not.
    :param database:
    :param lastfm_json:
    :param genre_list:
    :return:
    """
    database.connect()
    tags = lastfm.get_tags(lastfm_json)
    for tag in tags:
        if tag not in genre_list:
            database.execute_query("INSERT INTO genres (genre) VALUES (%s)", (tag,))
            logger.info(f"Inserted new genre: {tag}")
    database.close()


def insert_last_fm_data(database: Database, artist_list: list):
    logger.debug("Starting to insert Last.fm data into db.")
    database.connect()
    mbid_list = lastfm.get_current_mbids_from_db(database)
    genre_list = lastfm.get_genres_from_db(database)
    processed_artists = set(artist_list)  # Track processed artists to avoid duplicates

    for artist in artist_list:
        artist_info = lastfm.get_artist_info(artist)
        if artist_info:
            check_mbid_and_insert(database, artist_info, mbid_list)
            check_tags_and_insert(database, artist_info, genre_list)
            similar_artists = lastfm.get_similar_artists(artist_info)
            for similar_artist in similar_artists:
                if similar_artist[0] not in processed_artists:
                    database.execute_query("INSERT INTO artists (artist) VALUES (%s)", (similar_artist[0],))
                    logger.info(f"Inserted similar artist: {similar_artist[0]}")
                    processed_artists.add(similar_artist[0])
            sleep(5)
    database.close()