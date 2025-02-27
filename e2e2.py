import plex.plex_library as p
import db.db_functions as dbf
import db.db_update as dbu
from loguru import logger
import sys
import db.useful_queries as dbq
import analysis.lastfm as lfm
from time import sleep
import analysis.ffmpeg as f


def configure_logging():
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")
    logger.add('logs/e2e2.log', level="DEBUG")


if __name__ == '__main__':
    configure_logging()

    db = dbu.database
    db.connect()

    # drop tables
    db.drop_all_tables()

    # Create tables
    db.create_all_tables()

    server = p.plex_connect()  # Connect to TEST_SERVER
    library = p.get_music_library(server, p.TEST_LIBRARY) # Get TEST_LIBRARY
    tracks, lib_size = p.get_all_tracks_limit(library)
    track_list = p.listify_track_data(tracks,'/mnt/hdd/')
    p.export_track_data(track_list, 'output/e2e2_test_track_data.csv')



    # Populate test db
    dbf.insert_tracks(dbf.database, 'output/e2e2_test_track_data.csv')
    dbf.populate_artists_table(dbf.database)
    dbf.populate_artist_id_column(dbf.database)

    # db_update functions to populate the genres table and the track_genre table
    genre_list = dbu.populate_genres_table_from_track_data(db)
    dbu.insert_genres_if_not_exists(db, genre_list)
    dbu.populate_track_genre_table(db)

    # ffmpeg analysis
    # TOD I am thinking that I should work this from the csv rather than the db.  Read each row
    all_tracks = db.execute_select_query(dbq.ALL_TRACK_IDS_FILEPATHS_TITLES)
    for id, filepath, title in all_tracks:
        track_info = f.get_track_info(filepath)
        if track_info:
            mbid = f.ffmpeg_get_mbtid(track_info)
            # TODO INSERT QUERY
