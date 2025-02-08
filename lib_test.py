import plex.plex_library as p
from loguru import logger
import sys


def configure_logging():
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")
    logger.add('logs/run.log', level="DEBUG")

if __name__ == '__main__':
    configure_logging()
    server = p.plex_connect()
    library = p.get_music_library(server, p.MUSIC_LIBRARY)
    tracks, lib_size = p.get_all_tracks_limit(library)
    track_list = p.listify_track_data(tracks, 'woodstock', '/Volumes/Franklin/Media/')
    p.export_track_data(track_list, 'output/test_track_data.csv', 'woodstock')