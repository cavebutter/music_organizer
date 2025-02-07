import configparser
import os

# Initialize the config parser
config = configparser.ConfigParser()

# Read the config file
config.read('config.ini')

# Extract the required configuration items
WOODSTOCK_servername = config['WOODSTOCK']['servername']
musiclibrary = config['WOODSTOCK']['musiclibrary']
plex_username = config['WOODSTOCK']['username']
plex_password = config['WOODSTOCK']['password']

# Make them available to all modules in the plex package
__all__ = [
    'WOODSTOCK_servername',
    'musiclibrary',
    'plex_username',
    'plex_password'
]