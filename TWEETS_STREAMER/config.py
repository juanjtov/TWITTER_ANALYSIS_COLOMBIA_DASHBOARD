import os

class Credentials:
    #Keys
    API_KEY = os.getenv('API_KEY')
    API_SECRET_KEY = os.getenv('API_SECRET_KEY')
    #tokens
    ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
    ACCESS_TOKEN_SECRET = os.getenv('ACCESS_SECRET_TOKEN')

class Settings:
    TRACK_WORDS = ['Colombia','colombia']
    
    
class GcloudSettings:
    #PROJECT AND TOPIC IDENTIFIERS
    PROJECT_ID = os.getenv('PROJECT')
    TOPIC_ID = os.getenv('TOPIC_FROM_TWEEPY')
