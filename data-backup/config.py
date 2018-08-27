# -*- coding: utf-8 -*-
import datetime

class Config:
    # Salesforce Credentials and API setup
    USERNAME = ""
    PASSWORD = ""
    SECURITY_TOKEN = ""
    IS_SANDBOX = True
    API_VERSION = 43.0

    # Azure Credentials
    PUSH_TO_AZURE = True
    AZURE_CONNECTION_STRING = ""
    AZURE_CONTAINER_NAME = ""
    AZURE_FOLDER_NAME = "BAK_" + '{0:%Y%m%d}'.format(datetime.datetime.now())

    # Job Config
    CHUNK_SIZE = 100000
    SKIP_OBJECTS = []          # the list of objects to be skipped, not specify to fetch all the queryable objects
    BREAK_OBJECT = ''          # the object to break (from the whole object list), not specify to fetch all the queryable objects
    WHITE_LIST_OBJECT = []     # if not empty, only the object listed will be fetched.
