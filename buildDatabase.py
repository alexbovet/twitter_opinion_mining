__author__ = "Alexandre Bovet"

import os
import sys
import time
from TwSqliteDB import updateSqliteTables, createIndexes
import sqlite3


from baseModule import baseModule

class buildDatabse(baseModule):
    """ Creates or updates the SQLite database with the tweet informations.
        
        buildDatabse must be initialized with a dictionary containing keys 
        `tweet_archive_dirs` and `sqlite_db_filename`.
        
        Read the tweets from the .taj files located in the directories 
        listed in `tweet_archive_dirs` and add them to the database 
        `sqlite_db_filename`. If the database already exists, it 
        will be updated with new tweets.
    """
    
    def run(self):
        
        #==============================================================================
        # PARAMETERS
        #==============================================================================
        # list of directories where the tweets JSON files for this 
        # database are stored
        tweet_archive_dirs = self.job['tweet_archive_dirs']
        
        # filename of the existing sqlite file or the one to create 
        sqlite_db_filename = self.job['sqlite_db_filename']

        #==============================================================================
        # OPTIONAL PARAMETERS
        #==============================================================================
        # drop indexes before updating
        DROP_ALL_INDEXES = self.job.get('DROP_ALL_INDEXES', True)
        # create indexes after updating
        CREATE_INDEXES = self.job.get('CREATE_INDEXES', True)
        
        
        
        # if already existing db
        if os.path.exists(sqlite_db_filename):
            #file and query dict will be updated from existing db
            CREATE_NEW_FILE_AND_QUERY_DICT = False
        
            # only update db with new files
            REMOVE_EXISTING_FILES = True
        else:
            CREATE_NEW_FILE_AND_QUERY_DICT = True
            REMOVE_EXISTING_FILES = False
                
        # find all tweet archive JSON files (taj)
        files = []
        for folder in tweet_archive_dirs:
            files.extend([os.path.join(folder,taj_file ) for taj_file in os.listdir(folder) if taj_file[-4:] == '.taj'])

                                          
        # create new dicts or copy them from the database
        # necessary to keep a unique id-item relation for queries, filenames and sources
        if CREATE_NEW_FILE_AND_QUERY_DICT:
            queries_dict = dict()
            filenames_dict = dict()
            sources_url_dict = dict()
            sources_content_dict = dict()
        
        else:    
        # get queries_dicts from database
            with sqlite3.connect(sqlite_db_filename,
                                 detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
                c = conn.cursor()
                
                c.execute("SELECT query, id FROM query")
                
                queries_dict = dict(qt for qt in c.fetchall())
                
                c.execute("SELECT filename, id FROM filename")
                
                filenames_dict = dict(qt for qt in c.fetchall())
                
                c.execute("SELECT source_url, id FROM source_url")    
                
                sources_url_dict = dict(qt for qt in c.fetchall())
                
                c.execute("SELECT source_content, id FROM source_content")    
                
                sources_content_dict = dict(qt for qt in c.fetchall())
                
        #remove files already in the database
        if REMOVE_EXISTING_FILES:
            existing_files = [file for file in files if os.path.basename(file) in filenames_dict.keys()]

            #do not remove the most recent file in the database as it might have been updated in the meantime
            files_mtimes = sorted(zip(existing_files, map(os.path.getmtime, existing_files)),
                                 key=lambda x: x[1])
            files_to_removes = [f for f, mtime in files_mtimes]
            files_to_removes = files_to_removes[:-1]
                              
            files = [file for file in files if file not in files_to_removes]         

                  
     
        # start by dropping all indexes for faster inserts             
        if DROP_ALL_INDEXES:
            with sqlite3.connect(sqlite_db_filename,
                                 detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
                c = conn.cursor()
                c.execute("SELECT name FROM sqlite_master WHERE type == 'index'")
                res = c.fetchall()
                for idx, in res:
                    print("Dropping index " + idx)
                    try:
                        c.execute("DROP INDEX {indx}".format(indx=idx))
                    except sqlite3.OperationalError as err:
                        print(err)
                
        # start building (updating database)        
        try:
            t0 = time.time()

            with sqlite3.connect(sqlite_db_filename, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            
                
                c = conn.cursor()
                
                
                c.execute('PRAGMA synchronous = NORMAL')
                # allows concurrent read
                c.execute('PRAGMA journal_mode = WAL')
                
                c.fetchall()
                
                
                
                for i, file in enumerate(files):
                    print(str(i) + ' over ' + str(len(files)))
                    
                    t2 = time.time()
                    
                    updateSqliteTables(conn, file,
                                       filenames_dict=filenames_dict,
                                       queries_dict=queries_dict,
                                       sources_url_dict=sources_url_dict,
                                       sources_content_dict=sources_content_dict,
                                       update_tweet_table=True,
                                       update_retweeted_status_table=True,
                                       update_user_table=True,
                                       update_hashtag_table=True,
                                       update_tweet_hashtag_user_table=True,
                                       update_mention_table=True,
                                       update_retweet_table=True,
                                       update_reply_table=True,
                                       update_quote_table=True,
                                       update_keyword_table=False,
                                       update_tweet_to_query_table=True)
                    
                
                    print('Transaction time ' + "{:.6}".format(time.time()-t2) + 's')
                    
                    print('Total time ' + "{:.6}".format(time.time()-t0) + 's')
                    
                    print('sqlite_file : ' + sqlite_db_filename)
                
                    conn.commit()
        
        except sqlite3.OperationalError as err:
            print(err)
            sys.exit(err)
        
        if CREATE_INDEXES:    
            with sqlite3.connect(sqlite_db_filename, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
                print('Creating indexes')
                t2 = time.time()
                createIndexes(conn)
                conn.commit()
                
                print('time ' + "{:.6}".format(time.time()-t2) + 's')
                
        # counting total number of tweets
        with sqlite3.connect(sqlite_db_filename, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            c = conn.cursor()
            
            c.execute("SELECT COUNT(*) FROM tweet")
            
            print("\nNumber of tweets: " + str(c.fetchall()))
                
            
        
    

