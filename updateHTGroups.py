__author__ = "Alexandre Bovet"


import sqlite3
from TwSqliteDB import addHTSupportGroup

from baseModule import baseModule

class updateHTGroups(baseModule):
    """ add a column to the sqlite db hashtag_tweet_user table to indicate
        hashtags selected by label propagation.
        
    """    
    
    def run(self):

        #==============================================================================
        # PARAMETERS
        #==============================================================================
        sqlite_file = self.job['sqlite_db_filename']
       
        ht_list_lists = self.job['htgs_lists']
        
        #==============================================================================
        # OPTIONAL PARAMETERS
        #==============================================================================        
        column_name = self.job.get('column_name_ht_group', 'ht_class')
        
        create_column = self.job.get('create_column_ht_group', True)
        create_index = self.job.get('create_index_ht_group', True)        
        
        # labels of each class
        ht_group_names = [str(i) for i, _ in enumerate(ht_list_lists)]
                          
        # check is column already exists
        with sqlite3.connect(sqlite_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            c = conn.cursor()
            c.execute("PRAGMA table_info(hashtag_tweet_user)")
            cnames = c.fetchall()
            
        if column_name in [cname for _, cname, _, _, _ ,_ in cnames]:
            create_column = False
            # set all column values to NULL
            with sqlite3.connect(sqlite_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
                c = conn.cursor()
                c.execute("""UPDATE hashtag_tweet_user
                              SET {cname} = NULL 
                              WHERE {cname} IS NOT NULL""".format(cname=column_name))
        
            
        #%%
        with sqlite3.connect(sqlite_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
        
        
            addHTSupportGroup(conn, ht_group_names, ht_list_lists,
                              create_column=create_column, create_index=create_index,
                              column_name=column_name)


