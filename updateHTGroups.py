__author__ = "Alexandre Bovet"


import sqlite3
from TwSqliteDB import addHTSupportGroup
import time

from baseModule import baseModule

class updateHTGroups(baseModule):
    """ Mark the hashtags used as labels in the database.
     
        Must be initialized with a dictionary `job` containing keys
        `sqlite_db_filename` and `htgs_lists`.
        
        `updateHTGroups` takes the lists of hashtags `htgs_lists` and mark then in 
        the database `sqlite_db_filename`.

        *Optional parameters that can be added to `job`:*
 
        :column_name_ht_group: name of the column added to the database (Default 
                               is `'ht_class'`). Different names can be used to
                               test different `htgs_list`.
        
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
        
#        create_column = self.job.get('create_column_ht_group', True)
#        create_index = self.job.get('create_index_ht_group', True)
        
        # create column unless it already exists
        create_column = True
        create_index = True
        
        t0 = time.time()
        # labels of each class
        ht_group_names = [str(i) for i, _ in enumerate(ht_list_lists)]
                          
        # check if column already exists
        with sqlite3.connect(sqlite_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            c = conn.cursor()
            c.execute("PRAGMA table_info(hashtag_tweet_user)")
            cnames = c.fetchall()
            
        if column_name in [cname for _, cname, _, _, _ ,_ in cnames]:
            print('column ' + str(column_name) + ' already exists, column values will be replaced.')
            create_column = False
            # first drop index
            c.execute("DROP INDEX IF EXISTS {cname}_supp_index".format(cname=column_name))
            conn.commit()
            # set all column values to NULL
            with sqlite3.connect(sqlite_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
                c = conn.cursor()
                c.execute("""UPDATE hashtag_tweet_user
                              SET {cname} = NULL 
                              """.format(cname=column_name))
        
            
        #%%
        with sqlite3.connect(sqlite_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
        
        
            addHTSupportGroup(conn, ht_group_names, ht_list_lists,
                              create_column=create_column, create_index=create_index,
                              column_name=column_name)
            
            
        self.print_elapsed_time(t0)


