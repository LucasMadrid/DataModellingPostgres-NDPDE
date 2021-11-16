import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
     Description: This function is responsible to read song data from 'data/song_data', load it into pandas dataframe called 'df'
     and extract the song and artist data to insert into song table and artist table.
     
     Argument:
        cur: the cursor object.
        filepath: path of the folder where is the song data.
        
     Returns:
         None
    
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
     Description: This function is responsible to read data from filepath (log data directory) and load it into a dataframe called 'df' as well as 
     apply filtering 'NextSong' and apply transformation to load time data into time table, user data into user table, and insert the necessary data
     for the fact table 'songplay'
     
     Arguments:
        cur: the cursor object.
        filepath: path of the directory where is the log data.
        
     Returns:
         None
    
    """
    
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data =(t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'weewk', 'month', 'year', 'weekday')    
    
    zipped_data = zip(column_labels, time_data)
    time_data_dict = dict(zipped_data)
    
    time_df = pd.DataFrame(time_data_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, row)

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        
        songplay_data = (pd.to_datetime(row.ts,unit='ms'), row.userId, row.level, \
                         songid, artistid, row.sessionId, str(row.location),str(row.userAgent))
        cur.execute(songplay_table_insert, songplay_data)
 

def process_data(cur, conn, filepath, func):
    """
    Description: This function is responsible for listing the files in a directory,
    and then executing the loading process for each file corresponding to the function
    that performs the transformation and load it to the database.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path directory.
        func: function that transforms the data and inserts it into the database.

    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()