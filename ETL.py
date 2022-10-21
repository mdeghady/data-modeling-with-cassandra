from tqdm import tqdm
import cassandra
import pandas as pd
import os

from CQL_quries import *
from CreateTables import ConnectToCassandra

def concatenate_csv(root_dir):
    """

    :param root_dir: Root directory for the data files
    :returns full_df: One big csv file has all of the rows of the csv files in the root directory
    Concatenate every file in root directory into one file
    """

    # Get the paths of every csv file in the event_data directory
    file_paths = []

    for root, dirs, files in os.walk(root_dir, topdown=True):
        for file in files:
            file_paths.append(os.path.join(root, file))

    # Create empty dataframs has only the name of the columns
    full_df_cols = ['artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName',
                    'length', 'level', 'location', 'method', 'page', 'registration',
                    'sessionId', 'song', 'status', 'ts', 'userId']
    full_df = pd.DataFrame(columns=full_df_cols)

    # Concatenate every csv file into one csv file
    for file in file_paths:
        df = pd.read_csv(file)
        full_df = pd.concat([full_df, df], axis=0)

    # Drop the columns which we will not use in queries
    full_df.drop(['auth', 'method', 'page', 'registration', 'ts', 'status'], axis=1, inplace=True)
    full_df.dropna(inplace=True)

    # Replace evey single apostrophe by a two apostrophes to make it easy to insert string type data into database
    full_df['artist'] = full_df['artist'].str.replace("'", "''")
    full_df['song'] = full_df['song'].str.replace("'", "''")
    full_df['firstName'] = full_df['firstName'].str.replace("'", "''")
    full_df['firstName'] = full_df['firstName'].str.replace("'", "''")

    return full_df

def insert_data(session , all_data):
    """

    :param all_data: Pandas DataFrame which has all of the data
    Insert data extracted from all_data to tables to make it easy to run the given quries
    """


    for _ ,row in tqdm(all_data.iterrows() , total = len(all_data) , desc = "inserting data"):
        insert_into_table(session, table_name='sessions',
                          values=f"{row['sessionId']},{row['itemInSession']},'{row['artist']} ',\
                           '{row['song']}' , {row['length']}")


        insert_into_table(session, table_name='users',
                          values=f"{int(row['userId'])},{int(row['sessionId'])},{int(row['itemInSession'])},\
                            '{row['artist']}','{row['song']}','{row['firstName']}','{row['lastName']}'")

        insert_into_table(session, table_name='songs',
                          values=f"'{row['song']}'  , {int(row['userId'])}, '{row['firstName']}' , '{row['lastName']}' ")


def main():
    full_df = concatenate_csv('event_data')


    session, cluster = ConnectToCassandra()# connect to database

    session.set_keyspace('sparkfy')

    insert_data(session , full_df)

    session.shutdown()
    cluster.shutdown()

    print("Done")


if __name__ == '__main__':
    main()

