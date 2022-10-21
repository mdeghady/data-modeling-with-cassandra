import cassandra
from cassandra.cluster import Cluster

from CQL_quries import create_table

def ConnectToCassandra():
    """
    Create a connection to cassandra databas and return session, cluster to be able to
    execute quries and shutdown the connection
    """
    try:
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()

    except Exception as e:
        print("Can't connect to Cassandra Data Base")
        print(e)

    return session, cluster


def createKeySpace(session):
    """

    :param session: Session attribute to be able to run quries in the database
    run quries to delete the keyspace if it exists and create it again
    """
    drop_query = "Drop kEYSPACE IF EXISTS sparkfy"
    try:
        session.execute(drop_query)
    except Exception as e:
        print("Can't drop the key space")
        print(e)

    create_query = """CREATE KEYSPACE IF NOT EXISTS sparkfy
                    WITH REPLICATION = 
                    {'class' : 'SimpleStrategy' , 'replication_factor' : 1}"""

    try:
        session.execute(create_query)
    except Exception as e:
        print("Can't create the key space")
        print(e)

    try:
        session.set_keyspace('sparkfy')
    except Exception as e:
        print("can't set keyspace")

def create_all_tables(session):
    """

    :param session: Session attribute to be able to run quries in the database
    simple function to create all tables which will be used in the project
    """

    tables_col_names = {
        'sessions' : "sessionId int,  itemInSession int ,artist text , song_title text ,song_length  float",
        'users' : "userId int,  sessionId int, itemInSession int,artist text , song_title text , firstName text ,lastName text ",
        'songs' : "song_title text , userId int, firstName text ,lastName text "
    }

    tables_primary_key = {
        'sessions' : "(sessionId , itemInSession)",
        'users' : "(userId , sessionId) , itemInSession",
        'songs' : "(song_title) , userId"
    }

    tables = ['sessions' , 'users' , 'songs']

    for table in tables:

        create_table(session,
                     table_name=table,
                     column_names=tables_col_names[table],
                     primary_keys=tables_primary_key[table])



def main():

    session , cluster = ConnectToCassandra()#connect to database
    createKeySpace(session)

    create_all_tables(session)

    #shutdown the session and close the connection
    session.shutdown()
    cluster.shutdown()


if __name__ == '__main__':
    main()

