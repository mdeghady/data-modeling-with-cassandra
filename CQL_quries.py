def create_table(session, table_name, column_names, primary_keys):
    """
    :param session: Session attribute to be able to run quries in the database
    :param table_name: The name of the table that will be created
    :param column_names: The name of the columns of the table that will be created
    :param primary_keys: partition and clustered keys for the table in the form (partition keys) , clustered keys
    Run query to drop the table if it exists and create it again as the following query
    DROPT TABLE IF EXISTS table_name
    CREATE TABLE table_name (column_names ,PRIMARY KEY primary_keys)
    """
    query = f"DROP TABLE IF EXISTS {table_name}"
    try:
        session.execute(query)
    except Exception as e:
        print("Can't Drop table")
        print(e)

    query = f"""CREATE TABLE {table_name} ({column_names},
            PRIMARY KEY ({primary_keys}));"""
    try:
        session.execute(query)
    except Exception as e:
        print("Can't Create table")
        print(query)
        print(e)


def insert_into_table(session , table_name , values):
    """

    :param session: Session attribute to be able to run quries in the database
    :param table_name: The name of the table which the values will be inserted into
    :param values: Values which will be inserted into the table
    Insert new values to the column by run the following quries
    INSERT INTO table_name (table columns) VALUES (values)
    """
    #Columns of every Table in the database to make it easy to write an insert query
    tables_columns = {
        'sessions': "(sessionId , itemInSession, artist , song_title , song_length)",
        'users': "(userId , sessionId, itemInSession , artist , song_title , firstName , lastName)",
        'songs': "(song_title,userId , firstName , lastName)"
    }
    query = f"INSERT INTO {table_name} {tables_columns[table_name]} VALUES ({values}) "
    try:
        session.execute(query)
    except Exception as e:
        print("Can't insert into table")
        print(query)
        print(e)
    