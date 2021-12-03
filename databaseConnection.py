import psycopg2


def getDatabaseConnection(dbName='capstoneDB', user='scotty'):
    """Connect to the postgres database

    Parameters:
    dbName (string): database name
    user (string): username for database

    Returns: DB connection
    """
    try:
        conn = psycopg2.connect("dbname={0} user={1}".format(dbName, user))
    except Exception as err:
        print('--- Failed to connect to {0} as {1} ---'.format(dbName, user))
        print(err)
    else:
        return conn

def databaseConnected():
    """Check if database connection can be established

    Parameters: None

    Returns:
    Boolean - connected or not
    """
    conn = getDatabaseConnection()
    if (conn):
        conn.close()
        return True
    else:
        return False
