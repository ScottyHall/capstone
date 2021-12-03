from databaseConnection import getDatabaseConnection


def createDroughtTable():
    """Create drought table in postgres

    Parameters: None

    Returns: boolean True on success"""
    conn = getDatabaseConnection()
    if (conn):
        cur = conn.cursor()
        try:
            # cur.execute('DROP TABLE IF EXISTS drought;')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS drought(
                    id serial PRIMARY KEY,
                    year smallint NOT NULL,
                    month smallint NOT NULL,
                    state_fips smallint NOT NULL,
                    county_fips integer NOT NULL,
                    pdsi numeric NOT NULL,
                    date date
                );''')
            conn.commit()
        except Exception as err:
            print('--- Failed to create drought table ---')
            print(err)
            cur.close()
            conn.close()
            return False
        else:
            print('--- Successfully Created drought table in database ---')
            cur.close()
            conn.close()
            return True
    else:
        print('No database connection')
        return False


def createStatesTable():
    """Create states table in postgres

    Parameters: None

    Returns: boolean True on success"""
    conn = getDatabaseConnection()
    if (conn):
        cur = conn.cursor()
        try:
            # cur.execute('DROP TABLE IF EXISTS states;')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS states(
                    id serial PRIMARY KEY,
                    name varchar(32) NOT NULL,
                    postal_code varchar(2) NOT NULL,
                    fips smallint UNIQUE NOT NULL
                );''')
            conn.commit()
        except Exception as err:
            print('--- Failed to create states table ---')
            print(err)
            cur.close()
            conn.close()
            return False
        else:
            print('--- Successfully Created states table in database ---')
            cur.close()
            conn.close()
            return True
    else:
        print('No database connection')
        return False


def createCountiesTable():
    """Create counties table in postgres

    Parameters: None

    Returns: boolean True on success"""
    conn = getDatabaseConnection()
    if (conn):
        cur = conn.cursor()
        try:
            # cur.execute('DROP TABLE IF EXISTS counties;')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS counties(
                    id serial PRIMARY KEY,
                    fips integer UNIQUE NOT NULL,
                    name varchar(64) NOT NULL
                );''')
            conn.commit()
        except Exception as err:
            print('--- Failed to create counties table ---')
            print(err)
            cur.close()
            conn.close()
            return False
        else:
            print('--- Successfully Created counties table in database ---')
            cur.close()
            conn.close()
            return True
    else:
        print('No database connection')
        return False
