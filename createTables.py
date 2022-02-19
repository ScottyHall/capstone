from databaseConnection import getDatabaseConnection


def createRainTable():
    """Create rain table in postgres

    Parameters: None

    Returns: boolean True on success"""
    conn = getDatabaseConnection()
    if (conn):
        cur = conn.cursor()
        try:
            cur.execute('DROP TABLE IF EXISTS rain;')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS rain(
                    id serial PRIMARY KEY,
                    state_id varchar(2) NOT NULL,
                    county_id varchar(3) NOT NULL,
                    year smallint NOT NULL,
                    jan numeric NOT NULL,
                    feb numeric NOT NULL,
                    mar numeric NOT NULL,
                    apr numeric NOT NULL,
                    may numeric NOT NULL,
                    jun numeric NOT NULL,
                    jul numeric NOT NULL,
                    aug numeric NOT NULL,
                    sep numeric NOT NULL,
                    oct numeric NOT NULL,
                    nov numeric NOT NULL,
                    dec numeric NOT NULL
                );''')
            conn.commit()
        except Exception as err:
            print('--- Failed to create rain table ---')
            print(err)
            cur.close()
            conn.close()
            return False
        else:
            print('--- Successfully Created rain table in database ---')
            cur.close()
            conn.close()
            return True
    else:
        print('No database connection')
        return False


def createPdsiPrecipTable():
    """Create pdsiPrecip table in postgres

    Parameters: None

    Returns: boolean True on success"""
    conn = getDatabaseConnection()
    if (conn):
        cur = conn.cursor()
        try:
            cur.execute('DROP TABLE IF EXISTS pdsi_precip;')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS pdsi_precip(
                    id serial PRIMARY KEY,
                    year smallint NOT NULL,
                    month smallint NOT NULL,
                    county_fips varchar(5) NOT NULL,
                    pdsi numeric NOT NULL,
                    precip NUMERIC NOT NULL,
                    state_fips varchar(2) NOT NULL
                );''')
            conn.commit()
        except Exception as err:
            print('--- Failed to create pdsiPrecip table ---')
            print(err)
            cur.close()
            conn.close()
            return False
        else:
            print('--- Successfully Created pdsiPrecip table in database ---')
            cur.close()
            conn.close()
            return True
    else:
        print('No database connection')
        return False


def createDroughtTable():
    """Create drought table in postgres

    Parameters: None

    Returns: boolean True on success"""
    conn = getDatabaseConnection()
    if (conn):
        cur = conn.cursor()
        try:
            cur.execute('DROP TABLE IF EXISTS drought;')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS drought(
                    id serial PRIMARY KEY,
                    year smallint NOT NULL,
                    month smallint NOT NULL,
                    state_fips varchar(2) NOT NULL,
                    county_fips varchar(5) NOT NULL,
                    pdsi numeric NOT NULL
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
            cur.execute('DROP TABLE IF EXISTS states;')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS states(
                    id serial PRIMARY KEY,
                    name varchar(32) NOT NULL,
                    postal_code varchar(2) NOT NULL,
                    fips varchar(2) UNIQUE NOT NULL,
                    noaa_code varchar(2) NOT NULL
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
            cur.execute('DROP TABLE IF EXISTS counties;')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS counties(
                    id serial PRIMARY KEY,
                    fips varchar(5) UNIQUE NOT NULL,
                    name varchar(64) NOT NULL,
                    fips_only varchar(3) NOT NULL
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
