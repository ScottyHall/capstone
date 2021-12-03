# Used to clean the drought data, county data, and state data
from datetime import date
import pandas as pd
import numpy as np
from io import StringIO
from databaseConnection import getDatabaseConnection
from createTables import createDroughtTable, createCountiesTable, createStatesTable


def cleanFipsCols(drought: pd.DataFrame):
    """Clean fips col edits the fips codes to ensure all are 5 digits
    Adds leading zeros when needed

    Parameter:
    drought: pd.DataFrame

    Returns:
    drought: pd.DataFrame with edited fips
    """
    drought['countyfips'] = drought['countyfips'].str.zfill(5)
    return drought


def insertIntoDrought(dataFrame: pd.DataFrame):
    """Insert drought data into the database

    Parameters:
    dataFrame: pd.DataFrame

    Returns: None
    """
    conn = getDatabaseConnection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM drought LIMIT 3;")
    if (cur.fetchone() == None):
        try:
            for row in dataFrame.itertuples(index=False):
                sql = cur.mogrify("INSERT INTO drought (year, month, state_fips, county_fips, pdsi, date) VALUES(%s, %s, %s, %s, %s);",
                                  (row[0], row[1], row[2], row[3], row[4], row[5]))
                cur.execute(sql)
        except Exception as err:
            print(err)
        else:
            print(dataFrame)
            conn.commit()
        finally:
            cur.close()
            conn.close()
    else:
        cur.close()
        conn.close()


def insertIntoStates(dataFrame: pd.DataFrame):
    """Insert state data into the database

    Parameters:
    dataFrame: pd.DataFrame

    Returns: None
    """
    conn = getDatabaseConnection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM states LIMIT 3;")
    if (cur.fetchone() == None):
        try:
            for row in dataFrame.itertuples(index=False):
                sql = cur.mogrify("INSERT INTO states (name, postal_code, fips) VALUES(%s, %s, %s);",
                                  (row[0], row[1], row[2]))
                cur.execute(sql)
        except Exception as err:
            print(err)
        else:
            print(dataFrame)
            conn.commit()
        finally:
            cur.close()
            conn.close()
    else:
        cur.close()
        conn.close()


def insertIntoCounties(dataFrame: pd.DataFrame):
    """Insert county data into the database

    Parameters:
    dataFrame: pd.DataFrame

    Returns: None
    """
    conn = getDatabaseConnection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM counties LIMIT 3;")
    if (cur.fetchone() == None):
        try:
            for row in dataFrame.itertuples(index=False):
                sql = cur.mogrify("INSERT INTO counties (fips, name) VALUES(%s, %s);",
                                  (row[0], row[1]))
                cur.execute(sql)
        except Exception as err:
            print(err)
        else:
            print(dataFrame)
            conn.commit()
        finally:
            cur.close()
            conn.close()
    else:
        cur.close()
        conn.close()


def insertIntoMissingCounties(countyFips: np.array):
    pass


def insertMissingCounties():
    """Insert missing counties manually inserts the two missing counties from the county data

    Parameters: None

    Returns: None
    """
    conn = getDatabaseConnection()
    cur = conn.cursor()

    """
    https://www.ddorn.net/data/FIPS_County_Code_Changes.pdf
    Colorado, 2001: Broomfield county (FIPS 8014) is created out of parts of Adams, Boulder, Jefferson, and Weld counties.
    The Census Bureau estimates that the resulting population
    loss was 21,512 for Boulder, 15,870 for Adams, 1,726 for Jefferson, and 69 for Weld county. (Dorn)
    """
    cur.execute("SELECT name FROM counties WHERE fips=8014;")
    if (cur.fetchone() == None):
        cur.execute(
            "INSERT INTO counties (fips, name) VALUES(8014, 'Broomfield County');")

    """
    https://www.ddorn.net/data/FIPS_County_Code_Changes.pdf
    Florida, 1997: Dade county (FIPS 12025) is renamed as Miami-Dade county (FIPS 12086).
    """
    cur.execute("SELECT name FROM counties WHERE fips=12086;")
    if (cur.fetchone() == None):
        cur.execute(
            "INSERT INTO counties (fips, name) VALUES(12086, 'Miami-Dade County');")

    conn.commit()
    cur.close()
    conn.close()


def getMissingStates(states: pd.DataFrame, drought: pd.DataFrame):
    """Checks states that do not have a match to the drought data
    These are the states that we have, but are not in the data

    Parameters:
    states: DataFrame
    drought: DataFrame

    Returns:
    missingStates[state_fips]: np.array

    Equivalent SQL query:
    SELECT a.name FROM states a LEFT OUTER JOIN drought b ON(a.fips = b.state_fips) WHERE b.year IS NULL;
    """
    missingStates = np.array

    leftJoin = states.merge(drought, how='left', left_on=[
                            'state_fips'], right_on=['statefips'])
    naRows = leftJoin[leftJoin['year'].isna()]
    print('Known States Not Found {0} ========================'.format(
        len(naRows)))
    print(naRows['state_name'])
    missingStates = naRows['state_fips'].values

    return missingStates


def getMissingCounties(counties: pd.DataFrame, drought: pd.DataFrame):
    """Checks counties that do not have a match to the drought data
    These are the counties that we have, but are not in the data

    Parameters:
    counties: DataFrame
    drought: DataFrame

    Returns:
    missingCounties[county_fips]: np.array

    Equivalent SQL query:
    SELECT a.name FROM counties a LEFT OUTER JOIN drought b ON(a.fips = b.county_fips) WHERE b.year IS NULL;
    """
    missingCounties = np.array

    leftJoin = counties.merge(drought, how='left', left_on=[
        'county_fips'], right_on=['countyfips'])
    naRows = leftJoin[leftJoin['year'].isna()]
    print('Known Counties Not Found {0} ========================'.format(
        len(naRows)))
    print(naRows['county_name'])
    missingCounties = naRows['county_fips'].values

    return missingCounties


def cleanFips(drought: pd.DataFrame, counties: pd.DataFrame, states: pd.DataFrame):
    """cleanFips main method for checking fips data for successful joining

    Parameters:
    drought: pd.DataFrame - drought source dataframe
    counties: pd.DataFrame - county source dataframe
    states: pd.DataFrame - state source dataframe

    Returns:
    doesNotInclude: dict - states, counties, stateErrors, countyErrors
    """
    droughtRows = len(drought.index)
    missingStates = getMissingStates(states, drought)
    missingCounties = getMissingCounties(counties, drought)

    droughtStatesJoin = drought.merge(
        states, how='left', left_on='statefips', right_on='state_fips')
    stateErrors = droughtRows - len(droughtStatesJoin.index)

    droughtCountiesJoin = drought.merge(
        counties, how='left', left_on='countyfips', right_on='county_fips')
    countyErrors = droughtRows - len(droughtCountiesJoin.index)

    doesNotInclude = {
        'states': missingStates,
        'counties': missingCounties,
        'stateErrors': stateErrors,
        'countyErrors': countyErrors
    }

    print('Invalid states: {0}'.format(stateErrors))
    print('Invalid counties: {0}'.format(countyErrors))

    return doesNotInclude


def ingestCSV(path: str = 'sourceData/drought.csv'):
    """IngestCSV pulls in source csv data with pandas

    Parameters:
    path: str - csv location

    Returns:
    dfIngest: pd.DataFrame
    """
    dfIngest = pd.read_csv(path, dtype={"countyfips": str})
    return dfIngest
