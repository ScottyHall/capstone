# Used to clean the drought data, county data, and state data
from datetime import date
import json
import pandas as pd
import numpy as np
from io import StringIO

from pandas.core.frame import DataFrame
from databaseConnection import getDatabaseConnection
from createTables import createDroughtTable, createCountiesTable, createStatesTable, createRainTable


def cleanRainfallData(df: pd.DataFrame):
    i = 0
    data = {'state_id': [], 'county_id': [], 'year': [],
            'jan': [], 'feb': [], 'mar': [], 'apr': [], 'may': [], 'jun': [],
            'jul': [], 'aug': [], 'sep': [], 'oct': [], 'nov': [], 'dec': []}
    for row in df.itertuples(index=False):
        if (len(row[0]) == 11):
            idState = row[0][:2]
            idCounty = row[0][2:5]
            idRain = row[0][5:7]
            idYear = row[0][7:]
            if (idRain == '01'):
                data['state_id'].append(idState)
                data['county_id'].append(idCounty)
                data['year'].append(idYear)
                data['jan'].append(row[1])
                data['feb'].append(row[2])
                data['mar'].append(row[3])
                data['apr'].append(row[4])
                data['may'].append(row[5])
                data['jun'].append(row[6])
                data['jul'].append(row[7])
                data['aug'].append(row[8])
                data['sep'].append(row[9])
                data['oct'].append(row[10])
                data['nov'].append(row[11])
                data['dec'].append(row[12])
                i += 1
    if (len(df) == i):
        print('All rows IDs are valid length')
    newDf = pd.DataFrame(data)
    return newDf

def addThreeDigitFipsToCounties(df: pd.DataFrame):
    """ Take the last three digits of the zipcodes to get rid of the generic for NOAA references
    where the state code differs from the norm.

    Parameter:
    df: pd.DataFrame

    Returns:
    newDf: pd.DataFrame with new fips_only variable
    """
    data = {'county_fips': [], 'county_name': [], 'fips_only': []}
    for row in df.itertuples(index=False):
        if (len(row[0]) == 5):
            fipsOnly = row[0][2:]
            data['county_fips'].append(row[0])
            data['county_name'].append(row[1])
            data['fips_only'].append(fipsOnly)
    newDf = pd.DataFrame(data)
    return newDf

def getGeoData(geoSource: str = 'sourceData/geo.json'):
    with open(geoSource) as f:
        counties = json.load(f)
    return counties

def insertAmtEqualsSource(dfLen: int, dbTblLen: int):
    if (dfLen == dbTblLen):
        print('Source Rows and DB Rows Match. Total Rows: {0}'.format(dfLen))
        return True
    else:
        print('Source Data Rows: {0} | Database rows: {1}'.format(dfLen, dbTblLen))
        return False

def cleanFipsCols(df: pd.DataFrame, column: str, lenCol: int):
    """Clean fips col edits the fips codes to ensure all are lenCol digits long
    Adds leading zeros when needed

    Parameter:
    df: pd.DataFrame
    column: str - column name for fips data

    Returns:
    df: pd.DataFrame with edited fips
    """
    df[column] = df[column].str.zfill(lenCol)
    return df

def insertIntoRain(dataFrame: pd.DataFrame):
    """Insert rain data into the database

    Parameters:
    dataFrame: pd.DataFrame

    Returns:
    bool - data was inserted and commited to db successfully
    """
    conn = getDatabaseConnection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM rain LIMIT 3;")
    if (cur.fetchone() == None):
        totalRows = len(dataFrame.index)
        currentRow = 0
        print('Starting Rain Insert into Database...')
        try:
            for row in dataFrame.itertuples(index=False):
                currentRow += 1
                percentage = (currentRow / totalRows) * 100
                if (percentage % 5 == 0):
                    print('{0}% complete'.format(percentage))
                sql = cur.mogrify("INSERT INTO rain (state_id, county_id, year, jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                                  (row.state_id, row.county_id, row.year, row.jan, row.feb, row.mar, row.apr, row.may, row.jun, row.jul, row.aug, row.sep, row.oct, row.nov, row.dec))
                cur.execute(sql)
        except Exception as err:
            print(err)
            cur.close()
            conn.close()
            return False
        else:
            # only commit the data to DB if there is no loss of data from source
            if (insertAmtEqualsSource(totalRows, currentRow)):
                conn.commit()
                return True
            else:
                print('RAIN DATA NOT COMMITED TO DB! Row counts do not match source data')
                return False            
    else:
        cur.close()
        conn.close()
        return False

def insertIntoDrought(dataFrame: pd.DataFrame):
    """Insert drought data into the database

    Parameters:
    dataFrame: pd.DataFrame

    Returns:
    bool - data was inserted and commited to db successfully
    """
    conn = getDatabaseConnection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM drought LIMIT 3;")
    if (cur.fetchone() == None):
        totalRows = len(dataFrame.index)
        currentRow = 0
        print('Starting Drought Insert into Database...')
        try:
            for row in dataFrame.itertuples(index=False):
                currentRow += 1
                percentage = (currentRow / totalRows) * 100
                if (percentage % 5 == 0):
                    print('{0}% complete'.format(percentage))
                sql = cur.mogrify("INSERT INTO drought (year, month, state_fips, county_fips, pdsi) VALUES(%s, %s, %s, %s, %s);",
                                  (row.year, row.month, row.statefips, row.countyfips, row.pdsi))
                cur.execute(sql)
        except Exception as err:
            print(err)
            cur.close()
            conn.close()
            return False
        else:
            # only commit the data to DB if there is no loss of data from source
            if (insertAmtEqualsSource(totalRows, currentRow)):
                conn.commit()
                return True
            else:
                print('DROUGHT DATA NOT COMMITED TO DB! Row counts do not match source data')
                return False            
    else:
        cur.close()
        conn.close()
        return False


def insertIntoStates(dataFrame: pd.DataFrame):
    """Insert state data into the database

    Parameters:
    dataFrame: pd.DataFrame

    Returns:
    bool - data was inserted and commited to db successfully
    """
    conn = getDatabaseConnection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM states LIMIT 3;")
    if (cur.fetchone() == None):
        totalRows = len(dataFrame.index)
        currentRow = 0
        print('Starting States Insert into Database...')
        try:
            for row in dataFrame.itertuples(index=False):
                currentRow += 1
                percentage = (currentRow / totalRows) * 100
                if (percentage % 5 == 0):
                    print('{0}% complete'.format(percentage))
                sql = cur.mogrify("INSERT INTO states (name, postal_code, fips, noaa_code) VALUES(%s, %s, %s, %s);",
                                  (row[0], row[1], row[2], row[3]))
                cur.execute(sql)
        except Exception as err:
            print(err)
            cur.close()
            conn.close()
            return False
        else:
            # only commit the data to DB if there is no loss of data from source
            if (insertAmtEqualsSource(totalRows, currentRow)):
                conn.commit()
                return True
            else:
                print('STATES DATA NOT COMMITED TO DB! Row counts do not match source data')
                return False        
    else:
        cur.close()
        conn.close()
        return False


def insertIntoCounties(dataFrame: pd.DataFrame):
    """Insert county data into the database

    Parameters:
    dataFrame: pd.DataFrame

    Returns:
    bool - data was inserted and commited to db successfully
    """
    conn = getDatabaseConnection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM counties LIMIT 3;")
    if (cur.fetchone() == None):
        totalRows = len(dataFrame.index)
        currentRow = 0
        print('Starting Counties Insert into Database...')
        try:
            for row in dataFrame.itertuples(index=False):
                currentRow += 1
                percentage = (currentRow / totalRows) * 100
                if (percentage % 5 == 0):
                    print('{0}% complete'.format(percentage))
                sql = cur.mogrify("INSERT INTO counties (fips, name, fips_only) VALUES(%s, %s, %s);",
                                  (row.county_fips, row.county_name, row.fips_only))
                cur.execute(sql)
        except Exception as err:
            print(err)
            cur.close()
            conn.close()
            return False
        else:
            # only commit the data to DB if there is no loss of data from source
            if (insertAmtEqualsSource(totalRows, currentRow)):
                conn.commit()
                return True
            else:
                print('COUNTIES DATA NOT COMMITED TO DB! Row counts do not match source data')
                return False
    else:
        cur.close()
        conn.close()
        return False


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
    cur.execute("SELECT name FROM counties WHERE fips='08014';")
    if (cur.fetchone() == None):
        cur.execute(
            "INSERT INTO counties (fips, name, fips_only) VALUES('08014', 'Broomfield County', '014');")

    """
    https://www.ddorn.net/data/FIPS_County_Code_Changes.pdf
    Florida, 1997: Dade county (FIPS 12025) is renamed as Miami-Dade county (FIPS 12086).
    """
    cur.execute("SELECT name FROM counties WHERE fips='12086';")
    if (cur.fetchone() == None):
        cur.execute(
            "INSERT INTO counties (fips, name, fips_only) VALUES('12086', 'Miami-Dade County', '086');")

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
    dfIngest = pd.read_csv(path, dtype={
        "countyfips": str,
        "county_fips": str,
        "statefips": str,
        "state_fips": str,
        "noaa_state_fips": str,
        "id_code": str
    })
    return dfIngest
