from os import stat
import plotly.graph_objects as go
import pandas as pd
from databaseConnection import databaseConnected
from createTables import createDroughtTable, createCountiesTable, createStatesTable
from dataClean import ingestCSV, cleanFips, insertIntoDrought, insertIntoStates, insertIntoCounties, insertMissingCounties, cleanFipsCols
from visualization import exportPlotlySVG, generateScatterPlot, genScatterPltNonlin, genCountyChart


def cleanAndPrep(dfDrought: pd.DataFrame, dfCounties: pd.DataFrame, dfStates: pd.DataFrame, newDB: bool = False):
    """Clean, prep, and insert data into database
    
    Parameters:
    dfDrought: pd.DataFrame - drought data
    dfCounties: pd.DataFrame - county data
    dfStates: pd.DataFrame - state data
    newDB?: bool - optional True = wipe data and insert all, default = False

    Returns:
    bool - data cleaned and successfully inserted into database
    """
    droughtTable = False
    countiesTable = False
    statesTable = False
    canInsertIntoDB = False

    if (databaseConnected() and newDB):
        # create tables
        droughtTable = createDroughtTable()
        countiesTable = createCountiesTable()
        statesTable = createStatesTable()
    elif (newDB == False):
        droughtTable = True
        countiesTable = True
        statesTable = True
    else:
        print('Could not pass table section of cleaning')
        return False

    # main fips cleaning for joining data
    doesNotInclude = cleanFips(dfDrought, dfCounties, dfStates)

    if (databaseConnected() and droughtTable and countiesTable and statesTable):
        if (doesNotInclude['stateErrors'] + doesNotInclude['countyErrors'] == 0):
            canInsertIntoDB = True

    if (newDB and canInsertIntoDB):
        # insert data into database
        if (insertIntoDrought(dfDrought) and insertIntoStates(dfStates) and insertIntoCounties(dfCounties)):
            insertMissingCounties()
            return True
    elif (newDB == False and canInsertIntoDB):
        print('Existing Database Used. No database changes commited during cleaning')
        return True
    else:
        return False


def generateVisualizations(dfDrought):
    generateScatterPlot(dfDrought)
    genScatterPltNonlin(dfDrought)
    # genCountyChart(dfDrought)
    # genCountyChart(dfDrought)


def main():
    # ingest source csv data
    dfDrought = ingestCSV()
    dfCounties = ingestCSV('sourceData/counties.csv')
    dfStates = ingestCSV('sourceData/states.csv')

    # clean the fips columns
    dfDrought = cleanFipsCols(dfDrought, 'countyfips', 5)
    dfDrought = cleanFipsCols(dfDrought, 'statefips', 2)
    dfCounties = cleanFipsCols(dfCounties, 'county_fips', 5)
    dfStates = cleanFipsCols(dfStates, 'state_fips', 2)

    # last param to True if database tables to be dropped and re-inserted
    dataCleaned = cleanAndPrep(dfDrought, dfCounties, dfStates)

    if (dataCleaned):
        print('Data cleaning completed successfully ========================')
        generateVisualizations(dfDrought)
    else:
        print('Could not process data further, failed cleaning process')


if __name__ == "__main__":
    main()
