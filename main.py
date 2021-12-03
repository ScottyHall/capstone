from os import stat
import plotly.graph_objects as go
import pandas as pd
from databaseConnection import databaseConnected
from createTables import createDroughtTable, createCountiesTable, createStatesTable
from dataClean import ingestCSV, cleanFips, insertIntoDrought, insertIntoStates, insertIntoCounties, insertMissingCounties, cleanFipsCols
from visualization import exportPlotlySVG, generateScatterPlot, genScatterPltNonlin, genCountyChart


def cleanAndPrep(dfDrought: pd.DataFrame, dfCounties: pd.DataFrame, dfStates: pd.DataFrame):
    canInsertIntoDB = False
    if (databaseConnected()):
        # create tables
        droughtTable = createDroughtTable()
        countiesTable = createCountiesTable()
        statesTable = createStatesTable()

    # main fips cleaning for joining data
    doesNotInclude = cleanFips(dfDrought, dfCounties, dfStates)

    if (databaseConnected() and droughtTable and countiesTable and statesTable):
        if (doesNotInclude['stateErrors'] + doesNotInclude['countyErrors'] == 0):
            canInsertIntoDB = True

    if (canInsertIntoDB):
        # insert data into database
        insertIntoDrought(dfDrought)
        insertIntoStates(dfStates)
        insertIntoCounties(dfCounties)
        insertMissingCounties()


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

    dfDrought = cleanFipsCols(dfDrought)

    # cleanAndPrep(dfDrought, dfCounties, dfStates)
    # generateVisualizations()


if __name__ == "__main__":
    main()
