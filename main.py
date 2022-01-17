from os import stat
import numpy as np
from numpy.lib.function_base import median

from pandas.core.frame import DataFrame
import plotly.graph_objects as go
import pandas as pd
from databaseConnection import databaseConnected
from createTables import createDroughtTable, createCountiesTable, createStatesTable, createRainTable
from dataClean import ingestCSV, cleanFips, insertIntoDrought, insertIntoStates, insertIntoCounties, insertMissingCounties, cleanFipsCols, getGeoData, cleanRainfallData, insertIntoRain, addThreeDigitFipsToCounties
from visualization import exportPlotlySVG, generateScatterPlot, genScatterPltNonlin, genCountyChartTimeline, genCountyChart, visualizeCountiesAllYears, stackedHistogram
from machineLearning import mlTester, concatData


def cleanAndPrep(dfDrought: pd.DataFrame, dfCounties: pd.DataFrame, dfStates: pd.DataFrame, dfRain: pd.DataFrame, newDB: bool = False):
    """Clean, prep, and insert data into database
    
    Parameters:
    dfDrought: pd.DataFrame - drought data
    dfCounties: pd.DataFrame - county data
    dfStates: pd.DataFrame - state data
    dfRain: pd.DataFrame - rain data
    newDB?: bool - optional True = wipe data and insert all, default = False

    Returns:
    bool - data cleaned and successfully inserted into database
    """
    droughtTable = False
    countiesTable = False
    statesTable = False
    rainTable = False
    canInsertIntoDB = False

    if (databaseConnected() and newDB):
        # create tables
        droughtTable = createDroughtTable()
        countiesTable = createCountiesTable()
        statesTable = createStatesTable()
        rainTable = createRainTable()
    elif (newDB == False):
        droughtTable = True
        countiesTable = True
        statesTable = True
        rainTable = True
    else:
        print('Could not pass table section of cleaning')
        return False

    # main fips cleaning for joining data
    doesNotInclude = cleanFips(dfDrought, dfCounties, dfStates)

    if (databaseConnected() and droughtTable and countiesTable and statesTable and rainTable):
        if (doesNotInclude['stateErrors'] + doesNotInclude['countyErrors'] == 0):
            canInsertIntoDB = True

    if (newDB and canInsertIntoDB):
        # insert data into database
        if (insertIntoDrought(dfDrought) and insertIntoStates(dfStates) and insertIntoCounties(dfCounties) and insertIntoRain(dfRain)):
            insertMissingCounties()
            return True
    elif (newDB == False and canInsertIntoDB):
        print('Existing Database Used. No database changes commited during cleaning')
        return True
    else:
        return False


def generateVisualizations(dfDrought):
    # generateScatterPlot(dfDrought)
    # genScatterPltNonlin(dfDrought)
    # genCountyChart(dfDrought)
    # genCountyChartTimeline(dfDrought)
    # visualizeCountiesAllYears(dfDrought)
    stackedHistogram(dfDrought)

def generateAllCountyVisualization(dfDrought, yearsNp):
    for year in yearsNp:
        df = dfDrought.loc[dfDrought['year'] >= year]
        yearStr = year.astype(str)

def generateDataByCounty(dfDrought, counties):
    """generate additional data by each county
    """
    for county in counties[:10]:
        countyByYear = dfDrought.loc[dfDrought['countyfips'] == county]
        pdsi = countyByYear['pdsi']
        calcMedian = np.median(pdsi)
        calcAverage = np.average(pdsi)
        calcStd = np.std(pdsi)
        calcMin = np.amin(pdsi)
        calcMax = np.amax(pdsi)
        # print('Median: {0}, Average: {1}, Std: {2}, Min: {3}, Max: {4}'.format(calcMedian, calcAverage, calcStd, calcMin, calcMax))
        # genScatterPltNonlin(countyByYear, 'title2')

def main():
    # counties = getGeoData()
    # ingest source csv data
    dfDrought = ingestCSV()
    dfCounties = ingestCSV('sourceData/counties.csv')
    dfStates = ingestCSV('sourceData/states.csv')
    dfRain = ingestCSV('sourceData/climdiv-pcpncy-v1.0.0-20220108.csv')

    # clean the fips columns
    dfDrought = cleanFipsCols(dfDrought, 'countyfips', 5)
    dfDrought = cleanFipsCols(dfDrought, 'statefips', 2)
    dfCounties = cleanFipsCols(dfCounties, 'county_fips', 5)
    dfStates = cleanFipsCols(dfStates, 'noaa_state_fips', 2)
    dfStates = cleanFipsCols(dfStates, 'state_fips', 2)
    dfRain = cleanFipsCols(dfRain, 'id_code', 11)

    # add additional data
    dfRain = cleanRainfallData(dfRain)
    dfCounties = addThreeDigitFipsToCounties(dfCounties)

    # last param to True if database tables to be dropped and re-inserted
    # send True as the final input on cleanAndPrep to insert new data to database
    dataCleaned = cleanAndPrep(dfDrought, dfCounties, dfStates, dfRain)

    years = dfDrought['year'].unique()
    counties = dfDrought['countyfips'].unique()

    if (dataCleaned):
        print('Data cleaning completed successfully ========================')
        # df = dfDrought.loc[dfDrought['year'] >= 1960]
        # numpy data array for machine learning algorithms

        # dfDroughtDataArr = df[['pdsi', 'statefips']].to_numpy()
        # mlTester(dfDroughtDataArr)

        dfCombinedDroughtRainData = concatData(dfDrought, dfRain, dfStates)
    
        generateDataByCounty(dfDrought, counties)

        # generateVisualizations(dfDrought)
        # generateAllCountyVisualization(dfDrought, years)
    else:
        print('Could not process data further, failed cleaning process')


if __name__ == "__main__":
    main()
