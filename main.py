from distutils.log import debug
from os import stat
import sys
from typing import Dict
import numpy as np
from numpy.lib.function_base import median

from pandas.core.frame import DataFrame
import plotly.graph_objects as go
import pandas as pd
from databaseConnection import databaseConnected
from createTables import createDroughtTable, createCountiesTable, createStatesTable, createRainTable, createPdsiPrecipTable
from dataClean import ingestCSV, cleanFips, insertIntoDrought, insertIntoStates, insertIntoCounties, insertMissingCounties, insertIntoPdsiPrecip, cleanFipsCols, getGeoData, cleanRainfallData, insertIntoRain, addThreeDigitFipsToCounties
from visualization import generateScatterPlot, genScatterPltNonlin, genCountyChartTimeline, genCountyChart, lineChartPrecip, visualizeCountiesAllYears, stackedHistogram, genCountyPrecipCombined, genCountyPDSICombined, genCountyLowerQuartilePdsi, genCountyLowerQuartilePrecip, genBubbleChart, lineChartCorr
from machineLearning import getAnnualQuantileCountyDatasetPdsi, getAnnualQuantileCountyDatasetPrecip, concatData


def cleanAndPrep(dfDrought: pd.DataFrame, dfCounties: pd.DataFrame,
                 dfStates: pd.DataFrame, dfRain: pd.DataFrame,
                 newDB: bool = False):
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


def getAverageAnnual(dfCombined: pd.DataFrame, counties: np.array, years: np.array):
    """get the annual PDSI average by county

    Parameters:
    dfCombined: pd.DataFrame - combined drought and precip data
    counties: np.array - np array counties

    Returns:
    dfPDSI: pd.DataFrame - dataframe with annual average PDSI
    """
    print('Calculating Averages Consolidated Annually ========================')
    annualMeanData = {'year': [], 'countyFips': [], 'stateFips': [],
                      'pdsiAvg': [], 'precipAvg': []}

    for index, g in dfCombined.groupby(['county_fips', 'year']):
        annualMeanData['year'].append(g['year'].unique()[0])
        annualMeanData['countyFips'].append(g['county_fips'].unique()[0])
        annualMeanData['stateFips'].append(g['state_fips'].unique()[0]),
        annualMeanData['pdsiAvg'].append(round(g['pdsi'].mean(), 2))
        annualMeanData['precipAvg'].append(round(g['rainfall'].mean(), 2))

    dfAnnualMeans = pd.DataFrame(annualMeanData)
    dfAnnualMeans = dfAnnualMeans.loc[dfAnnualMeans.index.drop_duplicates()]
    print('Finished gathering counties')

    return dfAnnualMeans


def visualizations(dfDrought: pd.DataFrame, dfCombined: pd.DataFrame, dfAnnualMeans: pd.DataFrame, years: np.ndarray):
    """Method to run all visualizations

    Parameters:
    dfDrought: pd.DataFrame - drought data
    dfCombined: pd.DataFrame - combined pdsi and precipitation data
    years: 
    """
    print('Starting Visualizations ========================')

    for year in years:
        # annual precip by year
        genCountyPrecipCombined(
            dfAnnualMeans, 'annualAvgPrecip' + str(year), year)

        # annual pdsi by year
        genCountyPDSICombined(dfAnnualMeans, 'annualAvgPDSI' + str(year), year)

        print(year)

    print('Finished Visualizations ========================')


def quartileVisualizations(quartilePdsi, quartilePrecip, years):
    """Generate visualizations that depend on initial binning on quartile

    Parameters:
    quartilePdsi
    quartilePrecip
    years: int[] - array of valid years to calculate off of

    Returns: Void
    """
    genBubbleChart(quartilePdsi.get(
        'q1'), years, 'Annual Average PDSI Lower Quartile (Dry)', 'Q1', 'LowerQuartileAnnualAvgPdsi', 'k-nearest Neighbor Regression Annual PDSI Lower Quartile')

    genBubbleChart(quartilePdsi.get(
        'q3'), years, 'Annual Average PDSI Upper Quartile (Wet)', 'Q3', 'UpperQuartileAnnualAvgPdsi', 'k-nearest Neighbor Regression Annual PDSI Upper Quartile')

    genCountyLowerQuartilePdsi(quartilePdsi.get('q1'), 'LowerQuartilePdsi')

    genCountyLowerQuartilePrecip(
        quartilePrecip.get('q1'), 'LowerQuartilePrecip')


def annualPdsiPrecipCorr(dfCombinedDroughtRainData: pd.DataFrame, years):
    forDataFrame = {'year': [], 'corrCoeff': []}
    for year in years:
        corrCoeff = dfCombinedDroughtRainData.loc[dfCombinedDroughtRainData['year'] == year].corr(
            'pearson')['rainfall']['pdsi']
        if (corrCoeff):
            forDataFrame['year'].append(year)
            forDataFrame['corrCoeff'].append(corrCoeff)
    dfCorrCoeff = pd.DataFrame(data=forDataFrame)

    return dfCorrCoeff


def annualPrecipCombined(dfAnnualMeans: pd.DataFrame, years):
    forDataFrame = {'year': [], 'precipAvg': [], 'precipMedian': []}
    for year in years:
        precipAvg = dfAnnualMeans.loc[dfAnnualMeans['year']
                                      == year]['precipAvg'].mean()
        precipMedian = dfAnnualMeans.loc[dfAnnualMeans['year']
                                         == year]['precipAvg'].median()
        if (precipAvg and precipMedian):
            forDataFrame['year'].append(year)
            forDataFrame['precipAvg'].append(precipAvg)
            forDataFrame['precipMedian'].append(precipMedian)
    dfAnnualPrecipCombined = pd.DataFrame(data=forDataFrame)

    return dfAnnualPrecipCombined


def main():
    populateNewDbTables = False
    performDataClean = True
    performVisualizations = False
    performQuartileVisualizations = True
    performLineVisualizations = True

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
    if (performDataClean):
        dataCleaned = cleanAndPrep(
            dfDrought, dfCounties, dfStates, dfRain, populateNewDbTables)
    else:
        dataCleaned = False

    years = dfDrought['year'].unique()
    counties = dfDrought['countyfips'].unique()

    if (dataCleaned or performDataClean is False):
        cleanStatus = 'completed successfully' if dataCleaned else 'skipped'
        print('Data cleaning {0} ========================'.format(cleanStatus))

        # dataframe of concatenated drought, precipitation, and state data
        dfCombinedDroughtRainData = concatData(dfDrought, dfRain, dfStates)

        # insert PDSI precip table
        if (populateNewDbTables):
            if (createPdsiPrecipTable()):
                insertIntoPdsiPrecip(dfCombinedDroughtRainData)

        # gets average annual precip and pdsi by each year by each county
        dfAnnualMeans = getAverageAnnual(
            dfCombinedDroughtRainData, counties, years)

        dfAnnualPrecipCombined = annualPrecipCombined(dfAnnualMeans, years)

        # get the thresholds for q1 and q3 along with IQR
        annualPdsiQuantile = dfAnnualMeans['pdsiAvg'].quantile(q=[0.25, 0.75])
        annualPrecipQuatile = dfAnnualMeans['precipAvg'].quantile(q=[
                                                                  0.25, 0.75])

        # get the correlation between pdsi and precipitation separated by year
        corrByYear = annualPdsiPrecipCorr(dfCombinedDroughtRainData, years)
        corrAvg = dfAnnualMeans.corr('pearson')['pdsiAvg']['precipAvg']
        print(
            'Correlation PDSI and Precipitation all yearly averages: {0}'.format(corrAvg))

        quartilePdsi = getAnnualQuantileCountyDatasetPdsi(
            dfAnnualMeans, annualPdsiQuantile, annualPrecipQuatile)

        quartilePrecip = getAnnualQuantileCountyDatasetPrecip(
            dfAnnualMeans, annualPdsiQuantile, annualPrecipQuatile)

        if (performQuartileVisualizations):
            quartileVisualizations(quartilePdsi, quartilePrecip, years)

        if (performLineVisualizations and corrAvg):
            lineChartPrecip(dfAnnualPrecipCombined, corrAvg, 'precipAvg')
            lineChartCorr(corrByYear, corrAvg, 'correlationPdsiPrecip')

        # Run visualizations
        if (performVisualizations):
            visualizations(dfDrought, dfCombinedDroughtRainData,
                           dfAnnualMeans, years)

    else:
        print('Could not process data further, failed cleaning process')


if __name__ == "__main__":
    main()
