import time
import warnings

import numpy as np
import matplotlib.pyplot as plt

import pandas as pd
from sklearn.neighbors import KNeighborsRegressor


def concatData(dfDrought: pd.DataFrame, dfRain: pd.DataFrame, dfStates: pd.DataFrame):
    """Concat Data method concats rainfall data to each entry to pdsi dataframe
    convert the dataframe to a numpy array for sklearn

    Parameters:
    dfDrought: pd.DataFrame - drought data
    dfRain: pd.DataFrame - rainfall data

    Returns:
    dataset: pd.DataFrame - all merged data
    year, month, county_fips, pdsi, rainfall, state_fips
    """
    print('Combining PDSI and Precip Data ========================')
    dfMergeRain = pd.merge(
        dfRain, dfStates, left_on='state_id', right_on='noaa_state_fips')

    dfMergeRain = dfMergeRain.drop(['state_id'], axis=1)
    dfMergeRain['countyfips'] = dfMergeRain['state_fips'] + \
        dfMergeRain['county_id']

    dfMergeRain['year'] = dfMergeRain['year'].astype(int)
    dfDrought['year'] = dfDrought['year'].astype(int)

    dfMergeRainDrought = pd.merge(dfDrought, dfMergeRain, on=[
                                  'year', 'countyfips'], how='left')

    # month translation dict for tuple index
    months = {1: 6, 2: 7, 3: 8, 4: 9, 5: 10, 6: 11,
              7: 12, 8: 13, 9: 14, 10: 15, 11: 16, 12: 17}
    dataForDF = {'year': [], 'month': [], 'county_fips': [],
                 'pdsi': [], 'rainfall': [], 'state_fips': []}
    for row in dfMergeRainDrought.itertuples(index=False):
        dataForDF['year'].append(row[0])
        dataForDF['month'].append(row[1])
        dataForDF['county_fips'].append(row[3])
        dataForDF['pdsi'].append(row[4])
        dataForDF['rainfall'].append(row[months[row[1]]])
        dataForDF['state_fips'].append(row[2])
    dfNewDroughtRain = pd.DataFrame(data=dataForDF)

    return dfNewDroughtRain


def getAnnualQuantileCountyDatasetPdsi(dfAnnualMeans: pd.DataFrame, annualPdsiQuantile: pd.DataFrame, annualPrecipQuatile: pd.DataFrame):
    """Annual Quantile Dataset by county and year

    Gets the data that falls within the lower quartile given a year

    Parameters:
    dfAnnualMeans: pd.DataFrame - 'year': [], 'countyFips': [], 'stateFips': [], 'pdsiAvg': [], 'precipAvg': []

    Returns:
    annualQuartiles: {'q1', 'q3'}
    """
    dfLowerQuart = dfAnnualMeans[dfAnnualMeans['pdsiAvg']
                                 <= annualPdsiQuantile[0.25]]
    dfUpperQuart = dfAnnualMeans[dfAnnualMeans['pdsiAvg']
                                 >= annualPdsiQuantile[0.75]]
    dfIqr = annualPdsiQuantile[0.75] - annualPdsiQuantile[0.25]

    annualQuartiles = {'q1': dfLowerQuart, 'q3': dfUpperQuart, 'iqr': dfIqr}

    return annualQuartiles


def getAnnualQuantileCountyDatasetPrecip(dfAnnualMeans: pd.DataFrame, annualPdsiQuantile: pd.DataFrame, annualPrecipQuantile: pd.DataFrame):
    """Annual Quantile Dataset by county and year

    Gets the data that falls within the lower quartile given a year

    Parameters:
    dfAnnualMeans: pd.DataFrame - 'year': [], 'countyFips': [], 'stateFips': [], 'pdsiAvg': [], 'precipAvg': []

    Returns:
    annualQuartiles: {'q1', 'q3'}
    """
    dfLowerQuart = dfAnnualMeans[dfAnnualMeans['pdsiAvg']
                                 <= annualPrecipQuantile[0.25]]
    dfUpperQuart = dfAnnualMeans[dfAnnualMeans['pdsiAvg']
                                 >= annualPrecipQuantile[0.75]]
    dfIqr = annualPrecipQuantile[0.75] - annualPrecipQuantile[0.25]

    annualQuartiles = {'q1': dfLowerQuart, 'q3': dfUpperQuart, 'iqr': dfIqr}

    return annualQuartiles


def kNearestNeighborModels(df: pd.DataFrame):
    """K nearest neighbor regression lines

    Get the weighted and unweighted average of nearest plots to get the regression lines
    """
    X = df.year.values.reshape(-1, 1)

    xRange = np.linspace(X.min(), X.max(), 100)

    knnUniform = KNeighborsRegressor(10, weights='uniform')
    knnUniform.fit(X, df.countyAmt)
    y_uni = knnUniform.predict(xRange.reshape(-1, 1))

    knnDist = KNeighborsRegressor(10, weights='distance')
    knnDist.fit(X, df.countyAmt)
    yDist = knnDist.predict(xRange.reshape(-1, 1))

    d = {'xRange': xRange, 'yDist': yDist, 'yUni': y_uni}
    return d
