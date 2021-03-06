import json
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.figure_factory as ff
import plotly
import plotly.express as px
import os
import matplotlib.pyplot as plt
from dataClean import getGeoData
from machineLearning import kNearestNeighborModels


counties = getGeoData()


def exportMatplotPNG(figure: plt, fileName: str, exportDir: str = 'visualizations/png'):
    figure.savefig(exportDir + '/' + fileName + '.png')


def exportPlotlySVG(figure: plotly.graph_objects, fileName: str, exportDir: str = 'visualizations/svg'):
    """
    export plot svg for plotly export

    Parameters:
    figure: plotly.graph_objects - plot object
    fileName: str - name of export svg not including .svg
    exportDir?: str - directory for export (default=visualizations)

    Return: None
    """
    if (os.path.exists(exportDir)):
        try:
            figure.write_image(exportDir + '/' + fileName + '.svg')
        except Exception as err:
            print('Could not export plotly svg: {0}'.format(err))
        else:
            print('Exported Plotly {0}/{1}.svg'.format(exportDir, fileName))
    else:
        print(
            'Directory "{0} was not found for exporting plot"'.format(exportDir))


def exportPlotlyPNG(figure: plotly.graph_objects, fileName: str, exportDir: str = 'visualizations/png'):
    """
    export plot png for plotly export

    Parameters:
    figure: plotly.graph_objects - plot object
    fileName: str - name of export png not including .png
    exportDir?: str - directory for export (default=visualizations)

    Return: None
    """
    if (os.path.exists(exportDir)):
        try:
            figure.write_image(exportDir + '/' + fileName +
                               '.png', width=1024, height=768)
        except Exception as err:
            print('Could not export plotly png: {0}'.format(err))
        else:
            print('Exported Plotly {0}/{1}.png'.format(exportDir, fileName))
    else:
        print(
            'Directory "{0} was not found for exporting plot"'.format(
                exportDir)
        )


def exportPlotlyHTML(figure: plotly.graph_objects, fileName: str, exportDir: str = 'visualizations/html'):
    """
    export plot html for plotly export

    Parameters:
    figure: plotly.graph_objects - plot object
    fileName: str - name of export html not including .html
    exportDir?: str - directory for export (default=visualizations)

    Return: None
    """
    if (os.path.exists(exportDir)):
        try:
            figure.write_html(exportDir + '/' + fileName + '.html', config=dict(
                displayModeBar=False
            ))
        except Exception as err:
            print('Could not export plotly html: {0}'.format(err))
        else:
            print('Exported Plotly {0}/{1}.html'.format(exportDir, fileName))
    else:
        print(
            'Directory "{0} was not found for exporting plot"'.format(
                exportDir)
        )


def generateScatterPlot(df, title: str):
    fig = px.scatter(df, x='year', y='pdsi', color='pdsi')
    exportPlotlySVG(fig, title)


def genScatterPltNonlin(df: pd.DataFrame, title: str):
    # df = df.loc[df['year'] == 1950]
    fig = px.scatter(df, x='year', y='pdsi',
                     trendline='ols', trendline_options=dict(log_x=True),
                     title='Log-transformed fit on linear axes')
    exportPlotlySVG(fig, title)


def genCountyChartTimeline(dfDrought):
    """Gen county chart creates figure map of US with filled in counties as a timeline
    saves figure as image using export methods

    Parameters:
    dfDrought: df.DataFrame - dataframe for visualization

    Returns: None
    """
    df = dfDrought.loc[dfDrought['year'] >= 1960]
    # df = dfDrought
    fig = px.choropleth(df, geojson=counties, locations='countyfips', color='pdsi',
                        color_continuous_scale='Viridis_r',
                        animation_frame='year',
                        range_color=(10, -10),
                        scope='usa',
                        labels={'pdsi': 'PDSI'}
                        )
    fig.update_layout(margin={'r': 0, 't': 0, 'l': 0, 'b': 0})
    # exportPlotlyHTML(fig, 'countyMap', 'visualizations/countyMaps/html')
    # exportPlotlyPNG(fig, 'countyMap2011', 'visualizations/countyMaps')
    exportPlotlySVG(fig, 'countyMap2011', 'visualizations/countyMaps')


def genCountyChart(dfDrought, name: str):
    """Gen county chart creates figure map of US with filled in counties
    saves figure as image using export methods

    Parameters:
    dfDrought: df.DataFrame - dataframe for visualization

    Returns: None
    """
    # df = dfDrought.loc[dfDrought['year'] >= 1960]
    df = dfDrought
    fig = px.choropleth(df, geojson=counties, locations='countyfips', color='pdsi',
                        color_continuous_scale='Viridis_r',
                        range_color=(10, -10),
                        scope='usa',
                        labels={'pdsi': 'PDSI'}
                        )
    fig.update_layout(margin={'r': 0, 't': 0, 'l': 0, 'b': 0})
    # exportPlotlyHTML(fig, 'countyMap', 'visualizations/countyMaps/html')
    exportPlotlyPNG(fig, name, 'visualizations/countyMaps')
    # exportPlotlySVG(fig, 'countyMap2011', 'visualizations/countyMaps')


def visualizeCountiesAllYears(dfDrought):
    """Visualize Counties for all years
    loops through all years present in DataFrame and exports

    Parameters:
    dfDrought: df.DataFrame - dataframe for visualization

    Returns: None
    """
    print('generating county map PNG for all years present in data...')

    years = dfDrought['year'].unique()

    for year in years[0:5]:
        print('generating map for year {0}'.format(year))
        df = dfDrought.loc[dfDrought['year'] >= year]
        fig = px.choropleth(df, geojson=counties, locations='countyfips', color='pdsi',
                            color_continuous_scale='Viridis_r',
                            range_color=(10, -10),
                            scope='usa',
                            labels={'pdsi': 'PDSI'}
                            )
        fig.update_layout(margin={'r': 0, 't': 0, 'l': 0, 'b': 0})
        exportPlotlyPNG(fig, 'countyMap{0}'.format(
            year.astype(str)), 'visualizations/countyMaps')
        fig = None


def stackedHistogram(dfDrought):

    counties = dfDrought['countyfips'].unique()

    fig = go.Figure()

    for county in counties[0:1]:
        df = dfDrought.loc[dfDrought['countyfips'] == county]
        months = df['month'].unique()
        for month in months:
            monthChart = None
            monthChart = df.loc[df['month'] == month]
            fig.add_trace(go.Histogram(
                x=monthChart['year'], y=monthChart['pdsi']))

     # for index, row in df.iterrows():
        # fig.add_trace(go.Histogram(x=row.year, y=row.pdsi))
    # fig.add_trace(go.Histogram(x=df['countyfips'], y=df['pdsi']))

    # Overlay both histograms
    fig.update_layout(barmode='overlay')
    # Reduce opacity to see both histograms
    fig.update_traces(opacity=0.1)
    fig.show()


def knNeighbor(df: pd.DataFrame, title: str, name: str, mlTitle: str, quartile: str):
    """K nearest neighbor regression lines

    Get the weighted and unweighted average of nearest plots to get the regression lines
    """
    fig = px.scatter(
        df, x='year', y='countyAmt',
        opacity=0.65,
        trendline='lowess',
        title=mlTitle
    )
    kNn = kNearestNeighborModels(df)
    xRange = kNn.get('xRange')
    yDist = kNn.get('yDist')
    yUni = kNn.get('yUni')
    fig.add_traces(go.Scatter(x=xRange, y=yUni, name='Uniform Weight'))
    fig.add_traces(go.Scatter(x=xRange, y=yDist, name='Distance Weight'))

    fig.update_layout(xaxis_title='Year',
                      yaxis_title='Number of Counties {0} PDSI Avg'.format(quartile))

    exportPlotlyPNG(fig, name, 'visualizations/machineLearning')


def genBubbleChart(df: pd.DataFrame, years, title: str, quartile: str, name: str, mlTitle: str):
    """Generates bubble scatter plot for avg pdsi

    color - precipAvg mean of all counties present for the year
    size - pdsiAvg mean of all counties present for the year
    y - number of counties in lower pdsi quartile
    x - year
    """
    x = []
    y = []
    size = []
    color = []
    for year in years:
        currentYearData = df[df['year'] == year]
        x.append(year)
        y.append(currentYearData['pdsiAvg'].count())
        preSize = currentYearData['pdsiAvg'].sum() * -1
        if (preSize < 0):
            size.append(1)
        else:
            size.append(abs(currentYearData['pdsiAvg'].mean()))
        color.append(currentYearData['precipAvg'].mean())

    d = {'year': x, 'countyAmt': y, 'size': size,
         'color': color}
    df = pd.DataFrame(data=d)

    knNeighbor(df, title, name, mlTitle, quartile)

    fig = px.scatter(
        df,
        x='year', y='countyAmt',
        trendline='lowess',
        color='color',
        size='size',
        opacity=0.7,
        title=title,
        color_continuous_scale="Viridis_r"
    )

    fig.update_layout(coloraxis_colorbar=dict(
        title='Precipitation Avg',
        thicknessmode='pixels', thickness=20,
        lenmode='pixels', len=200,
        yanchor='top', y=1,
        ticks='inside', ticksuffix=' inches',
        dtick=0.5
    ))

    # plotly figure layout
    fig.update_layout(xaxis_title='Year',
                      yaxis_title='Number of Counties {0} PDSI Avg'.format(quartile))

    exportPlotlyPNG(fig, name, 'visualizations/bubbleCharts')


def lineChartCorr(df: pd.DataFrame, corrAvg: float, name):
    """Generate line chart from correlation coefficient data frame

    year and float
    """
    title = 'PDSI to Precipitation Correlation by Year'
    labels = ['PDSI - Precipitation Correlation Coeff']
    fig = px.scatter(df, x='year', y='corrCoeff',
                     text='year',
                     color_discrete_sequence=px.colors.qualitative.Pastel,
                     title=title,
                     trendline='ols',
                     trendline_color_override='orchid',
                     labels=labels)

    fig.add_shape(type='line',
                  line=dict(color='Blue',),
                  x0=1895,
                  y0=corrAvg,
                  x1=2016,
                  y1=corrAvg,
                  xref='x',
                  yref='y'
                  )

    fig.add_annotation(x=1925, y=corrAvg,
                       text='Correlation Average: {:0.2f}'.format(corrAvg),
                       showarrow=False,
                       yshift=10)

    fig.update_layout(xaxis_title='Year',
                      yaxis_title='PDSI precipitation Correlation Coefficient'
                      )

    fig.update_traces(mode='lines')

    exportPlotlyPNG(fig, name, 'visualizations/lineCharts')


def lineChartPrecip(df: pd.DataFrame, corrAvg: float, name):
    """Generate line chart from correlation coefficient data frame

    year and float
    """
    title = 'Monthly Precipitation Average and Median By Year'
    labels = ['Monthly Precipitation']
    fig = px.scatter(df, x='year', y='precipAvg',
                     opacity=0,
                     text='year',
                     color_discrete_sequence=px.colors.qualitative.Pastel,
                     title=title,
                     trendline='lowess',
                     trendline_color_override='orchid',
                     labels=labels)

    fig.add_traces(go.Scatter(x=df['year'], y=df['precipAvg'],
                              name='Precipitation Average'))
    fig.add_traces(go.Scatter(x=df['year'], y=df['precipMedian'],
                   name='Precipitation Median'))

    fig.update_layout(xaxis_title='Year',
                      yaxis_title='Montly Precipitation'
                      )

    fig.update_traces(mode='lines')

    exportPlotlyPNG(fig, name, 'visualizations/lineCharts')


def genCountyLowerQuartilePdsi(dfLowerQuartile: pd.DataFrame, name: str):
    """Gen county chart creates figure map of US with filled in counties based on pdsi data
    saves figure as image using export methods

    Parameters:
    dfCombined: pd.DataFrame - dataframe for visualization
    name: str - file name for export
    year: int - year for visualization

    Returns: None
    """
    df = dfLowerQuartile
    df = df.loc[df['year'] >= 2001]
    fig = px.choropleth(df, geojson=counties, locations='countyFips', color='precipAvg',
                        title='Annual PDSI Lower Quartile for 2001 - 2016',
                        color_continuous_scale='Viridis_r',
                        range_color=(0, 10),
                        scope='usa',
                        labels={'year': 'Year'}
                        )
    fig.update_layout(margin={'r': 0, 't': 50, 'l': 0, 'b': 0})

    fig.update_layout(coloraxis_colorbar=dict(
        title='Single Month Precipitation',
        thicknessmode='pixels', thickness=20,
        lenmode='pixels', len=200,
        yanchor='top', y=1,
        ticks='inside', ticksuffix=' inches',
        dtick=2
    ))
    # exportPlotlyHTML(fig, 'countyMap', 'visualizations/countyMaps/html')
    exportPlotlyPNG(fig, name, 'visualizations/countyMaps')
    # exportPlotlySVG(fig, 'countyMap2011', 'visualizations/countyMaps')


def genCountyLowerQuartilePrecip(dfLowerQuartile: pd.DataFrame, name: str):
    """Gen county chart creates figure map of US with filled in counties based on pdsi data
    saves figure as image using export methods

    Parameters:
    dfCombined: pd.DataFrame - dataframe for visualization
    name: str - file name for export
    year: int - year for visualization

    Returns: None
    """
    df = dfLowerQuartile
    df = df.loc[df['year'] >= 2001]
    fig = px.choropleth(df, geojson=counties, locations='countyFips', color='pdsiAvg',
                        title='Annual Precipitation Lower Quartile for 2001 - 2016',
                        color_continuous_scale='Viridis_r',
                        range_color=(-10, 10),
                        scope='usa',
                        labels={'year': 'Year'}
                        )
    fig.update_layout(margin={'r': 0, 't': 50, 'l': 0, 'b': 0})

    fig.update_layout(coloraxis_colorbar=dict(
        title='Single Month PDSI',
        thicknessmode='pixels', thickness=20,
        lenmode='pixels', len=200,
        yanchor='top', y=1,
        ticks='inside',
        dtick=2
    ))
    # exportPlotlyHTML(fig, 'countyMap', 'visualizations/countyMaps/html')
    exportPlotlyPNG(fig, name, 'visualizations/countyMaps')
    # exportPlotlySVG(fig, 'countyMap2011', 'visualizations/countyMaps')


def genCountyPDSICombined(dfAnnualMeans: pd.DataFrame, name: str, year: int = 1960):
    """Gen county chart creates figure map of US with filled in counties based on pdsi data
    saves figure as image using export methods

    Parameters:
    dfCombined: pd.DataFrame - dataframe for visualization
    name: str - file name for export
    year: int - year for visualization

    Returns: None
    """
    df = dfAnnualMeans.loc[dfAnnualMeans['year'] == year]
    fig = px.choropleth(df, geojson=counties, locations='countyFips', color='pdsiAvg',
                        title='Annual PDSI {0}'.format(str(year)),
                        color_continuous_scale='Viridis_r',
                        range_color=(10, -10),
                        scope='usa',
                        labels={'pdsiAvg': 'PDSI Average'}
                        )
    fig.update_layout(margin={'r': 0, 't': 50, 'l': 0, 'b': 0})
    # exportPlotlyHTML(fig, 'countyMap', 'visualizations/countyMaps/html')
    exportPlotlyPNG(fig, name, 'visualizations/countyMaps')
    # exportPlotlySVG(fig, 'countyMap2011', 'visualizations/countyMaps')


def genCountyPrecipCombined(dfAnnualMeans: pd.DataFrame, name: str, year: int = 1960):
    """Gen county chart creates figure map of US with filled in counties based on precipitation data
    saves figure as image using export methods

    Parameters:
    dfCombined: pd.DataFrame - dataframe for visualization
    name: str - file name for export
    year: int - year for visualization

    Returns: None
    """
    df = dfAnnualMeans.loc[dfAnnualMeans['year'] == year]
    fig = px.choropleth(df, geojson=counties, locations='countyFips', color='precipAvg',
                        title='Annual Precipitation {0}'.format(str(year)),
                        color_continuous_scale='Viridis_r',
                        range_color=(0, 10),
                        scope='usa',
                        labels={'precipAvg': 'Precipitation Average'}
                        )
    fig.update_layout(margin={'r': 0, 't': 50, 'l': 0, 'b': 0})
    # exportPlotlyHTML(fig, 'countyMap', 'visualizations/countyMaps/html')
    exportPlotlyPNG(fig, name, 'visualizations/countyMaps')
    # exportPlotlySVG(fig, 'countyMap2011', 'visualizations/countyMaps')
