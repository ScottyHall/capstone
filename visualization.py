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
        print('Directory "{0} was not found for exporting plot"'.format(exportDir))


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
            figure.write_image(exportDir + '/' + fileName + '.png')
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
    fig = px.scatter(df, x="year", y="pdsi", color='pdsi')
    exportPlotlySVG(fig, title)


def genScatterPltNonlin(df: pd.DataFrame, title: str):
    # df = df.loc[df['year'] == 1950]
    fig = px.scatter(df, x="year", y="pdsi",
                     trendline="ols", trendline_options=dict(log_x=True),
                     title="Log-transformed fit on linear axes")
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
                        color_continuous_scale="Viridis_r",
                        animation_frame='year',
                        range_color=(10, -10),
                        scope="usa",
                        labels={'pdsi': 'PDSI'}
                        )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
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
                        color_continuous_scale="Viridis_r",
                        range_color=(10, -10),
                        scope="usa",
                        labels={'pdsi': 'PDSI'}
                        )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
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
                        color_continuous_scale="Viridis_r",
                        range_color=(10, -10),
                        scope="usa",
                        labels={'pdsi': 'PDSI'}
                        )
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        exportPlotlyPNG(fig, 'countyMap{0}'.format(year.astype(str)), 'visualizations/countyMaps')
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
            fig.add_trace(go.Histogram(x=monthChart['year'], y=monthChart['pdsi']))

     # for index, row in df.iterrows():
        # fig.add_trace(go.Histogram(x=row.year, y=row.pdsi))
    # fig.add_trace(go.Histogram(x=df['countyfips'], y=df['pdsi']))

    # Overlay both histograms
    fig.update_layout(barmode='overlay')
    # Reduce opacity to see both histograms
    fig.update_traces(opacity=0.1)
    fig.show()