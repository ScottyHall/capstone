import json
import pandas as pd
import plotly.figure_factory as ff
import plotly
import plotly.express as px
import os

with open('sourceData/geo.json') as f:
    counties = json.load(f)


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
            'Directory "{0} was not found for exporting plot"'.format(
                exportDir)
        )


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


def generateScatterPlot(df):
    df = df.head(10)
    fig = px.scatter(df, x="year", y="pdsi", color='pdsi')
    exportPlotlySVG(fig, 'test3')


def genScatterPltNonlin(df: pd.DataFrame):
    df = df.loc[df['year'] == 1950]
    fig = px.scatter(df, x="year", y="pdsi",
                     trendline="ols", trendline_options=dict(log_x=True),
                     title="Log-transformed fit on linear axes")
    exportPlotlySVG(fig, 'scatterPlot1950')


def genCountyChart(dfDrought):
    """Gen county chart creates figure map of US with filled in counties
    saves figure as image using export methods

    Parameters:
    dfDrought: df.DataFrame - dataframe for visualization

    Returns: None
    """
    df = dfDrought.loc[dfDrought['year'] >= 1960]
    # df = dfDrought
    print(df)
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
