import time
import warnings

import numpy as np
import matplotlib.pyplot as plt

import pandas as pd
from sklearn import cluster, datasets, mixture
from sklearn.neighbors import KNeighborsRegressor, kneighbors_graph
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.cluster import kmeans_plusplus
from sklearn.datasets import make_blobs


from itertools import cycle, islice

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
    annualQuartiles: {'q1', 'q4'}
    """
    dfLowerQuart = dfAnnualMeans[dfAnnualMeans['pdsiAvg']
                                 <= annualPdsiQuantile.index[0]]
    dfUpperQuart = dfAnnualMeans[dfAnnualMeans['pdsiAvg']
                                     >= annualPdsiQuantile.index[1]]
    # dfLowerQuart = dfLowerQuart[dfLowerQuart['precipAvg']
    #                             <= annualPrecipQuatile.index[0]]

    annualQuartiles = {'q1': dfLowerQuart, 'q4': dfUpperQuart}

    return annualQuartiles


def kNearestNeighborModels(df: pd.DataFrame):
    """K nearest neighbor regression lines

    Get the weighted and unweighted average of nearest plots to get the regression lines
    """
    X = df.year.values.reshape(-1, 1)
    x_range = np.linspace(X.min(), X.max(), 100)

    knnDist = KNeighborsRegressor(10, weights='distance')
    knnDist.fit(X, df.countyAmt)
    y_dist = knnDist.predict(x_range.reshape(-1, 1))

    knnUniform = KNeighborsRegressor(10, weights='uniform')
    knnUniform.fit(X, df.countyAmt)
    y_uni = knnUniform.predict(x_range.reshape(-1, 1))

    d = {'xRange': x_range, 'yDist': y_dist, 'yUni': y_uni}
    return d

def mlCluster(dataset: np.array):
    print('Starting Kmeans Clustering ========================')
    # X = dataset[['pdsiAvg', 'precipAvg']]
    # X = X.values

    # print(type(X))

    # Generate sample data
    n_samples = 4000
    n_components = 4

    X, y_true = make_blobs(
        n_samples=n_samples, centers=n_components, cluster_std=0.60, random_state=0
    )
    X = X[:, ::-1]

    print(X)
    print(y_true)

    # Calculate seeds from kmeans++
    centers_init, indices = kmeans_plusplus(X, n_clusters=4, random_state=0)

    # Plot init seeds along side sample data
    plt.figure(1)
    colors = ["#4EACC5", "#FF9C34", "#4E9A06", "m"]

    for k, col in enumerate(colors):
        cluster_data = y_true == k
        plt.scatter(X[cluster_data, 0], X[cluster_data, 1],
                    c=col, marker=".", s=10)

    plt.scatter(centers_init[:, 0], centers_init[:, 1], c="b", s=50)
    plt.title("K-Means++ Initialization")
    plt.xticks([])
    plt.yticks([])

    exportMatplotPNG(plt, 'machineLearning5')


def mlTester(datasets: np.ndarray):
    np.random.seed(0)

    plt.figure(figsize=(9 * 3 + 12, 5))
    plt.subplots_adjust(
        left=0.02, right=0.98, bottom=0.001, top=0.8, wspace=0.05, hspace=0.01
    )

    plot_num = 1

    default_base = {
        "quantile": 0.3,
        "eps": 0.3,
        "damping": 0.9,
        "preference": -200,
        "n_neighbors": 10,
        "n_clusters": 16,
        "min_samples": 20,
        "xi": 0.05,
        "min_cluster_size": 0.1,
    }

    datasets = [
        (datasets, {"damping": 0.75, "preference": -220, "n_clusters": 16})]

    for i_dataset, (dataset, algo_params) in enumerate(datasets):
        # update parameters with dataset-specific values
        params = default_base.copy()
        params.update(algo_params)

        X = dataset

        # normalize dataset for easier parameter selection
        X = StandardScaler().fit_transform(X)

        # ============
        # Create cluster objects
        # ============
        batchSize = int(len(X)/20)
        batchEnd = int(batchSize * 2)
        kmeans = cluster.MiniBatchKMeans(
            n_clusters=16, batch_size=batchSize, random_state=0)
        kmeans = kmeans.partial_fit(X[0:batchSize, :])
        kmeans = kmeans.partial_fit(X[batchSize:batchEnd, :])
        # print(kmeans.cluster_centers_)

        kmeans = cluster.MiniBatchKMeans(n_clusters=16).fit(X)
        # print(kmeans.cluster_centers_)

        clustering_algorithms = (
            ("MiniBatch\nKMeans", kmeans),
            # ("Affinity\nPropagation", affinity_propagation),
            # ("MeanShift", ms),
            # ("Spectral\nClustering", spectral),
            # ("Ward", ward),
            # ("Agglomerative\nClustering", average_linkage),
            # ("DBSCAN", dbscan),
            # ("OPTICS", optics),
            # ("BIRCH", birch),
            # ("Gaussian\nMixture", gmm),
        )

        for name, algorithm in clustering_algorithms:
            t0 = time.time()

            # catch warnings related to kneighbors_graph
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="the number of connected components of the "
                    + "connectivity matrix is [0-9]{1,2}"
                    + " > 1. Completing it to avoid stopping the tree early.",
                    category=UserWarning,
                )
                warnings.filterwarnings(
                    "ignore",
                    message="Graph is not fully connected, spectral embedding"
                    + " may not work as expected.",
                    category=UserWarning,
                )
                algorithm.fit(X)

            t1 = time.time()
            if hasattr(algorithm, "labels_"):
                y_pred = algorithm.labels_.astype(int)
            else:
                y_pred = algorithm.predict(X)

            plt.subplot(len(datasets), len(clustering_algorithms), plot_num)
            if i_dataset == 0:
                plt.title(name, size=18)

            colors = np.array(
                list(
                    islice(
                        cycle(
                            [
                                "#377eb8",
                                "#ff7f00",
                                "#4daf4a",
                                "#f781bf",
                                "#a65628",
                                "#984ea3",
                                "#999999",
                                "#e41a1c",
                                "#dede00",
                            ]
                        ),
                        int(max(y_pred) + 1),
                    )
                )
            )
            # add black color for outliers (if any)
            colors = np.append(colors, ["#000000"])
            plt.scatter(X[:, 0], X[:, 1], s=10, color=colors[y_pred])

            plt.xlim(-2.5, 2.5)
            plt.ylim(-2.5, 2.5)
            plt.xticks(())
            plt.yticks(())
            plt.text(
                0.99,
                0.01,
                ("%.2fs" % (t1 - t0)).lstrip("0"),
                transform=plt.gca().transAxes,
                size=15,
                horizontalalignment="right",
            )
            plot_num += 1
