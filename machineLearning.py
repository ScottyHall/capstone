import time
import warnings

import numpy as np
import matplotlib.pyplot as plt

import pandas as pd
from sklearn import cluster, datasets, mixture
from sklearn.neighbors import kneighbors_graph
from sklearn.preprocessing import StandardScaler
from itertools import cycle, islice

from visualization import exportMatplotPNG


def concatData(dfDrought: pd.DataFrame, dfRain: pd.DataFrame, dfStates: pd.DataFrame):
    """Concat Data method concats rainfall data to each entry to pdsi dataframe
    convert the dataframe to a numpy array for sklearn

    Parameters:
    dfDrought: pd.DataFrame - drought data
    dfRain: pd.DataFrame - rainfall data

    Returns:
    dataset: pd.DataFrame - all data merged
    """
    dfMergeRain = pd.merge(dfRain, dfStates, left_on='state_id', right_on='noaa_state_fips')

    dfMergeRain = dfMergeRain.drop(['state_id'], axis=1)
    dfMergeRain['countyfips'] = dfMergeRain['state_fips'] + dfMergeRain['county_id']

    dfMergeRain['year'] = dfMergeRain['year'].astype(int)
    dfDrought['year'] = dfDrought['year'].astype(int)

    dfMergeRainDrought = pd.merge(dfDrought, dfMergeRain, on=['year', 'countyfips'], how='left')

    return dfMergeRainDrought

 
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

    datasets = [(datasets, {"damping": 0.75, "preference": -220, "n_clusters": 16})]

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
        kmeans = cluster.MiniBatchKMeans(n_clusters=16, batch_size=batchSize, random_state=0)
        kmeans = kmeans.partial_fit(X[0:batchSize,:])
        kmeans = kmeans.partial_fit(X[batchSize:batchEnd,:])
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

    exportMatplotPNG(plt, 'machineLearning4')