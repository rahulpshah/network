from pyspark import SparkContext
from pyspark import SQLContext 
from pyspark.sql.types import *
from pyspark.sql import Row
import graphframes as gf
import csv
sc = SparkContext(appName="centrality")
sqlCtx = SQLContext(sc)

"""
    Constructs node and edge dataframe from the edgelist
    
    Parameters
    ----------
    g:graph

    Returns
    -------
    List of ID and centrality closeness as SparkSQL DataFrame
    """
def closeness(g):
    lst = g.vertices.map(lambda x: x[0]).collect()
    CC = []
    for v in lst:
        seq = lst.remove(v)
        results = g.shortestPaths(landmarks=seq)
        results = results.select("id", explode("distances"))
        CC += [(v,1/sum(results["distances"])]
    return sqlCtx.createDataFrame(CC, ["id", "closeness"])


def buildSchema(schemaString):
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)
    return schema


def read_graph(edgelist, sc):
    """
    Constructs node and edge dataframe from the edgelist
    
    Parameters
    ----------
    sc: SparkContext of the Spark Application
    edgelist: Name of the filename that contains the list of edges in the graph
    Takes edgelist in the form of 'u,v' format

    Returns
    -------
    Pair of vertices and edges as SparkSQL DataFrame
    """

    edges = sc.textFile(edgelist)
    edges = edges.map(lambda x: str(x).split(","))
    iedges = edges.map(lambda x: [x[1], x[0]])
    edges = edges.union(iedges)
    edges = edges.map(lambda x: Row(u=x[0], v=x[1]))

    nodes = edges.flatMap(lambda x: x).distinct()
    nodes = nodes.map(lambda x: Row(u=x))
    edges_df = sqlCtx.createDataFrame(edges, buildSchema("src dst"))
    nodes_df = sqlCtx.createDataFrame(nodes, buildSchema("id"))
    return nodes_df, edges_df

def createGraphFrame(v, e):
    return gf.GraphFrame(v, e)


def main():
    graph = '/home/aparna/Downloads/network/comp_network.txt'
    v, e = read_graph(graph, sc)
    G = createGraphFrame(v, e)
    df = closeness(G)
    df = df.sort("closeness", ascending=False)

if __name__ == '__main__':
    main()