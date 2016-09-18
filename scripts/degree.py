from pyspark import SparkContext
from pyspark import SQLContext 
from pyspark.sql.types import *
from pyspark.sql import Row
import graphframes as gf


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


def degree_dist(graph):
    """
    Constructs the degree distribution of a graph. The function takes GraphFrame as input and returns 
    a Spark DataFrame with 2 columns: degree and count
    
    Parameters
    ----------
    graph: GraphFrame object

    Returns
    -------
    degree_dist_df: SparkSQL DataFrame of the degree distribution
    """
    degrees = graph.degrees
    degree_rdd = degrees.rdd
    degree_count = degree_rdd.map(lambda x: (x[1], 1))

    degree_dist = degree_count.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
    degree_dist_df = sqlCtx.createDataFrame(degree_dist.map(lambda x: Row(x[0], x[1])), ["degree","count"])
    return degree_dist_df


def main():
    sc = SparkContext(appName="Degree")
    v, e = read_graph("../9_11_edgelist.txt", sc)
    g = createGraphFrame(v, e)


if __name__ == '__main__':
    main()
