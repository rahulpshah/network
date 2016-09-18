from pyspark import SparkContext
from pyspark import SQLContext 
from pyspark.sql.types import *
from pyspark.sql import Row
import graphframes as gf
import matplotlib.pyplot as plt
import networkx as nx

sc = SparkContext(appName="rahul")
sqlCtx = SQLContext(sc)


def createGraphfromNX():
    G1 = nx.erdos_renyi_graph(1000, 0.5)
    G2 = nx.powerlaw_cluster_graph(1000,500, 0.5)
    G3 = nx.barabasi_albert_graph(1000, 500)
    nx.write_edgelist(G1,"erdos.graph.large",delimiter=",", data=False)
    nx.write_edgelist(G2,"powerlaw.graph.large",delimiter=",", data=False)
    nx.write_edgelist(G3,"barabasi.graph.large",delimiter=",", data=False)


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

def power_law(ddf, name):

    lst = ddf.sort("degree").select("count").map(lambda x: x[0]).collect()
    s = sum(lst)
    lst = map(lambda x: (1.0*x)/s, lst)
    fig = plt.figure()
    plt.plot(lst)
    fig.suptitle('Power Test', fontsize=20)
    plt.xlabel('Degree', fontsize=18)
    plt.ylabel('P(Degree)', fontsize=16)
    fig.savefig(name+'.png')

def createGraphFrame(v, e):
    return gf.GraphFrame(v, e)


def degreedist(graph):
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
    createGraphfromNX()
    graphs = ["../stanford_graphs/youtube","../stanford_graphs/amazon","../stanford_graphs/dblp"]
    graphs += ['erdos','powerlaw','barabasi']
    for graph in graphs:
        v, e = read_graph(graph+".graph.large", sc)
        g = createGraphFrame(v, e)
        ddf = degreedist(g)
        power_law(ddf, graph)


if __name__ == '__main__':
    main()
