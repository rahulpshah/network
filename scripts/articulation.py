from pyspark import SparkContext
from pyspark import SQLContext 
from pyspark.sql.types import *
from pyspark.sql import Row
import graphframes as gf
import csv
sc = SparkContext(appName="articulation_pts")
sqlCtx = SQLContext(sc)

def getNoOfComponents(x, g):
    vertices = g.vertices.filter("id != '"+x+"'")
    edges = g.edges.filter("src != '"+x+"' and dst != '"+x+"'")
    g_ = gf.GraphFrame(vertices, edges)
    return g_.connectedComponents().select("component").distinct().count()


def articulation_pts(g):
    lst = g.vertices.map(lambda x: x[0]).collect()
    ans = []
    for v in lst:
        if(getNoOfComponents(v, g) == 1):
            ans += [(v, 0)]
        else:
            ans += [(v, 1)]
    return sqlCtx.createDataFrame(ans, ["terrorist", "isArticulationPoint"])


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
    graph = '../9_11_edgelist.txt'
    v, e = read_graph(graph, sc)
    G = createGraphFrame(v, e)
    ddf = articulation_pts(G)
    lst = ddf.filter("isArticulationPoint = 1").select("terrorist").map(lambda x: x[0]).collect()

    with open('terrorists.txt', 'w') as thefile:
    	for item in lst:
    		thefile.write("%s\n" % (item))

if __name__ == '__main__':
    main()
