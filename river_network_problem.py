# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <!----------------------------------------------------------------------
# MAGIC    Copyright 2023 Databricks
# MAGIC
# MAGIC    Licensed under the Apache License, Version 2.0 (the "License");
# MAGIC    you may not use this file except in compliance with the License.
# MAGIC    You may obtain a copy of the License at
# MAGIC
# MAGIC        http://www.apache.org/licenses/LICENSE-2.0
# MAGIC
# MAGIC    Unless required by applicable law or agreed to in writing, software
# MAGIC    distributed under the License is distributed on an "AS IS" BASIS,
# MAGIC    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# MAGIC    See the License for the specific language governing permissions and
# MAGIC    limitations under the License.
# MAGIC ---------------------------------------------------------------------->
# MAGIC
# MAGIC # River network problem (ancestors in GraphFrames)
# MAGIC
# MAGIC Suppose you have a network of rivers shown in the figure below.
# MAGIC
# MAGIC ![](https://raw.githubusercontent.com/coreyabs-db/river-network-problem/main/river-network-diagram.png)
# MAGIC
# MAGIC One important question you may want to ask about the network is what are all the upstream sources that feed into a given downstream source.
# MAGIC
# MAGIC In [NetworkX](https://networkx.org/documentation/stable/index.html), we have the [ancestors](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.dag.ancestors.html) function which provides this directly. But what if your data is too big for NetworkX (say, on the order of millions of nodes and edges or more)?
# MAGIC
# MAGIC In that case we can turn to [Apache Spark](https://spark.apache.org/) and use [GraphFrames](https://graphframes.github.io/graphframes/docs/_site/index.html). However, GraphFrames doesn't have a direct equivalent to ancestors. Fortunately, it does have a function called [shortestPaths](https://graphframes.github.io/graphframes/docs/_site/user-guide.html#shortest-paths). This notebook shows how to implement an equivalent of ancestors to solve this problem in the river network example.

# COMMAND ----------

from graphframes import GraphFrame
from pyspark.sql import functions as F

def ancestors(graph, source):
    return (
        graph.shortestPaths(landmarks=[source])
        .filter("size(distances) > 0")
        .filter(F.col("id") != source)
        .drop("distances"))

vertices = spark.range(9)
edges = (
    spark.createDataFrame([
    (0, 2), (1, 2), (3, 5), (4, 5),
    (5, 7), (6, 7), (2, 8), (7, 8)])
    .toDF("src", "dst"))

graph = GraphFrame(vertices, edges)
display(ancestors(graph, 7))

# COMMAND ----------

# MAGIC %md
# MAGIC And that's it! As you can see, if we put in node 7, it correctly returns nodes 3, 4, 5, and 6 as the upstream sources. Try it with your own example and feel free to extend it to suit your needs.
