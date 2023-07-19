# River network problem (ancestors in GraphFrames)
---

Suppose you have a network of rivers shown in the figure below.

![](https://raw.githubusercontent.com/coreyabs-db/river-network-problem/main/river-network-diagram.png)

One important question you may want to ask about the network is what are all the upstream sources that feed into a given downstream source.

In [NetworkX](https://networkx.org/documentation/stable/index.html), we have the [ancestors](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.dag.ancestors.html) function which provides this directly. But what if your data is too big for NetworkX (say, on the order of millions of nodes and edges or more)?

In that case we can turn to [Apache Spark](https://spark.apache.org/) and use [GraphFrames](https://graphframes.github.io/graphframes/docs/_site/index.html). However, GraphFrames doesn't have a direct equivalent to ancestors. Fortunately, it does have a function called [shortestPaths](https://graphframes.github.io/graphframes/docs/_site/user-guide.html#shortest-paths). This notebook shows how to implement an equivalent of ancestors to solve this problem in the river network example.

Open the notebook to see the example!

## License
---

Copyright 2023 Databricks, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
