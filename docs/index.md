# Ahghee - The Big Graph Database

[![Build status](https://ci.appveyor.com/api/projects/status/6581it232hdo2qa5?svg=true)](https://ci.appveyor.com/project/Astn/ahghee)

This project is still in it's early early stages, so click that *Watch* button.

## Design goals

- Massive graphs (Trillions of nodes)
- Write friendly (like Cassandra)
- Elastic scaling
- Masterless clustering
- Adaptive topology layout
- Fast (Millions of graph-node steps per second per server)
- Tinkerpop or a variation of Tinkerpop
- Cypher or a variation of Cypher
- Index-free adjancecy traversal
- Custom indexing
- Automatic adaptive indexing
- Storage local compute
- Large value support
- Standing queries 
- Virtual sub-graph
- Pluggable storage providers
- Pluggable query providers
- Dotnet core embedding
- Cross platform

## Approach
- TDD
- Functional Programming
- DevOps

### High level strategy
- Etcd for cluster registry
- Use a Log structured merge approach (like rocksdb)
- Cluster-nodes form a network where they only talk to a few other cluster-nodes
- Metrics about which cluster-nodes ultimatly receive data from other cluster-nodes used modify the network
- NeuralNetwork node+query classification used for balancing graph-nodes on the network
- A new cluster-node should be able to join the cluster just by authenticating with any cluster-node
- Gateway nodes should be able to join multiple clusters to form a WAN cluster
- Gateway nodes can control the flow of data between clusters (read/write/one-way)
