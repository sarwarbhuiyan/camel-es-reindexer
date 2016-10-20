# camel-es-reindexer

# Introduction

This is an example camel application which can use the camel-elasticsearch-http component to read data from one index (or index pattern) and index into another elasticsearch index. 

Performance, I managed to get higher performance on this than using logstash (es input and output) and python es reindex API. On my local virtual machine (4 cpus, 8gb heap) on my macbook pro (SSD drive), I managed to get 12k-17k documents/second reindexing speed for a corpus of Apache logs document.

# Usage

Download the release zip (that includes the uber jar with all dependencies) and run es-reindexer like follows:

To see usage instructions, run:

```
> ./es-reindexer
usage: es-reindexer
    --bulkSize <bulkSize>             Bulk Size (default: 500)
    --outputWorkers <outputWorkers>   Output Workers (default: 2)
    --preserveIDs                     Preserve IDs?
    --scrollPeriod <scrollPeriod>     Scroll Period (default: 1m)
    --sourceHost <sourceHost>         Source Elasticsearch Host
    --sourceIndex <sourceIndex>       Source Index
    --sourcePort <sourcePort>         Source Elasticsearch Port
    --targetHost <targetHost>         Target Elasticsearch Host
    --targetIndex <targetIndex>       Target Index
    --targetPort <targetPort>         Target Elasticsearch Port (Optional: if  unspecified will use original index names)

```

Example usage:

```
> ./es-reindexer --sourceHost=localhost --sourcePort=9200 --targetHost=localhost --targetPort=9200 --sourceIndex=logstash-* --targetIndex=logstash-1995-1 --outputWorkers=4 --bulkSize=1000 --scrollPeriod=5m

```
