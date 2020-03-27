graphdb-benchmarks
==================
本工程适配了Hugegraph0.10.4、Janusgraph0.4.0版本。并在源工程[hugegraph/graphdb-benchmarks](https://github.com/hugegraph/graphdb-benchmarks)基础上，增加了以下功能：
- 测试对象：Janusgraph、Janusgraph-client
- 测试场景：Kout、Kneighbor
- 自定义数据集：CustomData
- run-linux.sh启动脚本：便于测试环境运行，程序打包后即可在测试环境运行

Package
------
**打包**：mvn clean package  
**拷贝工程依赖jar**：mvn dependency:copy-dependencies -DoutputDirectory=lib  
**jar冲突问题**：Hugegraph与Janusgraph在使用Cassandra、本地DB、client这三种场景中会存在冲突问题。以下jar列表经过测试。可用于手动排除参考。
- conf/lib-cassandra-huge-janus.txt：Hugegraph与Janusgraph使用Cassandra作为后端
- conf/lib-local-huge-janus-neo4j.txt：Hugegraph使用RockDb、Janusgraph使用BerkelyDB、Neo4j
- conf/lib-client-huge-janus.txt：用于测试客户端(kout，kneighbor,massive/single insertion)，Hugegraph-client、Janusgraph(gremlin-driver)。

Configuration
------
**性能测试软件配置文件**：input.properties
**图软件配置文件**：
Hugegraph：conf/hugegraph.properties
Janusgraph：conf/ janusgraph.properites
gremlin-driver： conf/janus-remote.properties
gremlin-driver：conf/remote-objects.yaml

Script
------
mvn本机自动测试 ：run.sh
linux服务器运行：run-linux.sh
Janusgraph-server图清理：clean-janus.sh

Result
------
结果目录：results
测试流程跟踪metric结果路径：metrics
