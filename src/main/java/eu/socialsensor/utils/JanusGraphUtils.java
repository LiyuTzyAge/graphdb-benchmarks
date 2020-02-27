package eu.socialsensor.utils;

import eu.socialsensor.graphdatabases.JanusGraphCoreDatabase;
import jnr.ffi.annotations.In;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPath;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.File;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;

/**
 *  @author: liyu04
 *  @date: 2020/2/18
 *  @version: V1.0
 *
 * @Description:
 */
public class JanusGraphUtils
{
    /**
     * 创建图
     * 已测
     * @param batch 是否为批量模式
     * @param conf 配置文件路径
     * @return
     */
    public static JanusGraph createGraph(boolean batch,String conf) {
        Configuration configuration = readConf(conf);
        if (batch) {
            configuration.setProperty("storage.batch-loading", "true");
            configuration.setProperty("schema.default", "none");
        } else {
            configuration.setProperty("storage.batch-loading", "false");
        }
        configuration.setProperty("graph.set-vertex-id","true");
        return JanusGraphFactory.open(configuration);
    }

    public static Configuration readConf(String conf)
    {
        try {
            return new PropertiesConfiguration(conf);
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException(String.format("Unable to load properties file %s because %s", conf,
                    e.getMessage()));
        }

    }

    /**
     * 自定义id转换janusgraph-id，数值会发生变化
     * 已测
     * @param graph
     * @param id
     * @return
     */
    public static long toVertexId(JanusGraph graph, Long id)
    {
        long tmp = id;
        //janusgraph 顶点不许大于零
        if (tmp == 0) {
            long tt = Integer.MAX_VALUE;
            tmp = tt+1L;
        }
        return ((StandardJanusGraph) graph).getIDManager().toVertexId(tmp);
    }

    /**
     * 已测(不适用与批量模式，批量模式会自动为不存在的id分配点，因为关闭了一致性检查)
     * @param graph
     * @param id
     * @return
     */
    public static Vertex getVertex(JanusGraph graph, Long id)
    {
        Iterator<Vertex> vertices = graph.vertices(toVertexId(graph, id));
        return vertices.hasNext() ? vertices.next() : null;
    }

    /**
     * 根据自定义id，创建顶点
     * 已测
     * @param graph
     * @param id
     * @return
     */
    public static Vertex addVertex(JanusGraph graph, Long id)
    {
        Vertex v = graph.addVertex(T.label, JanusGraphCoreDatabase.NODE, T.id, toVertexId(graph, id));
        v.property(JanusGraphCoreDatabase.NODE_ID, id.intValue());
        return v;
    }

    /**
     * 删除图数据
     * 已测
     * @param graph
     * @throws BackendException
     */
    public static void dropGraph(JanusGraph graph) throws BackendException
    {
        JanusGraphFactory.drop(graph);
    }
    private static final Logger LOG = LogManager.getLogger();
    public static void shortestPath(JanusGraph graph,Vertex fromNode, Integer node)
    {
        LOG.debug("##janusgraph :shortest path {} round, (from node: {}, to node: {})",
                0, fromNode.id(), node);
        GraphTraversal<Vertex, Path> limit = graph.traversal().V(fromNode.id()).repeat(out().simplePath()).until(has("nodeId", node)).path().limit(1);

        LOG.debug("##janusgraph :{}", limit.hasNext() ? limit.next().toString() : null);
    }

    public static void main(String[] args) throws BackendException
    {
        JanusGraph graph = JanusGraphUtils.createGraph(false, "E:\\ideahouse\\hugeGraph\\benchmarks\\graphdb-benchmarks\\janusgraph.properties");
//        Vertex vertex1 = graph.vertices(256L).next();
//        shortestPath(graph, vertex1, 18944);
//        System.exit(1);
        //OLAP
//        GraphTraversalSource g = graph.traversal();
//        g.withComputer();
//        Path nodeId = g.V(256L).shortestPath().with(ShortestPath.edges, Direction.OUT).with(ShortestPath.target, has("nodeId", 2)).next();
//        System.out.println(nodeId.toString());
        Vertex v1 = getVertex(graph, 1L);
        if (null != v1) {
            System.out.println("====================v1 exists");
        }else {
            System.out.println("====================v1 not exists");
            addVertex(graph, 1L);
        }

        if (null != getVertex(graph, 1L)) {
            System.out.println("==============================v1 exists");
        }else {
            System.out.println("====================v1 not exists");
        }
        graph.tx().commit();
        graph.close();
        System.exit(1);
        dropGraph(graph);
        graph.close();
    }
}
