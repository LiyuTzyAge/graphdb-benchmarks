package eu.socialsensor.utils;

import eu.socialsensor.graphdatabases.JanusGraphCoreDatabase;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.File;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
    private static long toVertexId(JanusGraph graph, Long id)
    {
        return ((StandardJanusGraph) graph).getIDManager().toVertexId(id);
    }

    /**
     * 已测
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

    public static void main(String[] args) throws BackendException
    {
        JanusGraph graph = JanusGraphUtils.createGraph(false, "E:\\ideahouse\\hugeGraph\\benchmarks\\graphdb-benchmarks\\janusgraph.properties");
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
        dropGraph(graph);
        graph.close();
    }
}
