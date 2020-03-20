package eu.socialsensor.insert;

import eu.socialsensor.graphdatabases.JanusGraphCoreDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.JanusGraphUtils;
import eu.socialsensor.utils.Utils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.diskstorage.BackendException;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static eu.socialsensor.graphdatabases.JanusGraphCoreDatabase.createSchema;

/**
 *  @author: liyu04
 *  @date: 2020/2/18
 *  @version: V1.0
 *
 * @Description:
 */
public class JanusGraphCoreMassiveInsertion extends InsertionBase<Vertex,Vertex>
{
    private final JanusGraph graph;
//    private final JanusGraphTransaction tx;
    private Set<Integer> allVertices = new HashSet<>();
    private Map<Integer,Vertex> allVerticesMap = new HashMap<>(1000000);
    private Map<String, Vertex> allVerticesMap2 = new HashMap<>(100000);
//    private volatile List<Integer> vertices;
    private volatile List<Pair<Vertex, Vertex>> edges;
    private volatile List<Triple<String,Pair<Vertex, Vertex>,Map<String,Object>>> edges2;
    private static final int EDGE_BATCH_NUMBER = 270;
    private ExecutorService pool = Executors.newFixedThreadPool(8);
    private static final Logger LOG = LogManager.getLogger();
    public JanusGraphCoreMassiveInsertion(JanusGraph graph)
    {
        super(GraphDatabaseType.JANUSGRAPH_CORE, null);
        this.graph = graph;
//        this.tx = graph.tx().createThreadedTx();
        reset();
        reset2();
    }

    /**
     * 最好创建点时，指定id和node_id 为同一个值
     * @param value
     *            the identifier of the vertex
     * @return
     */
    @Override
    protected Vertex getOrCreate(String value)
    {
        Vertex vertex;
        Integer v = Integer.valueOf(value.trim());
        if (!this.allVerticesMap.containsKey(v)) {
            //this.vertices.add(v);
            vertex = JanusGraphUtils.addVertex(this.graph, v.longValue());
            this.allVerticesMap.put(v, vertex);
        } else {
            vertex = allVerticesMap.get(v);
        }

        return vertex;
    }

    private void reset() {
//        this.vertices = new ArrayList<>();
        this.edges = new ArrayList<>(EDGE_BATCH_NUMBER);
    }

    private void reset2()
    {
        this.edges2 = new ArrayList<>(EDGE_BATCH_NUMBER);
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest)
    {
        this.edges.add(Pair.of(src, dest));
        if (this.edges.size() >= EDGE_BATCH_NUMBER) {
            this.batchCommit();
            this.reset();
        }
    }

    private void batchCommit() {
        //有时数据无法写入，可能是外面线程写入顶点的缓存没有提交
        this.graph.tx().commit();
//        List<Integer> vs = this.vertices;
        List<Pair<Vertex, Vertex>> es = this.edges;

        this.pool.submit(() -> {
            try {
//                for (Integer v : vs) {
//                    if (v < 20) {
//                        System.out.println("====commmit===" + v);
//                    }
//                    //批量模式无法使用getVertex，一致性检查被关闭
//                    JanusGraphUtils.addVertex(this.graph, v.longValue());
//                }
//                Vertex source;
//                Vertex target;
                for (Pair<Vertex, Vertex> e : es) {
                    //并发情况下可能 vertex还未写入，产生NullPointException,如果写入够快则不会
                    //可以先写点再写边
//                    source = JanusGraphUtils.getVertex(this.graph, e.getLeft().longValue());
//                    target = JanusGraphUtils.getVertex(this.graph, e.getRight().longValue());
//                    source.addEdge(JanusGraphCoreDatabase.SIMILAR, target);
                    e.getLeft().addEdge(JanusGraphCoreDatabase.SIMILAR, e.getRight());
                }
                this.graph.tx().commit();
            } catch (Exception e) {
                LOG.error("massive insert error",e);
            }
        });
    }
    private Vertex getVertex(JanusGraphTransaction tx,Long id)
    {
        return tx.getVertex(JanusGraphUtils.toVertexId(graph, id));
//        return vertices.hasNext() ? vertices.next() : null;
    }

    private Vertex addVertex( JanusGraphTransaction tx,Long id)
    {
        Vertex v = tx.addVertex(T.label, JanusGraphCoreDatabase.NODE, T.id, JanusGraphUtils.toVertexId(graph, id));
        v.property(JanusGraphCoreDatabase.NODE_ID, id.intValue());
        return v;
    }

    @Override
    protected void post() {
        batchCommit2();
        reset2();
        batchCommit();
        reset();
        this.pool.shutdown();
        try {
            this.pool.awaitTermination(60 * 5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    public void post2()
    {
        post();
    }


    @Override
    public Vertex getOrCreateCust(String label, @Nullable String id, Map<String,Object> properties)
    {
        Vertex vertex;
        if (Objects.isNull(id)) {
            return JanusGraphUtils.addVertex(this.graph, label, properties);
        }
        if (!this.allVerticesMap2.containsKey(id)) {
            //this.vertices.add(v);
            vertex = JanusGraphUtils.addVertex(this.graph, label, properties);
            this.allVerticesMap2.put(id, vertex);
        } else {
            vertex = allVerticesMap2.get(id);
        }
        return vertex;
    }

    public void relateNodesCust(String label,final Vertex src, final Vertex dest,Map<String,Object> properties)
    {
        this.edges2.add(Triple.of(label, Pair.of(src, dest), properties));
        if (this.edges2.size() >= EDGE_BATCH_NUMBER) {
            this.batchCommit2();
            this.reset2();
        }
    }

    private ConcurrentHashSet<String> edgeCache = new ConcurrentHashSet<>();
    private void batchCommit2() {
        //有时数据无法写入，可能是外面线程写入顶点的缓存没有提交
        this.graph.tx().commit();
//        List<Integer> vs = this.vertices;
        List<Triple<String,Pair<Vertex, Vertex>,Map<String,Object>>> es = this.edges2;
        this.pool.submit(() -> {
            try {
                for (Triple<String,Pair<Vertex, Vertex>,Map<String,Object>> e : es) {
                    JanusGraphVertex src = (JanusGraphVertex)e.getMiddle().getLeft();
                    JanusGraphVertex target = (JanusGraphVertex)e.getMiddle().getRight();
                    String key = src.id() + ">" + target.id();
                    if (!edgeCache.contains(key)) {
                        src.addEdge(e.getLeft(),target , Utils.mapTopair(e.getRight()));
                        edgeCache.add(key);
                    }
                }
                this.graph.tx().commit();
            } catch (Exception e) {
                LOG.error("massive insert error",e);
            }
        });
    }


    public static void main(String[] args) throws BackendException
    {
        JanusGraph graph = JanusGraphUtils.createGraph(false, "E:\\ideahouse\\hugeGraph\\benchmarks\\graphdb-benchmarks\\janusgraph.properties");
        JanusGraphCoreMassiveInsertion testGraph = new JanusGraphCoreMassiveInsertion(graph);
        createSchema(graph);
        Random r = new Random();
        Random r2 = new Random(1000000);
        Integer a = Math.abs(r.nextInt());
//        JanusGraphVertex vertex = testGraph.tx.getVertex(JanusGraphUtils.toVertexId(graph, a.longValue()));
//        System.out.println(vertex);
//        Iterator<Vertex> vertices = graph.vertices(JanusGraphUtils.toVertexId(graph, a.longValue()));
//        System.out.println("========"+vertices.next());
//        JanusGraphUtils.addVertex(graph, a.longValue());
//        JanusGraphUtils.addVertex(graph,279331075584L);
//        System.out.println("========"+JanusGraphUtils.getVertex(graph,a.longValue()));
        System.out.println("========"+JanusGraphUtils.getVertex(graph,279331075584L));
        graph.tx().commit();
//        JanusGraphUtils.dropGraph(graph);
        System.exit(1);
        for (int i = 0; i < 1000; i++) {
            int v_src = c_v_id(r);
            int v_dest = c_v_id(r2);
            if (v_dest == v_src) {
                continue;
            }
            testGraph.getOrCreate(String.valueOf(v_src));
            testGraph.getOrCreate(String.valueOf(v_dest));
//            testGraph.relateNodes(v_src,v_dest);
        }
        testGraph.post();
        System.out.println("end");
    }

    private static int c_v_id(Random random)
    {
        return random.nextInt(100000);
    }
}
