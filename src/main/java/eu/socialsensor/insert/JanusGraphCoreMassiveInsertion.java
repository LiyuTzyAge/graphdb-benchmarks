package eu.socialsensor.insert;

import eu.socialsensor.graphdatabases.JanusGraphCoreDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.JanusGraphUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.diskstorage.BackendException;

import java.util.*;
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
public class JanusGraphCoreMassiveInsertion extends InsertionBase<Integer>
{
    private final JanusGraph graph;
//    private final JanusGraphTransaction tx;
    private Set<Integer> allVertices = new HashSet<>();
    private volatile List<Integer> vertices;
    private volatile List<Pair<Integer, Integer>> edges;
    private static final int EDGE_BATCH_NUMBER = 500;
    private ExecutorService pool = Executors.newFixedThreadPool(8);
    private static final Logger LOG = LogManager.getLogger();
    public JanusGraphCoreMassiveInsertion(JanusGraph graph)
    {
        super(GraphDatabaseType.JANUSGRAPH_CORE, null);
        this.graph = graph;
//        this.tx = graph.tx().createThreadedTx();
        reset();
    }

    /**
     * 最好创建点时，指定id和node_id 为同一个值
     * @param value
     *            the identifier of the vertex
     * @return
     */
    @Override
    protected Integer getOrCreate(String value)
    {
        Integer v = Integer.valueOf(value.trim());
        if (!this.allVertices.contains(v)) {
            this.allVertices.add(v);
            this.vertices.add(v);
        }
        return v;
    }

    private void reset() {
        this.vertices = new ArrayList<>();
        this.edges = new ArrayList<>(EDGE_BATCH_NUMBER);
    }

    @Override
    protected void relateNodes(Integer src, Integer dest)
    {
        this.edges.add(Pair.of(src, dest));
        if (this.edges.size() >= EDGE_BATCH_NUMBER) {
            this.batchCommit();
            this.reset();
        }
    }

    private void batchCommit() {
        List<Integer> vs = this.vertices;
        List<Pair<Integer, Integer>> es = this.edges;

        this.pool.submit(() -> {
            try {
//            int count = 0;
//            int ecount = 0;
                for (Integer v : vs) {

//                if (JanusGraphUtils.getVertex(this.graph,v.longValue()) == null) {
//                    JanusGraphUtils.addVertex(this.graph,v.longValue());
//                    count++;
//                }
                    if (v < 20) {
                        System.out.println("====commmit===" + v);
                    }
                    //批量模式无法使用getVertex，一致性检查被关闭
                    JanusGraphUtils.addVertex(this.graph, v.longValue());
//                count++;
                }
                Vertex source;
                Vertex target;
                for (Pair<Integer, Integer> e : es) {
                    //TODO:待验证
                    source = JanusGraphUtils.getVertex(this.graph, e.getLeft().longValue());
                    target = JanusGraphUtils.getVertex(this.graph, e.getRight().longValue());
                    source.addEdge(JanusGraphCoreDatabase.SIMILAR, target);
//                ecount++;
                }
//            long vcount = this.graph.traversal().V().count().next();
//            System.out.println("==debug==:v_count:" + count + "e_count:" + ecount+"all vertex:"+vcount);
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
        batchCommit();
        reset();
        this.pool.shutdown();
        try {
            this.pool.awaitTermination(60 * 5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
            testGraph.relateNodes(v_src,v_dest);
        }
        testGraph.post();
        System.out.println("end");
    }

    private static int c_v_id(Random random)
    {
        return random.nextInt(100000);
    }
}
