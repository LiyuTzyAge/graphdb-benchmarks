package eu.socialsensor.insert;

import eu.socialsensor.graphdatabases.JanusGraphCoreDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.JanusGraphUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    private Set<Integer> allVertices = new HashSet<>();
    private List<Integer> vertices;
    private List<Pair<Integer, Integer>> edges;
    private static final int EDGE_BATCH_NUMBER = 500;
    private ExecutorService pool = Executors.newFixedThreadPool(8);
    public JanusGraphCoreMassiveInsertion(JanusGraph graph)
    {
        super(GraphDatabaseType.JANUSGRAPH_CORE, null);
        this.graph = graph.tx().createThreadedTx();
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
            for (Integer v : vs) {
                if (JanusGraphUtils.getVertex(this.graph, v.longValue()) == null) {
                    //TODO:待验证
                    JanusGraphUtils.addVertex(this.graph, v.longValue());
                }
            }
            Vertex source;
            Vertex target;
            for (Pair<Integer, Integer> e: es) {
                //TODO:待验证
                source = JanusGraphUtils.getVertex(this.graph,e.getLeft().longValue());
                target = JanusGraphUtils.getVertex(this.graph,e.getRight().longValue());
                source.addEdge(JanusGraphCoreDatabase.SIMILAR, target);
            }
            this.graph.tx().commit();
        });
    }



    @Override
    protected void post() {
        this.pool.shutdown();
        try {
            this.pool.awaitTermination(60 * 5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
