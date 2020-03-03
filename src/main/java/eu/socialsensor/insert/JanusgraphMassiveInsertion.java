package eu.socialsensor.insert;

import eu.socialsensor.graphdatabases.JanusGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.JanusGraphClient;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *  @author: liyu04
 *  @date: 2020/3/3
 *  @version: V1.0
 *
 * @Description:
 */
public class JanusgraphMassiveInsertion extends InsertionBase<Integer>
{
    private JanusGraphClient client;
    private ExecutorService pool = Executors.newFixedThreadPool(8);
    private Set<Integer> vertices = new HashSet<>();

    private static final int VERTEX_BATCH_NUMBER = 500;
    private static final int EDGE_BATCH_NUMBER = 500;

    private List<String> vertexList = new ArrayList<>(VERTEX_BATCH_NUMBER);
    private List<String> edgeList = new ArrayList<>(EDGE_BATCH_NUMBER);
    protected JanusgraphMassiveInsertion(JanusGraphClient client)
    {
        super(GraphDatabaseType.JANUSGRAPH, null);
        this.client = client;
    }

    @Override
    protected Integer getOrCreate(String value)
    {
        Integer v = Integer.valueOf(value);
        if (!this.vertices.contains(v)) {
            this.vertices.add(v);
            this.vertexList.add(client.addVertexStr(JanusGraphDatabase.NODE, JanusGraphDatabase.NODE_ID, v));
        }

        if (this.vertexList.size() >= VERTEX_BATCH_NUMBER) {
            batchcommitVertex();
        }
        return v;
    }

    @Override
    protected void relateNodes(Integer src, Integer dest)
    {
        this.edgeList.add(client.addEdgeStr(JanusGraphDatabase.SIMILAR,src,dest));
        if (this.edgeList.size() >= EDGE_BATCH_NUMBER) {
            batchcommitEdge();
        }
    }

    public void batchcommitVertex() {
        List<String> list = this.vertexList;
        this.vertexList = new ArrayList<>(VERTEX_BATCH_NUMBER);
        this.pool.submit(() -> {
            client.commitRequest(list);
        });
    }

    public void batchcommitEdge() {
        List<String> list = this.edgeList;
        this.edgeList = new ArrayList<>(EDGE_BATCH_NUMBER);
        this.pool.submit(() -> {
            this.client.commitRequest(list);
        });
    }

    @Override
    protected void post() {
        if (this.vertexList.size() > 0) {
            batchcommitVertex();
        }
        if (this.edgeList.size() > 0) {
            batchcommitEdge();
        }
        this.pool.shutdown();
        try {
            this.pool.awaitTermination(3, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
