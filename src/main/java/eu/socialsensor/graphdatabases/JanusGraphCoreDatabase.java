package eu.socialsensor.graphdatabases;

import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe;
import eu.socialsensor.insert.JanusGraphCoreMassiveInsertion;
import eu.socialsensor.insert.JanusGraphCoreSingleInsertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.utils.JanusGraphUtils;
import eu.socialsensor.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import eu.socialsensor.main.GraphDatabaseType;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;

/**
 *  @author: liyu04
 *  @date: 2020/2/18
 *  @version: V1.0
 *
 * @Description:
 */
public class JanusGraphCoreDatabase extends GraphDatabaseBase<Iterator<Vertex>, Iterator<Edge>, Vertex, Edge>
{
    private static final Logger LOG = LogManager.getLogger();
    //存储配置文件
    private final String conf ;
    private JanusGraph graph;
    public GraphTraversalSource g;
    private volatile JanusGraphTransaction transaction = null;
    private static int counter = 0;
    private static final int BATCH_SIZE = 500;

    protected JanusGraphCoreDatabase(BenchmarkConfiguration config, File dbStorageDirectory)
    {
        super(GraphDatabaseType.JANUSGRAPH_CORE, dbStorageDirectory);
        conf = config.getJanusgraphConf();
    }

    @Override
    public Vertex getOtherVertexFromEdge(Edge edge, Vertex oneVertex)
    {
        return edge.inVertex().equals(oneVertex) ? edge.outVertex() : edge.inVertex();
    }

    @Override
    public Vertex getSrcVertexFromEdge(Edge edge)
    {
        return edge.inVertex();
    }

    @Override
    public Vertex getDestVertexFromEdge(Edge edge)
    {
        return edge.outVertex();
    }

    @Override
    public Vertex getVertex(Integer i)
    {
        //通过id查询，需要测试验证 //TODO : 待验证
        return graph.vertices(i).next();
        //通过索引查询
//        return graph.traversal().V().has(NODE_ID, i).next();
    }

    @Override
    public Iterator<Edge> getAllEdges()
    {
        return graph.edges();
    }

    @Override
    public Iterator<Edge> getNeighborsOfVertex(Vertex v)
    {
        return v.edges(Direction.BOTH, SIMILAR);
    }

    @Override
    public boolean edgeIteratorHasNext(Iterator<Edge> it)
    {
        return it.hasNext();
    }

    @Override
    public Edge nextEdge(Iterator<Edge> it)
    {
        return it.next();
    }

    @Override
    public void cleanupEdgeIterator(Iterator<Edge> it)
    {
        CloseableIterator.closeIterator(it);
    }

    @Override
    public Iterator<Vertex> getVertexIterator()
    {
        return graph.vertices();
    }

    @Override
    public boolean vertexIteratorHasNext(Iterator<Vertex> it)
    {
        return it.hasNext();
    }

    @Override
    public Vertex nextVertex(Iterator<Vertex> it)
    {
        return it.next();
    }

    @Override
    public void cleanupVertexIterator(Iterator<Vertex> it)
    {
        CloseableIterator.closeIterator(it);
    }

    /**
     * 创建普通图，用于查询测试
     */
    @Override
    public void open()
    {
        open(false);
    }

    private void open(boolean batch)
    {
        this.graph = JanusGraphUtils.createGraph(batch,this.conf);
        createTransaction(batch);
    }

    public void createTransaction(boolean batch) {
        if (transaction == null) {
            if (batch) {
                //批量模式
                transaction = graph.buildTransaction().enableBatchLoading().start();
            } else {
                //事务模式
                transaction = graph.newTransaction();
            }
            g = transaction.traversal();
        }
    }

    @Override
    public void createGraphForSingleLoad()
    {
        open();
    }

    @Override
    public void massiveModeLoading(File dataPath)
    {
        JanusGraphCoreMassiveInsertion ji = new JanusGraphCoreMassiveInsertion(this.graph);
        ji.createGraph(dataPath,0);
    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber)
    {
        JanusGraphCoreSingleInsertion si = new JanusGraphCoreSingleInsertion(this.graph,resultsPath);
        si.createGraph(dataPath,scenarioNumber);
    }

    @Override
    public void createGraphForMassiveLoad()
    {
        open(true);
    }

    @Override
    public void shutdown()
    {
        //参照hugegraph
//        if (this.graph == null) {
//            return;
//        }
//        this.graph.close();
    }

    @Override
    public void delete()
    {
        Utils.deleteRecursively(dbStorageDirectory);
    }

    @Override
    public void shutdownMassiveGraph()
    {
        //参照hugegraph
//        if (this.graph == null) {
//            return;
//        }
//        this.graph.close();
    }

    @Override
    public void shortestPath(Vertex fromNode, Integer node)
    {
        LOG.debug("##janusgraph :shortest path {} round, (from node: {}, to node: {})",
                counter, fromNode.id(), node);
        counter++;
        GraphTraversal<Vertex, Path> limit = g.V(fromNode.id()).repeat(out().simplePath()).until(has(NODE_ID, node)).path().limit(1);
        LOG.debug("##janusgraph :{}", limit.toString());
    }

    @Override
    public int getNodeCount()
    {
        return g.V().count().next().intValue();
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId)
    {
        Set<Integer> neighbors = new HashSet<>();
        GraphTraversal<Vertex, Vertex> outs = g.V().has(NODE_ID, nodeId).out(SIMILAR);
        while (outs.hasNext()) {
            Integer neighborId = outs.next().value(NODE_ID);
            neighbors.add(neighborId);
        }
        return neighbors;
    }

    @Override
    public double getNodeWeight(int nodeId)
    {
        return this.getNodeDegree(getVertex(nodeId).id(), Direction.OUT);
    }

    private long getNodeDegree(Object id, Direction direction) {
        switch (direction) {
            case IN:
                return this.g.V(id).inE(SIMILAR).count().next();
            case OUT:
                return this.g.V(id).outE(SIMILAR).count().next();
            case BOTH:
            default:
                throw new AssertionError(String.format(
                        "Only support IN or OUT, but got: '%s'", direction));
        }
    }

    @Override
    public void initCommunityProperty()
    {
        LOG.debug("Init community property");

        int communityCounter = 0;
        Iterator<Vertex> vertices = this.g.V();
        while (vertices.hasNext()) {
            Vertex v = vertices.next();
            v.property(NODE_COMMUNITY, communityCounter);
            v.property(COMMUNITY, communityCounter);
            communityCounter++;
            if (communityCounter % (BATCH_SIZE / 2) == 0) {
                this.graph.tx().commit();
            }
        }

        LOG.debug("Initial community number is: {}", communityCounter);
        this.graph.tx().commit();
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities)
    {
        // NOTE: this method won't be called (LouvainMethod.updateBestCommunity)
        Set<Integer> communities = new HashSet<>();
        Iterator<Vertex> vertices = this.g.V().has(NODE_COMMUNITY, nodeCommunities);
        while (vertices.hasNext()) {
            Vertex v = vertices.next();
            Iterator<Vertex> neighbors = v.vertices(Direction.OUT, SIMILAR);
            while (neighbors.hasNext()) {
                Vertex neighbor = neighbors.next();
                Integer community = neighbor.value(COMMUNITY);
                communities.add(community);
            }
        }
        return communities;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community)
    {
        Set<Integer> nodes = new HashSet<>();
        Iterator<Vertex> vertices = this.g.V().has(COMMUNITY, community);
        while (vertices.hasNext()) {
            nodes.add(((Number) vertices.next().id()).intValue());
        }
        return nodes;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity)
    {
        Set<Integer> nodes = new HashSet<>();
        Iterator<Vertex> vertices = this.g.V().has(NODE_COMMUNITY,
                nodeCommunity);
        while (vertices.hasNext()) {
            nodes.add(((Number) vertices.next().id()).intValue());
        }
        return nodes;
    }

    @Override
    public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes)
    {
        // NOTE: this method won't be called due to Cache
        long edges = 0L;
        Set<Integer> commVertices = this.g.V().has(COMMUNITY, communityNodes).values(NODE_ID).toStream().map(id->(Integer)id).collect(Collectors.toSet());
        Iterator<Vertex> vertices = this.g.V().has(NODE_COMMUNITY,
                nodeCommunity);
        while (vertices.hasNext()) {
            Iterator<Edge> neighbors = this.g.V(vertices.next().id())
                    .outE(SIMILAR);
            while (neighbors.hasNext()) {
                Edge edge = neighbors.next();
                Integer nb = ((Number) edge.outVertex().id()).intValue();
                if (commVertices.contains(nb)) {
                    edges++;
                }
            }
        }
        return edges;
    }

    @Override
    public double getCommunityWeight(int community)
    {
        long communityWeight = 0L;
        Iterator<Vertex> vertices = this.g.V().has(COMMUNITY, community);
        while (vertices.hasNext()) {
            communityWeight += getNodeDegree(vertices.next(),Direction.OUT);
        }
        return communityWeight;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity)
    {
        // NOTE: this method won't be called due to Cache
        long nodeCommunityWeight = 0L;
        Iterator<Vertex> vertices = this.g.V().has(NODE_COMMUNITY,
                nodeCommunity);
        while (vertices.hasNext()) {
            nodeCommunityWeight += getNodeDegree(vertices.next(),Direction.OUT);
        }
        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int from, int to)
    {
        Iterator<Vertex> vertices = this.g.V().has(NODE_COMMUNITY, from);
        int count = 0;
        while (vertices.hasNext()) {
            vertices.next().property(COMMUNITY, to);
            if (++count % BATCH_SIZE == 0) {
                this.graph.tx().commit();
            }
        }
        this.graph.tx().commit();
    }

    @Override
    public double getGraphWeightSum()
    {
        return this.g.E().count().next();
    }

    @Override
    public int reInitializeCommunities()
    {
        LOG.debug("ReInitialize communities");

        Map<Integer, Integer> initCommunities = new HashMap<>();
        int communityCounter = 0;
        int count = 0;
        Iterator<Vertex> vertices = this.g.V();
        while (vertices.hasNext()) {
            Vertex v = vertices.next();
            Integer communityId = v.value(COMMUNITY);
            if (!initCommunities.containsKey(communityId)) {
                initCommunities.put(communityId, communityCounter);
                communityCounter++;
            }
            Integer newCommunityId = initCommunities.get(communityId);
            v.property(COMMUNITY, newCommunityId);
            v.property(NODE_COMMUNITY, newCommunityId);
            if (++count % (BATCH_SIZE / 2) == 0) {
                this.graph.tx().commit();
            }
        }
        this.graph.tx().commit();

        LOG.debug("Community number is now: {}", communityCounter);
        return communityCounter;
    }

    @Override
    public int getCommunityFromNode(int nodeId)
    {
        //TODO:待验证
        Vertex vertex = this.graph.vertices(nodeId).next();
        return vertex.value(COMMUNITY);
    }

    @Override
    public int getCommunity(int nodeCommunity)
    {
        Vertex vertex = this.g.V()
                .has(NODE_COMMUNITY, nodeCommunity)
                .limit(1).next();
        return vertex.value(COMMUNITY);
    }

    @Override
    public int getCommunitySize(int community)
    {
        // NOTE: this method won't be called due to Cache
        Set<Integer> nodeCommunities = new HashSet<>();
        Iterator<Vertex> vertices = this.g.V().has(COMMUNITY, community);
        while (vertices.hasNext()) {
            Vertex v = vertices.next();
            int nodeCommunity = v.value(NODE_COMMUNITY);
            if (!nodeCommunities.contains(nodeCommunity)) {
                nodeCommunities.add(nodeCommunity);
            }
        }
        return nodeCommunities.size();
    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities)
    {
        Map<Integer, List<Integer>> communities = new HashMap<>();
        for (int i = 0; i < numberOfCommunities; i++) {
            List<Integer> ids = new ArrayList<>();
            Iterator<Vertex> vertices = this.g.V().has(COMMUNITY, i);
            while (vertices.hasNext()) {
                ids.add(((Number) vertices.next().id()).intValue());
            }
            communities.put(i, ids);
        }
        return communities;
    }

    @Override
    public boolean nodeExists(int nodeId)
    {
        //TODO:待验证
        return this.graph.vertices(nodeId).hasNext();
    }
///////////////////////待续
//    private void createSchema()
//    {
//        final TitanManagement mgmt = g.getManagementSystem();
//        if (!titanGraph.getIndexedKeys(Vertex.class).contains(NODE_ID))
//        {
//            final PropertyKey key = mgmt.makePropertyKey(NODE_ID).dataType(Integer.class).make();
//            mgmt.buildIndex(NODE_ID, Vertex.class).addKey(key).unique().buildCompositeIndex();
//        }
//        if (!titanGraph.getIndexedKeys(Vertex.class).contains(COMMUNITY))
//        {
//            final PropertyKey key = mgmt.makePropertyKey(COMMUNITY).dataType(Integer.class).make();
//            mgmt.buildIndex(COMMUNITY, Vertex.class).addKey(key).buildCompositeIndex();
//        }
//        if (!titanGraph.getIndexedKeys(Vertex.class).contains(NODE_COMMUNITY))
//        {
//            final PropertyKey key = mgmt.makePropertyKey(NODE_COMMUNITY).dataType(Integer.class).make();
//            mgmt.buildIndex(NODE_COMMUNITY, Vertex.class).addKey(key).buildCompositeIndex();
//        }
//
//        if (mgmt.getEdgeLabel(SIMILAR) == null)
//        {
//            mgmt.makeEdgeLabel(SIMILAR).multiplicity(Multiplicity.MULTI).directed().make();
//        }
//        mgmt.commit();
//    }
}
