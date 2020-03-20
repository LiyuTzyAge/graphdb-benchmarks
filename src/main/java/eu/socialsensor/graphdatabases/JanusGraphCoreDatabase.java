package eu.socialsensor.graphdatabases;

import eu.socialsensor.insert.*;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.utils.JanusGraphUtils;
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
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.util.JanusGraphCleanup;
import org.janusgraph.diskstorage.BackendException;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static eu.socialsensor.utils.JanusGraphUtils.*;
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
    public static final String NODE = "node";

    public JanusGraphCoreDatabase(BenchmarkConfiguration config, File dbStorageDirectory)
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
//        return graph.vertices(i).next();
        return JanusGraphUtils.getVertex(this.graph, i.longValue());
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
        return v.edges(Direction.BOTH);
//        return v.edges(Direction.BOTH, SIMILAR);
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
     *    FN, FA, FS and CW
     */
    @Override
    public void open()
    {
        buildGraphEnv(false,false);
    }

    private void open(boolean batch)
    {
        this.graph = JanusGraphUtils.createGraph(batch,this.conf);
        createTransaction(batch);
    }

    private void createTransaction(boolean batch) {
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
        buildGraphEnv(false,true);
    }

    @Override
    public void massiveModeLoading(File dataPath)
    {
        JanusGraphCoreMassiveInsertion ji = new JanusGraphCoreMassiveInsertion(this.graph);
        ji.createGraph(dataPath,0);
    }

    @Override
    public void massiveModeLoading(File dataPath, CustomData customData)
    {
        JanusGraphCoreMassiveInsertion ji = new JanusGraphCoreMassiveInsertion(this.graph);
        customData.createGraph(dataPath, ji, 0);
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
        buildGraphEnv(true,true);
        //TODO: 测试下批量配置的效果
    }

    @Override
    public void createGraphForCustom(Custom custom)
    {
        open(true);
        clear(true);
        open(true);
        custom.createSchema(this.graph.openManagement(), GraphDatabaseType.JANUSGRAPH_CORE);
    }

    @Override
    public void shutdown()
    {
        //参照hugegraph
        if (this.graph == null) {
            return;
        }
        close();
    }

    @Override
    public void delete()
    {
//        open();
//        try {
//            JanusGraphFactory.drop(graph);
//        } catch (BackendException e) {
//            LOG.warn("##janusgraph drop faild ", e.getMessage());
//        }
//        Utils.deleteRecursively(dbStorageDirectory);
    }

    @Override
    public void shutdownMassiveGraph()
    {
        //参照hugegraph
        if (this.graph == null) {
            return;
        }
        close();
    }

    @Override
    public void shortestPath(Vertex fromNode, Integer node)
    {
        LOG.debug("##janusgraph :shortest path {} round, (from node: {}, to node: {})",
                counter, fromNode.id(), node);
        counter++;
//        Path path = g.V(fromNode.id()).repeat(out().simplePath()).until(has(NODE_ID, node)).path().limit(1).next();
        //"g.V(%s).repeat(out().simplePath())" +
        //                                     ".until(hasId(%s).or().loops().is(gte(3)" +
        //                                     ")).hasId(%s).path().limit(1)"
        Path path = graph.traversal().V(fromNode.id()).repeat(out().simplePath()).times(5).emit(has(NODE_ID, node)).path().limit(1).next();
        LOG.debug("##janusgraph :{}", path.toString());
    }

    @Override
    public long kout(int k, int node)
    {
        return graph.traversal().V().has(NODE_ID, node).repeat(out()).times(k).count().next();
    }

    @Override
    public long kneighbor(int k, int node)
    {
        return 0;
    }

    @Override
    public int getNodeCount()
    {
        return graph.traversal().V().count().next().intValue();
    }

    /**
     *
     * @param nodeId properties
     * @return
     */
    @Override
    public Set<Integer> getNeighborsIds(int nodeId)
    {
        Set<Integer> neighbors = new HashSet<>();
        GraphTraversal<Vertex, Vertex> outs = graph.traversal().V().has(NODE_ID, nodeId).out(SIMILAR);
        while (outs.hasNext()) {
            Integer neighborId = outs.next().value(NODE_ID);
            neighbors.add(neighborId);
        }
        return neighbors;
    }

    /**
     *
     * @param nodeId properties
     * @return
     */
    @Override
    public double getNodeWeight(int nodeId)
    {
        return this.getNodeDegree(nodeId, Direction.OUT);
    }

    /**
     *
     * @param id properties
     * @param direction
     * @return
     */
    private long getNodeDegree(Object id, Direction direction) {
        switch (direction) {
            case IN:
                return this.graph.traversal().V().has(NODE_ID,id).inE(SIMILAR).count().next();
            case OUT:
                return this.graph.traversal().V().has(NODE_ID,id).outE(SIMILAR).count().next();
            case BOTH:
            default:
                throw new AssertionError(String.format(
                        "Only support IN or OUT, but got: '%s'", direction));
        }
    }
    private long getNodeDegree2(Object id, Direction direction) {
        switch (direction) {
            case IN:
                return this.graph.traversal().V(id).inE(SIMILAR).count().next();
            case OUT:
                return this.graph.traversal().V(id).outE(SIMILAR).count().next();
            case BOTH:
            default:
                throw new AssertionError(String.format(
                        "Only support IN or OUT, but got: '%s'", direction));
        }
    }
    private void gCommit()
    {
        this.graph.tx().commit();
//        createTransaction(false);
    }

    @Override
    public void initCommunityProperty()
    {
        LOG.debug("Init community property");

        int communityCounter = 0;
        Iterator<Vertex> vertices = this.graph.traversal().V();
        while (vertices.hasNext()) {
            Vertex v = vertices.next();
            v.property(NODE_COMMUNITY, communityCounter);
            v.property(COMMUNITY, communityCounter);
            communityCounter++;
//            if (communityCounter % (BATCH_SIZE / 2) == 0) {
//                gCommit();
//            }
        }

        LOG.debug("Initial community number is: {}", communityCounter);
        gCommit();
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities)
    {
        // NOTE: this method won't be called (LouvainMethod.updateBestCommunity)
        Set<Integer> communities = new HashSet<>();
        Iterator<Vertex> vertices = this.graph.traversal().V().has(NODE_COMMUNITY, nodeCommunities);
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

    /**
     *
     * @param community
     * @return properties
     */
    @Override
    public Set<Integer> getNodesFromCommunity(int community)
    {
        Set<Integer> nodes = new HashSet<>();
        Iterator<Vertex> vertices = this.graph.traversal().V().has(COMMUNITY, community);
        while (vertices.hasNext()) {
            nodes.add(((Number) vertices.next().value(NODE_ID)).intValue());
        }
        return nodes;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity)
    {
        Set<Integer> nodes = new HashSet<>();
        Iterator<Vertex> vertices = this.graph.traversal().V().has(NODE_COMMUNITY,
                nodeCommunity);
        while (vertices.hasNext()) {
            nodes.add(((Number) vertices.next().value(NODE_ID)).intValue());
        }
        return nodes;
    }

    /**
     * zongdian
     * @param nodeCommunity
     * @param communityNodes
     * @return
     */
    @Override
    public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes)
    {
        // NOTE: this method won't be called due to Cache
        long edges = 0L;
        //update 0226
        Set<Integer> commVertices = this.graph.traversal().V().has(COMMUNITY, communityNodes).values(NODE_ID).toStream().map(id->((Number)id).intValue()).collect(Collectors.toSet());
        Iterator<Vertex> vertices = this.graph.traversal().V().has(NODE_COMMUNITY,
                nodeCommunity);
        while (vertices.hasNext()) {
            Iterator<Edge> neighbors = this.graph.traversal().V(vertices.next().id())
                    .outE(SIMILAR);
            while (neighbors.hasNext()) {
                Edge edge = neighbors.next();
                Integer nb = ((Number) edge.outVertex().value(NODE_ID)).intValue();
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
        Iterator<Vertex> vertices = this.graph.traversal().V().has(COMMUNITY, community);
        while (vertices.hasNext()) {
            communityWeight += getNodeDegree2(vertices.next().id(),Direction.OUT);
        }
        return communityWeight;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity)
    {
        // NOTE: this method won't be called due to Cache
        long nodeCommunityWeight = 0L;
        Iterator<Vertex> vertices = this.graph.traversal().V().has(NODE_COMMUNITY,
                nodeCommunity);
        while (vertices.hasNext()) {
            nodeCommunityWeight += getNodeDegree2(vertices.next().id(),Direction.OUT);
        }
        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int from, int to)
    {
        Iterator<Vertex> vertices = this.graph.traversal().V().has(NODE_COMMUNITY, from);
        int count = 0;
        while (vertices.hasNext()) {
            vertices.next().property(COMMUNITY, to);
//            if (++count % BATCH_SIZE == 0) {
//                gCommit();
//            }
        }
        gCommit();
    }

    @Override
    public double getGraphWeightSum()
    {
        return this.graph.traversal().E().count().next();
    }

    @Override
    public int reInitializeCommunities()
    {
        LOG.debug("ReInitialize communities");

        Map<Integer, Integer> initCommunities = new HashMap<>();
        int communityCounter = 0;
        int count = 0;
        Iterator<Vertex> vertices = this.graph.traversal().V();
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
//            if (++count % (BATCH_SIZE / 2) == 0) {
//                gCommit();
//            }
        }
        gCommit();

        LOG.debug("Community number is now: {}", communityCounter);
        return communityCounter;
    }

    /**
     *
     * @param nodeId properties
     * @return
     */
    @Override
    public int getCommunityFromNode(int nodeId)
    {
        Vertex vertex = this.graph.traversal().V().has(NODE_ID,nodeId).limit(1).next();
        return vertex.value(COMMUNITY);
    }

    @Override
    public int getCommunity(int nodeCommunity)
    {
        Vertex vertex = this.graph.traversal().V()
                .has(NODE_COMMUNITY, nodeCommunity)
                .limit(1).next();
        return vertex.value(COMMUNITY);
    }

    @Override
    public int getCommunitySize(int community)
    {
        // NOTE: this method won't be called due to Cache
        Set<Integer> nodeCommunities = new HashSet<>();
        Iterator<Vertex> vertices = this.graph.traversal().V().has(COMMUNITY, community);
        while (vertices.hasNext()) {
            Vertex v = vertices.next();
            int nodeCommunity = v.value(NODE_COMMUNITY);
            nodeCommunities.add(nodeCommunity);
        }
        return nodeCommunities.size();
    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities)
    {
        Map<Integer, List<Integer>> communities = new HashMap<>();
        for (int i = 0; i < numberOfCommunities; i++) {
            List<Integer> ids = new ArrayList<>();
            Iterator<Vertex> vertices = this.graph.traversal().V().has(COMMUNITY, i);
            while (vertices.hasNext()) {
//                ids.add(((Number) vertices.next().id()).intValue());
                ids.add(((Number) vertices.next().value(NODE_ID)).intValue());
            }
            communities.put(i, ids);
        }
        return communities;
    }

    /**
     *
     * @param nodeId vertexId
     * @return
     */
    @Override
    public boolean nodeExists(int nodeId)
    {
        //TODO:待验证
        return this.graph.vertices(nodeId).hasNext();
    }

    private void clear(boolean clear)
    {
        if (clear && graph != null) {
            try {
                JanusGraphUtils.dropGraph(this.graph);
            } catch (BackendException e) {
                LOG.warn("##janusgraph drop faild ", e.getMessage());
            }
        }
    }

    private void buildGraphEnv(boolean batch,boolean clear)
    {
        open(batch);
        clear(clear);
        open(batch);
//        graph.tx().rollback();
        //create schema
        createSchema(this.graph);
    }

    public static void createSchema(JanusGraph graph)
    {
        JanusGraphManagement mgmt = graph.openManagement();
        VertexLabel vertexLabel = null;
        EdgeLabel edgeLabel = null;

        if (!mgmt.containsVertexLabel(NODE)) {
            vertexLabel = mgmt.makeVertexLabel(NODE).make();
        }
        if (!mgmt.containsEdgeLabel(SIMILAR)) {
            edgeLabel = mgmt.makeEdgeLabel(SIMILAR).multiplicity(Multiplicity.MULTI).make();
        }
        PropertyKey nodeid = getOrCreatePropertyKey(mgmt, NODE_ID, Integer.class, Cardinality.SINGLE);
        buildVertexCompositeIndex(mgmt, NODE_ID, true, null, nodeid);
        PropertyKey community = getOrCreatePropertyKey(mgmt, COMMUNITY, Integer.class, Cardinality.SINGLE);
        buildVertexCompositeIndex(mgmt, COMMUNITY, false, null, community);
        PropertyKey node_community = getOrCreatePropertyKey(mgmt, NODE_COMMUNITY, Integer.class, Cardinality.SINGLE);
        buildVertexCompositeIndex(mgmt, NODE_COMMUNITY, false, null, node_community);
        mgmt.commit();
    }


    public void close(){
        if (transaction != null) transaction.close();
        if(graph != null) graph.close();
    }

    public JanusGraph getGraph()
    {

        return graph;
    }
    public JanusGraph openGraph()
    {
        open(true);
        clear(true);
        open(true);
        return graph;
    }
}
