package eu.socialsensor.graphdatabases;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import eu.socialsensor.insert.*;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.JanusGraphClient;
import eu.socialsensor.utils.JanusGraphUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 *  @author: liyu04
 *  @date: 2020/3/3
 *  @version: V1.0
 *
 * @Description:
 */
public class JanusGraphDatabase extends GraphDatabaseBase<Iterator<Vertex>, Iterator<Edge>, Vertex, Edge>
{
    public static final String NODE = "node";
    private static final int LIMIT = 100000;
    private final String serverConf;
    private final String conf;
    private JanusGraphClient client;
    private static final Logger LOG = LogManager.getLogger();

    public JanusGraphDatabase(BenchmarkConfiguration config, File dbStorageDirectory)
    {
        super(GraphDatabaseType.JANUSGRAPH, dbStorageDirectory);
        conf = config.getJanusgraphClientConf();
        serverConf = config.getJanusgraphConf();
    }

    //for test
    private JanusGraphDatabase()
    {
        super(GraphDatabaseType.JANUSGRAPH, new File("E://test"));
        serverConf = "E:/ideahouse/hugeGraph/benchmarks/graphdb-benchmarks/janusgraph.properties";
        conf = "E:/ideahouse/hugeGraph/benchmarks/graphdb-benchmarks/janus-remote.properties";
    }

    public static void main(String[] args) throws ConfigurationException
    {
//        String inputPath = "E:\\ideahouse\\hugeGraph\\benchmarks\\graphdb-benchmarks\\input.properties";
//        PropertiesConfiguration inputConf = new PropertiesConfiguration(new File(inputPath));
//        BenchmarkConfiguration conf = new BenchmarkConfiguration(inputConf);
//        JanusGraphDatabase database = new JanusGraphDatabase(conf, null);

        JanusGraphDatabase janus = new JanusGraphDatabase();
        janus.createGraphForMassiveLoad();
        janus.massiveModeLoading(new File("E:\\test\\Email-Enron.txt"));
//        janus.createGraphForSingleLoad();
//        janus.singleModeLoading(new File("E:\\test\\Email-Enron.txt"),new File("E:\\test\\"),1);
        Vertex vertex = janus.getVertex(0);

//        long kout = janus.kout(3, 0);
//        long kneighbor = janus.kneighbor(3, 0);
//        System.out.println("==============" + vertex);
//        System.out.println("==============kout" + kout);
//        System.out.println("==============keighbor" + kneighbor);
        janus.shutdown();
        System.out.println("=======================");
    }

    private void buildGraphEnv(boolean clear)
    {
        if (clear) {
            //无效
//            clear();
        }
        client = new JanusGraphClient(conf);
        try {
            client.openGraph();
        } catch (ConfigurationException e) {
            LOG.error("connect to janusgraph faild ", e);
            throw new RuntimeException("connect to janusgraph faild ");
        }
        createSchema(this.client);
    }

    public void createSchema(JanusGraphClient client)
    {
        client.createSchema(createSchemaRequest());
    }

    private String createSchemaRequest()
    {
        final StringBuilder s = new StringBuilder();

        s.append("JanusGraphManagement mgmt = graph.openManagement(); ");
        s.append("boolean created = false; ");
        s.append("if (mgmt.getVertexLabel(\"" + NODE + "\").iterator().hasNext()) { mgmt.rollback(); created = false; } else { ");
        s.append("VertexLabel v_label = mgmt.makeVertexLabel(\"" + NODE + "\").make(); ");
        s.append("EdgeLabel e_label = mgmt.makeEdgeLabel(\"" + SIMILAR + "\").multiplicity(Multiplicity.SIMPLE).make(); ");
        s.append("PropertyKey nodeid = mgmt.makePropertyKey(\"" + NODE_ID + "\").dataType(Integer.class).make(); ");
        s.append("PropertyKey community = mgmt.makePropertyKey(\"" + COMMUNITY + "\").dataType(Integer.class).make(); ");
        s.append("PropertyKey node_community = mgmt.makePropertyKey(\"" + NODE_COMMUNITY + "\").dataType(Integer.class).make(); ");
        s.append("mgmt.buildIndex(\"benchmark_node_uniq\", Vertex.class).unique().addKey(nodeid).indexOnly(v_label).buildCompositeIndex(); ");
        s.append("mgmt.buildIndex(\"benchmark_community_uniq\", Vertex.class).addKey(community).indexOnly(v_label).buildCompositeIndex(); ");
        s.append("mgmt.buildIndex(\"benchmark_node_community_uniq\", Vertex.class).addKey(node_community).indexOnly(v_label).buildCompositeIndex(); ");
        s.append("mgmt.commit(); created = true; }");
        return s.toString();
    }

    @Override
    public void open()
    {
        buildGraphEnv(false);
    }

    /**
     * 使用server模式清理，客户端没有清理功能
     */
    private void clear()
    {
        try {
            JanusGraphUtils.dropGraph(JanusGraphUtils.createGraph(false, serverConf));
        } catch (Exception e) {
            LOG.warn("##janusgraph drop faild ", e.getMessage());
        }

    }

    @Override
    public void createGraphForSingleLoad()
    {
        buildGraphEnv(true);
    }

    @Override
    public void createGraphForSingleLoad(Custom custom)
    {
        throw new RuntimeException("not support !");
    }

    @Override
    public void massiveModeLoading(File dataPath)
    {
        JanusgraphMassiveInsertion insertion =
                new JanusgraphMassiveInsertion(this.client);
        insertion.createGraph(dataPath, 0);
    }

    @Override
    public void massiveModeLoading(File dataPath, CustomData customData)
    {
        JanusgraphMassiveInsertion insertion =
                new JanusgraphMassiveInsertion(this.client);
        customData.createGraph(dataPath, insertion, 0);
    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber)
    {
        JanusgraphSingleInsertion insertion =
                new JanusgraphSingleInsertion(
                this.client, resultsPath);
        insertion.createGraph(dataPath, scenarioNumber);
    }

    @Override
    public void singleModeLoading(File dataPath, CustomData customData, File resultsPath, int scenarioNumber)
    {
        throw new RuntimeException("not support !");
    }

    @Override
    public void createGraphForMassiveLoad()
    {
        buildGraphEnv(true);
    }

    @Override
    public void shutdown()
    {
        if (null == client) {
            return;
        }
        try {
            client.closeGraph();
        } catch (Exception e) {
            LOG.error("close janusgraph client error ", e);
        }
    }

    @Override
    public void delete()
    {
        //
    }

    @Override
    public void shutdownMassiveGraph()
    {
        shutdown();
    }

    //以下查询待测
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
    public Vertex getVertex(Integer id)
    {
        //通过索引查询
        return client.getVertex(NODE, NODE_ID, id);
    }

    @Override
    public Iterator<Edge> getAllEdges()
    {
        //不要支持
        return null;
    }

    @Override
    public Iterator<Edge> getNeighborsOfVertex(Vertex v)
    {
        //不要支持
        return client.g().V(v).bothE(SIMILAR).next(LIMIT).iterator();
    }

    @Override
    public boolean edgeIteratorHasNext(Iterator<Edge> it)
    {
        //不要支持
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
        return client.g().V().hasLabel(NODE).toSet().iterator();
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


    @Override
    public void shortestPath(Vertex fromNode, Integer node)
    {
        //后续调研实现
    }

    /**
     * @param k
     * @param node
     * @return
     */
    @Override
    public long kout(int k, int node)
    {
        return client.getVertexTraversal(NODE, NODE_ID, node).repeat(out(SIMILAR)).times(k).count().next();
    }

    @Override
    public long kneighbor(int k, int node)
    {
//        BulkSet value = client.getVertexTraversal(NODE,NODE_ID,node).emit().repeat(bothE(SIMILAR).dedup().store("edges").otherV()).times(k).dedup().aggregate("vertices").bothE().where(without("edges")).as("edge").otherV().where(within("vertices")).select("edge").store("edges").cap("vertices").size();
//        BulkSet bs = (BulkSet) value;
        String query = String.format("g.V().has(\"nodeId\",%s).emit().repeat(bothE(\"%s\").dedup().store(\"edges\").otherV()).times(%d).dedup().aggregate(\"vertices\").bothE().where(without(\"edges\")).as(\"edge\").otherV().where(within(\"vertices\")).select(\"edge\").store(\"edges\").cap(\"vertices\").next().size()",
                node, SIMILAR, k);
        List<Result> resultList = client.gremlinConsole(query);
        System.out.println("======="+resultList);
        return resultList.get(0).getLong();
    }

    @Override
    public int getNodeCount()
    {
        return client.g().V().hasLabel(NODE).count().next().intValue();
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId)
    {
        Set<Integer> neighbors = new HashSet<>();
        List<Vertex> vertices = client.getVertexTraversal(NODE, NODE_ID, nodeId).out(SIMILAR).toList();
        for (Vertex v : vertices) {
            neighbors.add(v.value(NODE_ID));
        }
        return neighbors;
    }

    @Override
    public double getNodeWeight(int nodeId)
    {
        // NOTE: this method won't be called
        return client.getVertexTraversal(NODE, NODE_ID, nodeId).out(SIMILAR).count().next();
    }

    @Override
    public void initCommunityProperty()
    {
        LOG.debug("Init community property");
        int communityCounter = 0;
        Iterator<Vertex> vertexIterator = getVertexIterator();
        Map<String, Object> properties = new HashMap<>();
        while (vertexIterator.hasNext()) {
            properties.put(NODE_COMMUNITY, communityCounter);
            properties.put(COMMUNITY, communityCounter);
            client.addOrUpdateVertex(vertexIterator.next(), properties);
            communityCounter++;
            properties.clear();
        }
        LOG.debug("Initial community number is: " + communityCounter);
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities)
    {
        Set<Integer> communities = new HashSet<>();
        List<Vertex> vertices = client.g().V().hasLabel(NODE).has(NODE_COMMUNITY, nodeCommunities).toList();
        for (Vertex v : vertices) {
            for (Object community : client.getOutVerties(v, SIMILAR, COMMUNITY)) {
                communities.add((Integer) community);
            }
        }
        return communities;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community)
    {
        Set<Integer> nodes = new HashSet<>();
        for (Object v : client.getVertexTraversal(NODE, COMMUNITY, community).values(NODE_ID).toList()) {
            nodes.add((Integer) v);
        }
        return nodes;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity)
    {
        Set<Integer> nodes = new HashSet<>();
        for (Object v : client.getVertexTraversal(NODE, NODE_COMMUNITY, nodeCommunity).values(NODE_ID).toList()) {
            nodes.add((Integer) v);
        }
        return nodes;
    }

    @Override
    public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes)
    {
        double edges = 0;
        Iterable<Vertex> vertices = client.getVertexTraversal(NODE, NODE_COMMUNITY,
                nodeCommunity).toList();
        Iterable<Vertex> comVertices = client.getVertexTraversal(NODE, COMMUNITY,
                communityNodes).toList();
        for (Vertex vertex : vertices) {
            for (Vertex v : client.g().V(vertex).out(SIMILAR).toList()) {
                if (Iterables.contains(comVertices, v)) {
                    edges++;
                }
            }
        }
        return edges;
    }

    @Override
    public double getCommunityWeight(int community)
    {
        double communityWeight = 0;
        List<Vertex> vertices = client.getVertexTraversal(NODE, COMMUNITY, community).toList();
        if (vertices.size() > 1) {
            for (Vertex vertex : vertices) {
                communityWeight += client.getNodeOutDegree(vertex, SIMILAR);
            }
        }
        return communityWeight;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity)
    {
        double nodeCommunityWeight = 0;
        for (Vertex v : client.getVertexTraversal(NODE, NODE_COMMUNITY, nodeCommunity).toList()) {
            nodeCommunityWeight += client.getNodeOutDegree(v, SIMILAR);
        }
        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int from, int to)
    {
        Iterable<Vertex> vertices = client.getVertexTraversal(NODE, NODE_COMMUNITY, from).toList();
        for (Vertex v : vertices) {
            client.addOrUpdateVertex(v, ImmutableMap.of(COMMUNITY, to));
        }
    }

    @Override
    public double getGraphWeightSum()
    {
        return client.g().E().count().next();
    }

    @Override
    public int reInitializeCommunities()
    {
        LOG.debug("ReInitialize communities");
        Map<Integer, Integer> initCommunities = new HashMap<>();
        int communityCounter = 0;
        for (Vertex v : client.g().V().hasLabel(NODE).toList()) {
            int communityId = (int) client.getVertexProperty(v, COMMUNITY);
            if (!initCommunities.containsKey(communityId)) {
                initCommunities.put(communityId, communityCounter);
                communityCounter++;
            }
            int newCommunityId = initCommunities.get(communityId);
            client.addOrUpdateVertex(v, ImmutableMap.of(COMMUNITY, newCommunityId, NODE_COMMUNITY, newCommunityId));
        }
        LOG.debug("Community number is: " + communityCounter + " now");
        return communityCounter;
    }

    @Override
    public int getCommunityFromNode(int nodeId)
    {
        return (Integer) client.getVertexProperty(client.getVertex(NODE, NODE_ID, nodeId), COMMUNITY);
    }

    @Override
    public int getCommunity(int nodeCommunity)
    {
        return (Integer) client.getVertexProperty(client.getVertexTraversal(NODE, NODE_COMMUNITY, nodeCommunity).limit(1), COMMUNITY);
    }

    @Override
    public int getCommunitySize(int community)
    {
        Set<Integer> nodeCommunities = new HashSet<>();
        for (Object nc : client.getVertexTraversal(NODE, COMMUNITY, community).values(NODE_COMMUNITY).toList()) {
            int nodeCommunity = (Integer) nc;
            nodeCommunities.add(nodeCommunity);
        }
        return nodeCommunities.size();
    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities)
    {
        Map<Integer, List<Integer>> communities = new HashMap<>();
        for (int i = 0; i < numberOfCommunities; i++) {
//            List<Integer> vertices = new ArrayList<>();
            List<Integer> vertices = client.getVertexTraversal(NODE, COMMUNITY, i).values(NODE_ID).toStream().map(id -> (Integer) id).collect(Collectors.toList());
//            for (Vertex v : getVerticesByProperty(COMMUNITY, i)) {
//                vertices.add((Integer) v.id());
//            }
            communities.put(i, vertices);
        }
        return communities;
    }

    @Override
    public boolean nodeExists(int nodeId)
    {
        return client.getVertexTraversal(NODE, NODE_ID, nodeId).hasNext();
    }
}
