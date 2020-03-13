package eu.socialsensor.utils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.diskstorage.BackendException;

import java.util.*;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

/**
 *  @author: liyu04
 *  @date: 2020/3/3
 *  @version: V1.0
 *
 * @Description:
 */
public class JanusGraphClient
{
    protected String propFileName;
    protected Configuration conf;
    protected Graph graph;
    protected GraphTraversalSource g;
    protected boolean supportsTransactions;
    protected boolean supportsSchema;
    protected boolean supportsGeoshape;

    protected JanusGraph janusgraph;
    protected Cluster cluster;
    protected Client client;
    private static Logger logger =  LogManager.getLogger();
    /**
     * Constructs a graph app using the given properties.
     * @param fileName location of the properties file
     */
    public JanusGraphClient(final String fileName) {
        this.propFileName = fileName;
        this.supportsSchema = true;
        this.supportsTransactions = false;
        this.supportsGeoshape = false;

    }


    public GraphTraversalSource g()
    {
        return g;
    }

    public Client client()
    {
        return client;
    }

    /**
     * 迁移到HiveGraphSchema中，完成后删除此模块
     * @return
     */
    private String createSchemaRequest()
    {
        final StringBuilder s = new StringBuilder();

        s.append("JanusGraphManagement mgmt = graph.openManagement(); ");
        s.append("boolean created = false; ");

        // naive check if the schema was previously created
        s.append(
                "if (mgmt.getVertexLabel(\"hiveTree\").iterator().hasNext()) { mgmt.rollback(); created = false; } else { ");

        // properties
        s.append("PropertyKey hive_name = mgmt.makePropertyKey(\"hive_name\").dataType(String.class).make(); ");
        s.append("PropertyKey hive_type = mgmt.makePropertyKey(\"hive_type\").dataType(String.class).make(); ");
        s.append("PropertyKey hive_modification_time = mgmt.makePropertyKey(\"hive_modification_time\").dataType(Date.class).make(); ");
        s.append("PropertyKey hive_access_time = mgmt.makePropertyKey(\"hive_access_time\").dataType(Date.class).make(); ");
        s.append("PropertyKey hive_dampUpdate_time = mgmt.makePropertyKey(\"hive_dampUpdate_time\").dataType(Date.class).make(); ");
        s.append("PropertyKey hive_transient_lastDdlTime = mgmt.makePropertyKey(\"hive_transient_lastDdlTime\").dataType(Date.class).make(); ");
        s.append("PropertyKey hive_numFiles = mgmt.makePropertyKey(\"hive_numFiles\").dataType(Integer.class).make(); ");
        s.append("PropertyKey hive_numRows = mgmt.makePropertyKey(\"hive_numRows\").dataType(Long.class).make(); ");
        s.append("PropertyKey hive_path = mgmt.makePropertyKey(\"hive_path\").dataType(String.class).make(); ");
        s.append("PropertyKey hive_hashCode = mgmt.makePropertyKey(\"hive_hashCode\").dataType(Integer.class).make(); ");
        s.append("PropertyKey hive_fileBytes = mgmt.makePropertyKey(\"hive_fileBytes\").dataType(Long.class).make(); ");
        s.append("PropertyKey hive_create_time = mgmt.makePropertyKey(\"hive_create_time\").dataType(Date.class).make(); ");
        s.append("PropertyKey hive_childName = mgmt.makePropertyKey(\"hive_childName\").dataType(String.class).make(); ");
        s.append("PropertyKey isDone = mgmt.makePropertyKey(\"isDone\").dataType(Integer.class).make(); ");

        // vertex labels
        s.append("VertexLabel v_label = mgmt.makeVertexLabel(\"hiveTree\").make(); ");

        // edge labels
        s.append("EdgeLabel e_label = mgmt.makeEdgeLabel(\"hiveTree\").multiplicity(Multiplicity.SIMPLE).make(); ");

        // composite indexes
        s.append("mgmt.buildIndex(\"damp_hive_name_uniq\", Vertex.class).unique().addKey(hive_name).indexOnly(v_label).buildCompositeIndex(); ");
        s.append("mgmt.buildIndex(\"damp_hive_path\", Vertex.class).addKey(hive_path).indexOnly(v_label).buildCompositeIndex(); ");

        //edge indexes
        s.append("mgmt.buildEdgeIndex(e_label,\"damp_edge_centic_index\",Direction.BOTH,hive_childName); ");

        // mixed indexes
//        if (useMixedIndex) {
//            s.append("mgmt.buildIndex(\"vAge\", Vertex.class).addKey(age).buildMixedIndex(\"" + mixedIndexConfigName
//                    + "\"); ");
//            s.append("mgmt.buildIndex(\"eReasonPlace\", Edge.class).addKey(reason).addKey(place).buildMixedIndex(\""
//                    + mixedIndexConfigName + "\"); ");
//        }

        s.append("mgmt.commit(); created = true; }");

        return s.toString();
    }

    public GraphTraversalSource openGraph() throws ConfigurationException
    {
        logger.info("opening graph");
        conf = new PropertiesConfiguration(propFileName);

        // using the remote driver for schema
        try {
            cluster = Cluster.open(conf.getString("gremlin.remote.driver.clusterFile"));
            client = cluster.connect();
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }

        // using the remote graph for queries
        graph = EmptyGraph.instance();
        g = graph.traversal().withRemote(conf);
        return g;
    }

    public void closeGraph() throws Exception {
        logger.info("closing graph");
        try {
            if (g != null) {
                // this closes the remote, no need to close the empty graph
                g.close();
            }
            if (cluster != null) {
                // the cluster closes all of its clients
                cluster.close();
            }
        } finally {
            g = null;
            graph = null;
            client = null;
            cluster = null;
        }
    }

    /**
     * execute mulit-commands of string and waite result
     * @param req command
     * @return result
     */
    public List<Result> gremlinConsole(final String req) {
        // submit the request to the server
        final ResultSet resultSet = client.submit(req);
        // drain the results completely
        Stream<Result> futureList = resultSet.stream();
        List<Result> result = new LinkedList<>();
        futureList.forEach(result::add);
        return result;
    }

    ///////////////////////////////////////////////////////////
    public Vertex getVertex(Long vertexId) {
        if (vertexId == null)
            throw new IllegalArgumentException("Exception getVertex method (Long VertexId) is null");
        Vertex v = null;
        try {
            v = g.V(vertexId).next();
        } catch (NoSuchElementException e) {
            return null;
        }
        return v;
    }

    public Optional<Map<Object, Object>> getVertexValueMap(Vertex vertex)
    {
        return g.V(vertex).valueMap().tryNext();
    }


    public GraphTraversal<Vertex,Vertex> getVertexTraversal(String key, Object Value)
    {
        if (key == null || Value == null) throw new IllegalArgumentException("Exception getVertex method (key,value) is null");
        if (getVertex(key, Value) == null) {
            return null;
        }
        return g.V().has(key, Value);
    }

    public GraphTraversal<Vertex,Vertex> getVertexTraversal(String label,String key, Object Value)
    {
        if (key == null || Value == null) throw new IllegalArgumentException("Exception getVertex method (key,value) is null");
        if (getVertex(label,key, Value) == null) {
            return null;
        }
        return g.V().hasLabel(label).has(key, Value);
    }

    /**
     * get vertex from janusgraph by client remote
     * @param key key name
     * @param Value key value
     * @return vertex or null
     * @throws NoSuchElementException the vertex of (key,value) is not exites
     */
    public Vertex getVertex(String key, Object Value)  {
        Vertex vertex = null;
        if (key == null || Value == null) throw new IllegalArgumentException("Exception getVertex method (key,value) is null");
        try {
            vertex=g.V().has(key, Value).next();
        }catch (NoSuchElementException e){
            logger.debug("com.etl.DataDestination.JanusGraph.loader.BatchLoader : getVertex Exception : "+e.getClass()
                    + " "+e.getMessage()+" "+e.getStackTrace() + " KeyValue: "+key + " :: " + Value);
            return null;
        }
        return vertex;
    }

    public Vertex getVertex(String label,String key, Object Value)  {
        Vertex vertex = null;
        if (key == null || Value == null) throw new IllegalArgumentException("Exception getVertex method (key,value) is null");
        try {
            vertex=g.V().hasLabel(label).has(key, Value).next();
        }catch (NoSuchElementException e){
            logger.debug("com.etl.DataDestination.JanusGraph.loader.BatchLoader : getVertex Exception : "+e.getClass()
                    + " "+e.getMessage()+" "+e.getStackTrace() + " KeyValue: "+key + " :: " + Value);
            return null;
        }
        return vertex;
    }

    public Edge getEdge(String[] index, Vertex outVertex, String lable)
    {
        if(index == null || outVertex == null || lable == null) throw new IllegalArgumentException("Exception getEdge index outVertex lable is null");
        Edge edge = null;
        try {
            edge = g.V(outVertex).outE(lable).has(index[0], index[1]).limit(1).next();
        } catch (NoSuchElementException e) {
            logger.debug("com.etl.DataDestination.JanusGraph.loader.BatchLoader : getEdge Exception : "+e.getClass()
                    + " "+e.getMessage()+" "+e.getStackTrace() + " outVertex: "+outVertex.toString() + " :: " + Arrays.toString(index));
            return null;
        }
        return edge;
    }

    public GraphTraversal<Vertex,Edge> getEdgeTraversal(String[] index,Vertex outVertex,String lable)
    {
        if (getEdge(index, outVertex, lable) != null) {
            return g.V(outVertex).outE(lable).has(index[0], index[1]).limit(1);
        }
        return null;
    }

    public List<Object> getOutVertices(String key,String value,String edgeLabelString,String property)
    {
        return getVertexTraversal(key,value).out(edgeLabelString).values(property).toList();
    }

    public List<Object> getOutVerties(Vertex v,String edgdLabel,String property)
    {
        return g.V(v).out(edgdLabel).values(property).toList();
    }

    public List<Object> getOutVertices(String vertexLabel,String key,String value,String edgeLabelString,String property)
    {
        return getVertexTraversal(vertexLabel,key,value).out(edgeLabelString).values(property).toList();
    }


    /**
     * Deletes elements from the graph structure. When a vertex is deleted,
     * its incident edges are also deleted.
     */
    public void deleteVertex(Vertex vertex) {
        try {
            if (g == null) {
                return;
            }
            logger.info("deleting elements");
            // note that this will succeed whether or not pluto exists
            g.V(vertex).drop().iterate();
            if (supportsTransactions) {
                g.tx().commit();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            if (supportsTransactions) {
                g.tx().rollback();
            }
        }
    }

    public void removeTreeVertices(Vertex vertex)
    {
        //g.V(vertex.id()).repeat(out()).emit().drop();
        Traversal<Vertex, Vertex> others = g.V(vertex).repeat(out()).emit();
        List<Vertex> vertices = others.toList();
        for(int i=vertices.size()-1;i>=0;i--){
            System.out.println("--------"+vertices.get(i).id().toString());
            deleteVertex(vertices.get(i));
        }
        deleteVertex(vertex);
    }

    /**
     *  add or update the vertex
     * @param key key name
     * @param label lable of vertex
     * @param properties properties of vertex
     * @return
     */
    public Vertex addOrUpdateVertex(String key, String label, Map<String,Object> properties){
        GraphTraversal<Vertex, Vertex> traversal = getVertexTraversal(label,key,properties.get(key));
        if (traversal == null){
            traversal=g.addV(label);
        }
        return  replaceProperty(traversal, properties);
    }

    public Vertex addOrUpdateVertex(Vertex vertex, Map<String,Object> properties){
        if (null == vertex) {
            return null;
        }
        GraphTraversal<Vertex, Vertex> traversal = g.V(vertex);
        return  replaceProperty(traversal, properties);
    }


    public Edge addOrUpdateEdgeByCentricIndex(String[] index,Vertex outVertex,String lable,Vertex inVertex,Map<String,Object> properties)
    {
        GraphTraversal<Vertex, Edge> traversal;
        if (index[0] != null && index[1] != null && outVertex != null && inVertex != null) {
            traversal = getEdgeTraversal(index, outVertex, lable);
            if (traversal == null) {
                traversal = g.V(outVertex).as("a").V(inVertex).addE(lable).from("a");
                properties.put(index[0], index[1]);
            }
            return replaceProperty(traversal,properties);
        }
        return null;
    }

    public Edge addEdge(Vertex outVertex,String lable,Vertex inVertex)
    {
        return g.V(outVertex).as("a").V(inVertex).addE(lable).from("a").next();
    }

    /**
     * must commit (next()) by youself
     * @param traversal
     * @param key
     * @param value
     * @param <U>
     */
    private  <U> GraphTraversal<Vertex,U> replaceProperty(GraphTraversal<Vertex,U> traversal, String key, Object value){
        if (traversal == null) {
            return null;
        }
        if (key == null || value == null) {
            return traversal;
        }
        if (value instanceof List || value instanceof  Set) {
            Collection<?> collection = (Collection<?>) value;
            for (Object v : collection) {
                traversal.property(key, v);
            }
        } else {
            traversal.property(key, value);
        }
        return traversal;
    }

    public <U> U replaceProperty(GraphTraversal<Vertex,U> traversal, Map<String,Object> properties){
        if (traversal == null) {
            return null;
        }
        properties.forEach((key,value)->{
            replaceProperty(traversal, key, value);
        });
        return traversal.next();
    }

    /**
     * 判断schma是否已经创建,如果存在关系
     * @return
     */
    private boolean schemaHasCreate()
    {
        return schemaHasCreate("hive_name");
    }

    /**
     * 判断schma是否已经创建,如果存在关系
     * @return
     */
    private boolean schemaHasCreate(String key)
    {
        StringBuilder s = new StringBuilder();
        s.append("JanusGraphManagement mgmt = graph.openManagement(); ");
        s.append("mgmt.getPropertyKey(\""+key+"\").iterator().hasNext()");
        String command = s.toString();
        List<Result> resultList = gremlinConsole(command);
        return resultList.size() > 0 && resultList.get(0).getBoolean();
    }

    public void createSchema(String schemaRequest)
    {
        try {
//            String command = createSchemaRequest();
            List<Result> resultList = gremlinConsole(schemaRequest);
            resultList.forEach(r->{
                logger.info(r.toString());
            });
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void queryStr(String req)
    {
        List<Result> resultList = gremlinConsole(req);
    }

    public void createSchema()
    {
        createSchema(createSchemaRequest());
    }

    public double getNodeOutDegree(Vertex vertex,String edgeLabel)
    {
        return g.V(vertex).out(edgeLabel).count().next();
    }
    public Object getVertexProperty(Vertex vertex,String property)
    {
        return g.V(vertex).values(property).next();
    }

    public Object getVertexProperty(GraphTraversal<Vertex, Vertex> v, String property)
    {
        return v.values(property).next();
    }

    public String addVertexStr(String label,String key,Object value)
    {
        return "graph.addVertex(\"" + label + "\")" + ".property(\"" + key + "\"," + value + ");";
    }

    public String addVertexAndEdgeStr(String label,int srcId,int destId)
    {
        //g.V(outVertex).as("a").V(inVertex).addE(lable).from("a")
//        GraphTraversalSource g = graph.traversal();
//        g.V().hasLabel("node").has("nodeId", srcId).as("src").V().hasLabel("node").has("nodeId", destId).addE(label).from("src");
//        String req = String.format("v1 = g.V().has(\"nodeId\",%d);if(!v1.hasNext()){v1=graph.addVertex(\"node\");v1.property(\"nodeId\",%d)}else{v1=v1.next()};v2 = g.V().has(\"nodeId\",%d);if(!v2.hasNext()){v2=graph.addVertex(\"node\");v2.property(\"nodeId\",%d)}else{v2=v2.next()};v1.addEdge(\"similar\",v2);", srcId, srcId, destId, destId);
        String req = String.format("v1 = gg.V().has(\"nodeId\",%d);if(!v1.hasNext()){v1=tx.addVertex(\"node\");v1.property(\"nodeId\",%d)}else{v1=v1.next()};v2 = gg.V().has(\"nodeId\",%d);if(!v2.hasNext()){v2=tx.addVertex(\"node\");v2.property(\"nodeId\",%d)}else{v2=v2.next()};v1.addEdge(\"similar\",v2);", srcId, srcId, destId, destId);
        return req;
    }
    public static String commit()
    {
        return "tx.commit();";
    }

    public void commitRequest(List<String> reqs)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("tx = graph.buildTransaction().enableBatchLoading().start();");
        sb.append("gg = tx.traversal();");
        for (String req : reqs) {
            sb.append(req);
        }
        sb.append(commit());
        createSchema(sb.toString());
    }

    public static void main(String[] args)
    {
        JanusGraphClient batchLoader = new JanusGraphClient("conf/janus-remote.properties");
        try {
            batchLoader.openGraph();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        System.out.println(batchLoader.client.getSettings().toString());
        System.out.println(batchLoader.cluster.getMaxInProcessPerConnection());
        System.out.println(batchLoader.cluster.getMaxWaitForConnection());
        System.out.println(batchLoader.cluster.getKeepAliveInterval());
        System.exit(11);
        System.exit(11);
        batchLoader.schemaHasCreate();
        batchLoader.createSchema();
        System.exit(11);
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("name", "qqq");
        valueMap.put("age", 100);
        batchLoader.addOrUpdateVertex("name", "person", valueMap);
        try {
            batchLoader.closeGraph();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
