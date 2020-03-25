package eu.socialsensor.insert;

import com.google.common.collect.ImmutableMap;
import eu.socialsensor.graphdatabases.JanusGraphCoreDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.JanusGraphUtils;
import eu.socialsensor.utils.TaiShiDataUtils;
import eu.socialsensor.utils.Utils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 *  @author: liyu04
 *  @date: 2020/2/18
 *  @version: V1.0
 *
 * @Description:
 */
public class JanusGraphCoreSingleInsertion  extends InsertionBase<Vertex,Vertex>
{
    private final JanusGraph graph;
    public JanusGraphCoreSingleInsertion(JanusGraph graph, File resultsPath)
    {
        super(GraphDatabaseType.JANUSGRAPH_CORE, resultsPath);
        this.graph = graph;
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
        try {
            Integer id = Integer.valueOf(value.trim());
            Vertex vertex = JanusGraphUtils.getVertex(this.graph, id.longValue());
            if (null == vertex) {
                vertex = JanusGraphUtils.addVertex(this.graph, id.longValue());
            }
            return vertex;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest)
    {
        try {
            src.addEdge(JanusGraphCoreDatabase.SIMILAR, dest);
            this.graph.tx().commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
    custom dataset
     */

    public Vertex getOrCreateCust(String label, @Nullable String id, Map<String, Object> properties)
    {
        Vertex v = null;
        String keyName = TaiShiDataUtils.keyName(label);
        if(Objects.nonNull(id)){
            //get or create
            v = JanusGraphUtils.getVertexAutoId(this.graph,label, keyName,id);
        }
        if (Objects.nonNull(v)) {
            return v;
        }
        try {
            if (Objects.isNull(id)) {
                //add vertex dirct
                return JanusGraphUtils.addVertex(this.graph, label, properties);
            } else {
                //add vertex
                Map<String, Object> p =
                                properties.containsKey(keyName)?
                                properties:
                                ImmutableMap.<String, Object>builder().putAll(properties).put(keyName, id).build();
                return JanusGraphUtils.addVertex(this.graph, label, p);
            }
        }finally {
            this.graph.tx().commit();
        }
    }

    public  void relateNodesCust(String label,final Vertex src, final Vertex dest,Map<String,Object> properties)
    {
        //get or create : edge is single
        Iterator<Edge> edges = src.edges(Direction.OUT, label);
        if (!edgeHasExist(edges,dest)) {
            src.addEdge(label, dest, Utils.mapTopair(properties));
            this.graph.tx().commit();
        }
    }

    private static boolean edgeHasExist(Iterator<Edge> edges,final  Vertex dest)
    {
        if (!edges.hasNext()) {
            return false;
        }
        while (edges.hasNext()) {
            Edge e = edges.next();
            if (e.inVertex().id().equals(dest.id())) {
                return true;
            }
        }
        return false;
    }
}
