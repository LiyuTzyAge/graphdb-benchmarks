package eu.socialsensor.insert;

import eu.socialsensor.graphdatabases.JanusGraphCoreDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.JanusGraphUtils;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;

import java.io.File;

/**
 *  @author: liyu04
 *  @date: 2020/2/18
 *  @version: V1.0
 *
 * @Description:
 */
public class JanusGraphCoreSingleInsertion  extends InsertionBase<Vertex>
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
        Integer id = Integer.valueOf(value.trim());
        Vertex vertex = JanusGraphUtils.getVertex(this.graph, id.longValue());
        if (null == vertex) {
            vertex = JanusGraphUtils.addVertex(this.graph, id.longValue());
        }
        return vertex;
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest)
    {
        src.addEdge(JanusGraphCoreDatabase.SIMILAR, dest);
        this.graph.tx().commit();
    }
}
