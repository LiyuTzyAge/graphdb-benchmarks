package eu.socialsensor.insert;

import com.google.common.collect.ImmutableMap;
import eu.socialsensor.graphdatabases.JanusGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.JanusGraphClient;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;

/**
 *  @author: liyu04
 *  @date: 2020/3/3
 *  @version: V1.0
 *
 * @Description:
 */
public class JanusgraphSingleInsertion  extends InsertionBase<Vertex,Object>
{
    private final JanusGraphClient client;
    public JanusgraphSingleInsertion(JanusGraphClient client, File resultsPath)
    {
        super(GraphDatabaseType.JANUSGRAPH, resultsPath);
        this.client = client;
    }

    @Override
    protected Vertex getOrCreate(String value)
    {
        Vertex v = null;
        if (StringUtils.isNotBlank(value)) {
            Integer id = Integer.valueOf(value);
            v = client.getVertex(JanusGraphDatabase.NODE, JanusGraphDatabase.NODE_ID, id);
            if (v == null) {
                v = client.replaceProperty(client.g().addV(JanusGraphDatabase.NODE), ImmutableMap.of(JanusGraphDatabase.NODE_ID, id));
            }
        }
        return v;
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest)
    {
        client.addEdge(src, JanusGraphDatabase.SIMILAR, dest);
    }
}
