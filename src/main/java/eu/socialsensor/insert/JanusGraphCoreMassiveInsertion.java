package eu.socialsensor.insert;

import eu.socialsensor.main.GraphDatabaseType;
import org.janusgraph.core.JanusGraph;

import java.io.File;

/**
 *  @author: liyu04
 *  @date: 2020/2/18
 *  @version: V1.0
 *
 * @Description:
 */
public class JanusGraphCoreMassiveInsertion extends InsertionBase<Integer>
{
    public JanusGraphCoreMassiveInsertion(JanusGraph graph)
    {
        super(GraphDatabaseType.JANUSGRAPH_CORE, null);
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
        return null;
    }

    @Override
    protected void relateNodes(Integer src, Integer dest)
    {

    }
}
