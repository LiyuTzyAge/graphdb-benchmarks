package eu.socialsensor.insert;

import eu.socialsensor.main.GraphDatabaseType;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 *  @author: liyu04
 *  @date: 2020/3/10
 *  @version: V1.0
 *
 * @Description:
 */
public interface Custom
{
    /**
     * read file return data instance
     * @param fileOrDir
     * @return
     */
    Iterator<Object> readLine(File fileOrDir) throws IOException;

    /**
     * write data into graph
     * @param line
     * @param insertionBase
     * @param <T>
     */
    <T,U> void writeData(Object line, InsertionBase<T,U> insertionBase);

    void createSchema(Object graph, GraphDatabaseType type);
}
