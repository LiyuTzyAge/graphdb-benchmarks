package eu.socialsensor.insert;

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
public interface Custom<E>
{
    /**
     * read file return data instance
     * @param fileOrDir
     * @return
     */
    Iterator<E> readLine(File fileOrDir) throws IOException;

    /**
     * write data into graph
     * @param line
     * @param insertionBase
     * @param <T>
     */
    <T,U> void writeData(E line, InsertionBase<T,U> insertionBase);
}
