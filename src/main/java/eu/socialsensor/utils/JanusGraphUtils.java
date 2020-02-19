package eu.socialsensor.utils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.io.File;
import java.nio.file.Files;

/**
 *  @author: liyu04
 *  @date: 2020/2/18
 *  @version: V1.0
 *
 * @Description:
 */
public class JanusGraphUtils
{
    /**
     * 创建图
     * @param batch 是否为批量模式
     * @param conf 配置文件路径
     * @return
     */
    public static JanusGraph createGraph(boolean batch,String conf) {
        Configuration configuration = readConf(conf);
        if (batch) {
            configuration.setProperty("storage.batch-loading", "true");
            configuration.setProperty("schema.default", "none");
        } else {
            configuration.setProperty("storage.batch-loading", "false");
        }
        return JanusGraphFactory.open(configuration);
    }

    public static Configuration readConf(String conf)
    {
        try {
            return new PropertiesConfiguration(conf);
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException(String.format("Unable to load properties file %s because %s", conf,
                    e.getMessage()));
        }

    }
}
