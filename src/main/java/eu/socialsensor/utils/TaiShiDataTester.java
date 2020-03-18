package eu.socialsensor.utils;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.collect.ImmutableList;
import eu.socialsensor.graphdatabases.HugeGraphCoreDatabase;
import eu.socialsensor.graphdatabases.JanusGraphCoreDatabase;
import eu.socialsensor.insert.*;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.parboiled.common.Preconditions;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 *  @author: liyu04
 *  @date: 2020/3/16
 *  @version: V1.0
 *
 * @Description:
 */
public class TaiShiDataTester
{
    private TaiShiDataUtils taishi = new TaiShiDataUtils();
    public static final MetricRegistry metrics = new MetricRegistry();
    String input = "E:/ideahouse/hugeGraph/benchmarks/graphdb-benchmarks/input.properties";
    String storeFile = "E:/test/store";
    //Dir
    String dir = "E:\\360\\graph研究\\benchmarks";
    //file
    String file = "taishi_dataset_test1.txt";
//    HugeGraphCoreDatabase hugeCore;
//    JanusGraphCoreDatabase janus;

    private void createSchema(Object graph,GraphDatabaseType type)
    {
        taishi.createSchema(graph, type);
    }

    private BenchmarkConfiguration benchmarkConf()
    {
        BenchmarkConfiguration config = null;
        File store = new File(storeFile);

        try {
            Configuration appconfig = new PropertiesConfiguration(new File(input));
            config = new BenchmarkConfiguration(appconfig);
            if(config.publishCsvMetrics()) {
                final CsvReporter reporter = CsvReporter.forRegistry(metrics)
                        .formatFor(Locale.US)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .build(config.getCsvDir());
                reporter.start(config.getCsvReportingInterval(), TimeUnit.MILLISECONDS);
            }
            if(config.publishGraphiteMetrics()) {
                final Graphite graphite = new Graphite(new InetSocketAddress(config.getGraphiteHostname(), 80 /*port*/));
                final GraphiteReporter reporter = GraphiteReporter.forRegistry(metrics)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .filter(MetricFilter.ALL)
                        .build(graphite);
                reporter.start(config.getGraphiteReportingInterval(), TimeUnit.MILLISECONDS);
            }
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        return config;
    }


    private void readLine()
    {
        //Dir
        String dir = "E:\\360\\graph研究\\benchmarks";
        //file
        String file = "taishi_dataset_test1.txt";
        File d = new File(dir);
        File f = new File(dir + File.separator + file);

        checkReadLine(d);
        checkReadLine(f);
        try {
            checkTaiShiSer(taishi.readLine(d));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void checkReadLine(File file)
    {
        try {
            Iterator<Object> dirIter = taishi.readLine(file);
            ImmutableList<Object> objects = ImmutableList.copyOf(dirIter);
            assert objects.size() == 3 : objects;
            for (Object object : objects) {
                assert Objects.nonNull(object) : object;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void checkTaiShiSer(Iterator<Object> iterator)
    {
        //check line data is TaishiDataset
        TaiShiDataUtils.TaiShiDataset dataset;
        System.out.println("check data line ....");
        while (iterator.hasNext()) {
            dataset = (TaiShiDataUtils.TaiShiDataset) iterator.next();
            assert StringUtils.isNotBlank(dataset.getAttacker().trim());
            assert StringUtils.isNotBlank(dataset.getSip().trim());
            assert StringUtils.isNotBlank(dataset.getVictim().trim());
            assert StringUtils.isNotBlank(dataset.getDip().trim());
            assert dataset.getDport() > 0;
            assert dataset.getWrite_date() > 0;
            assert StringUtils.isNotBlank(dataset.getKill_chain());
            assert dataset.getSeverity() > 0;
            assert StringUtils.isNotBlank(dataset.getVictim_type());
            assert StringUtils.isNotBlank(dataset.getAttack_result());
            assert dataset.getAttack_type_id() > 0;
            assert StringUtils.isNotBlank(dataset.getAttack_type());
            assert dataset.getRule_id() > 0;
            assert StringUtils.isNotBlank(dataset.getRule_name());
            assert dataset.getRule_version() > 0;
            assert StringUtils.isNotBlank(dataset.getVuln_desc());
            assert StringUtils.isNotBlank(dataset.getVuln_name());
            assert StringUtils.isNotBlank(dataset.getVuln_harm());
            assert StringUtils.isNotBlank(dataset.getVuln_type());
            assert StringUtils.isNotBlank(dataset.getUri());
        }
    }

    private <T,E> void writeData(File dataFile, InsertionBase<T,E> insertionBase )
    {
        Iterator<Object> dirIter = null;
        try {
            dirIter = taishi.readLine(dataFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        while (dirIter.hasNext()) {
            //check insert one line
            taishi.writeData(dirIter.next(),insertionBase);
        }
    }

    private void checkWriteData(GraphTraversalSource traversal)
    {
        Long vertexCount = traversal.V().count().next();
        Long edgeCount = traversal.E().count().next();

        assert vertexCount == 10 : "vertex count is " + vertexCount;
        assert edgeCount == 13 : "edge count is " + edgeCount;
    }

    private <T,E> void createGraph(File dataFile, InsertionBase<T,E> insertionBase, int scenarioNumber)
    {
        //check insert mutil line
        CustomData customData = new CustomData(this.taishi);
        customData.createGraph(dataFile, insertionBase, scenarioNumber);
    }

    private static void test2(String id)
    {
        String[] parts = id.split("(?<!" + "`" + ")" + ">", -1);
        for(int i=0;i<parts.length;i++){
            System.out.println(i+":"+parts[i]);
        }
    }
    public static String escape(char splitor, char escape, String... values) {
        StringBuilder escaped = new StringBuilder((values.length + 1) << 4);
        // Do escape for every item in values
        for (String value : values) {
            if (escaped.length() > 0) {
                escaped.append(splitor);
            }

            if (value.indexOf(splitor) == -1) {
                escaped.append(value);
                continue;
            }
            //将values中包含的特殊字符splitor转义成escape+splitor
            // Do escape for current item
            for (int i = 0, n = value.length(); i < n; i++) {
                char ch = value.charAt(i);
                if (ch == splitor) {
                    escaped.append(escape);
                }
                escaped.append(ch);
            }
        }
        return escaped.toString();
    }

    public static void test2(Object... strs)
    {
        for (Object str : strs) {
            System.out.println(str);
        }
    }

    public static void main(String[] args) throws IOException
    {
//        Preconditions.checkArgument(
//                args.length == 3,
//                "input args must be 3 : input.properties ,storeFile , dataDir !");
        TaiShiDataTester tester = new TaiShiDataTester();
//        tester.input = args[0];
//        tester.storeFile = args[1];
//        tester.dir = args[2];
        File store = new File(tester.storeFile);
        File dataFile = new File(tester.dir);
        BenchmarkConfiguration conf = tester.benchmarkConf();
        //read file test
        //        tester.readLine();
        //hugegraph
//        HugeGraphCoreDatabase huge = new HugeGraphCoreDatabase(conf, store);
//        tester.createSchema(huge.loadGraph(true).schema(), GraphDatabaseType.HUGEGRAPH_CORE);
//        HugeGraphCoreMassiveInsertion hugeMassive = new HugeGraphCoreMassiveInsertion(huge.getGraph(),true);
//        tester.writeData(dataFile,hugeMassive);
//        hugeMassive.post2();
//        tester.checkWriteData(huge.getGraph().traversal());
//        huge.shutdown();
//        tester.createSchema(huge.loadGraph(true).schema(), GraphDatabaseType.HUGEGRAPH_CORE);
//        tester.createGraph(dataFile, hugeMassive, 0);
//        tester.checkWriteData(huge.getGraph().traversal());
//        System.out.println("TaiShi data massive insertion test End!!");

        //janusgraph
        JanusGraphCoreDatabase janus = new JanusGraphCoreDatabase(conf, store);
        tester.createSchema(janus.openGraph().openManagement(), GraphDatabaseType.JANUSGRAPH_CORE);
        JanusGraphCoreMassiveInsertion janusMassive = new JanusGraphCoreMassiveInsertion(janus.getGraph());
        tester.writeData(dataFile, janusMassive);
        janusMassive.post2();
//        tester.checkWriteData(janus.getGraph().traversal());
//        janus.shutdown();
//        tester.createSchema(janus.openGraph().openManagement(), GraphDatabaseType.JANUSGRAPH_CORE);
//        tester.createGraph(dataFile, janusMassive, 0);
        tester.checkWriteData(janus.getGraph().traversal());
        System.out.println("TaiShi data massive insertion test End!!");
    }
}
