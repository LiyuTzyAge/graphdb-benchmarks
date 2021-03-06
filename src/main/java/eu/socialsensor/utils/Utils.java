package eu.socialsensor.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import eu.socialsensor.graphdatabases.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.util.MathArrays;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;

/**
 * This class contains all the required utility functions for the benchmark
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class Utils
{
    public static final Logger logger = LogManager.getLogger();

    public static List<List<Double>> getDocumentsAs2dList(String docPath, int scenarios)
    {
        List<List<Double>> data = new ArrayList<List<Double>>(scenarios);
        for (int i = 0; i < scenarios; i++)
        {
            File intermediateFile = new File(docPath + "." + (i + 1));
            if (!intermediateFile.exists())
            {
                throw new IllegalStateException("file " + intermediateFile.getAbsolutePath() + " does not exist");
            }
            data.add(getListFromTextDoc(intermediateFile));
        }
        return data;
    }

    public static final List<String> readlines(File file)
    {
        if (file == null || !file.exists())
        {
            throw new IllegalArgumentException("file object must not be null and must exist: " + file.getAbsolutePath());
        }
        if (!file.isFile() || !(file.isFile() && file.canRead()))
        {
            throw new IllegalArgumentException("file object must be a readable file: " + file.getAbsolutePath());
        }
        LineIterator it;
        try
        {
            it = FileUtils.lineIterator(file, "UTF-8");
        }
        catch (IOException e)
        {
            throw new BenchmarkingException("Unable to read lines from file: " + file.getAbsolutePath(), e);
        }
        List<String> result = new LinkedList<String>();
        try
        {
            while (it.hasNext())
            {
                result.add(it.nextLine());
            }
        }
        finally
        {
            LineIterator.closeQuietly(it);
        }

        return result;
    }

    public static final List<List<String>> parseTabulatedLines(List<String> lines, int numberOfLinesToSkip)
    {
        if (numberOfLinesToSkip < 0 || numberOfLinesToSkip > lines.size())
        {
            throw new IllegalArgumentException("can skip at least zero and at most lines.size lines");
        }
        List<List<String>> result = new LinkedList<List<String>>();

        lines.subList(numberOfLinesToSkip, lines.size()).parallelStream()
            .forEachOrdered(line -> result.add(Arrays.asList(line.split("\t"))));
        return result;
    }

    public static final List<List<String>> readTabulatedLines(File file, int numberOfLinesToSkip)
    {
        return parseTabulatedLines(readlines(file), numberOfLinesToSkip);
    }

    public static void deleteRecursively(File file)
    {
        if (!file.exists())
        {
            return;
        }
        if (file.isDirectory())
        {
            for (File child : file.listFiles())
            {
                deleteRecursively(child);
            }
        }
        if (!file.delete())
        {
            throw new RuntimeException("Couldn't empty database.");
        }
    }

    public static void deleteMultipleFiles(String filePath, int numberOfFiles)
    {
        for (int i = 0; i < numberOfFiles; i++)
        {
            deleteRecursively(new File(filePath + "." + (i + 1)));
        }
    }

    public static void writeTimes(List<Double> insertionTimes, File outputPath)
    {
        try (BufferedWriter out = new BufferedWriter(new FileWriter(outputPath)))
        {
            for (Double insertionTime : insertionTimes)
            {
                out.write(insertionTime.toString());
                out.write("\n");
            }
        }
        catch (IOException e)
        {
            throw new BenchmarkingException("unable to write times to: " + outputPath.getAbsolutePath(), e);
        }
    }

    public static List<Double> getListFromTextDoc(File file)
    {
        List<String> lines = readlines(file);
        List<Double> values = new ArrayList<Double>(lines.size());
        for (String line : lines)
        {
            values.add(Double.valueOf(line.trim()));
        }
        return values;
    }

    public static <T, E> T getKeyByValue(Map<T, E> map, E value)
    {
        for (Entry<T, E> entry : map.entrySet())
        {
            if (value.equals(entry.getValue()))
            {
                return entry.getKey();
            }
        }
        return null;
    }

    public static final File generateStorageDirectory(GraphDatabaseType type, File storageBaseDir)
    {
        return new File(storageBaseDir, type.getShortname());
    }

    public static final GraphDatabase<?,?,?,?> createDatabaseInstance(BenchmarkConfiguration config, GraphDatabaseType type)
    {
        final GraphDatabase<?,?,?,?> graphDatabase;
        final File dbStorageDirectory = generateStorageDirectory(type, config.getDbStorageDirectory());
        if (GraphDatabaseType.TITAN_FLAVORS.contains(type))
        {
            graphDatabase = new TitanGraphDatabase(type, config, dbStorageDirectory);
        }
        else if (GraphDatabaseType.NEO4J == type)
        {
            graphDatabase = new Neo4jGraphDatabase(dbStorageDirectory);
        }
        else if (GraphDatabaseType.ORIENT_DB == type)
        {
            graphDatabase = new OrientGraphDatabase(config, dbStorageDirectory);
        }
        else if (GraphDatabaseType.SPARKSEE == type)
        {
            graphDatabase = new SparkseeGraphDatabase(config, dbStorageDirectory);
        }
        else if (GraphDatabaseType.HUGEGRAPH == type)
        {
            graphDatabase = new HugeGraphDatabase(config, dbStorageDirectory);
        }
        else if (GraphDatabaseType.HUGEGRAPH_CORE == type)
        {
            graphDatabase = new HugeGraphCoreDatabase(config, dbStorageDirectory);
        } else if (GraphDatabaseType.JANUSGRAPH_CORE == type)
        {
            graphDatabase = new JanusGraphCoreDatabase(config, dbStorageDirectory);
        } else if (GraphDatabaseType.JANUSGRAPH == type) {
            graphDatabase = new JanusGraphDatabase(config, dbStorageDirectory);
        } else {
            // For safety, will handle the null case
            throw new IllegalArgumentException("Unknown type: " + type == null ? "null" : type.toString());
        }

        return graphDatabase;
    }

    public static void createMassiveLoadDatabase(GraphDatabaseType type, BenchmarkConfiguration config)
    {
        final GraphDatabase<?,?,?,?> graphDatabase = createDatabaseInstance(config, type);
        graphDatabase.createGraphForMassiveLoad();
        graphDatabase.massiveModeLoading(config.getDataset());
        graphDatabase.shutdownMassiveGraph();
    }

    /**
     * Returns a graph database loaded with data in individual transactions.
     * Does not shut down the database after the data load
     * 
     * @param type
     *            database type
     * @param config
     *            for individual databases.
     * @return
     */
    public static GraphDatabase<?,?,?,?> createSingleLoadDatabase(GraphDatabaseType type, BenchmarkConfiguration config)
    {
        final GraphDatabase<?,?,?,?> graphDatabase = createDatabaseInstance(config, type);
        graphDatabase.createGraphForSingleLoad();
        graphDatabase.singleModeLoading(config.getDataset(), null /* resultsPath */, 0);
        return graphDatabase;
    }

    public static void deleteDatabase(GraphDatabaseType type, BenchmarkConfiguration config)
    {
        logger.info(String.format("Deleting graph database %s . . . .", type.getShortname()));

        final GraphDatabase<?,?,?,?> graphDatabase = createDatabaseInstance(config, type);
        graphDatabase.delete();
    }

    public static double[] convert(List<Double> list)
    {
        if (list == null || list.isEmpty())
        {
            return new double[0];
        }
        double[] result = new double[list.size()];
        for (int i = 0; i < list.size(); i++)
        {
            result[i] = list.get(i).doubleValue();
        }

        return result;
    }

    /**
     * 
     * @param output
     * @param times
     *            in milliseconds
     * @param benchmarkTitle
     */
    public static void writeResults(File output, Map<GraphDatabaseType, List<Double>> times, String benchmarkTitle)
    {
        logger.info("Write results to " + output);
        if (output.isDirectory())
        {
            throw new IllegalArgumentException("output was a directory: " + output.getAbsolutePath());
        }

        SortedMap<GraphDatabaseType, Double> means = new TreeMap<GraphDatabaseType, Double>();
        SortedMap<GraphDatabaseType, Double> standardDeviations = new TreeMap<GraphDatabaseType, Double>();
        for (GraphDatabaseType type : times.keySet())
        {
            //MathArrays.scale 数组的缩放复制，每个条目乘以val
            final double[] scaledTimesArray = MathArrays.scale(0.001, convert(times.get(type)));
            DescriptiveStatistics stats = new DescriptiveStatistics();
            for (double val : scaledTimesArray)
            {
                stats.addValue(val);
            }
            //计算平均值?
            means.put(type, stats.getMean());
            standardDeviations.put(type, stats.getStandardDeviation());
        }

        // use two passes so the compute is not interleaved with disk IO
        try (BufferedWriter out = new BufferedWriter(new FileWriter(output)))
        {
            // TODO(amcp) add other sample means p50 p90 p99 p99.9 etc
            out.write(String.format("DB,%s p100 Mean (s),Sample Size,Standard Deviation\n", benchmarkTitle));
            for (GraphDatabaseType type : times.keySet())
            {
                out.write(String.format("%s,%f,%d,%f\n", type.getShortname(), means.get(type), times.get(type).size(),
                    standardDeviations.get(type)));
            }
        }
        catch (IOException e)
        {
            throw new BenchmarkingException(String.format("Exception thrown when writing output to %s: %s", output,
                e.getMessage()));
        }
    }

    public static void writeTotalTime(File output,String benchmarkTitle,Map<GraphDatabaseType,Long> totalTimeMap)
    {
        try (BufferedWriter out = new BufferedWriter(new FileWriter(output))) {
            out.write(String.format("DB,%s total time (s)\n",benchmarkTitle));
            for (Entry<GraphDatabaseType, Long> entry : totalTimeMap.entrySet()) {
                //millsecond to second
                out.write(String.format("%s,%f\n",entry.getKey().getShortname(),entry.getValue().floatValue()/1000));
            }
        } catch (IOException e) {
            throw new BenchmarkingException(String.format("Exception thrown when writing output to %s: %s", output,
                    e.getMessage()));
        }
    }

    public static List<Double> calculateMeanList(List<List<Double>> lists)
    {
        if (lists == null || lists.isEmpty())
        {
            return new ArrayList<Double>(0);
        }
        List<Double> result = new ArrayList<Double>();
        for (List<Double> list : lists)
        {
            result.add(new DescriptiveStatistics(convert(list)).getMean());
        }
        return result;
    }

    public static Object[] mapTopair(Map<String, Object> properties)
    {
        if (Objects.isNull(properties)) {
            return new Object[0];
        }
        int size = properties.size() * 2;
        Object[] re = new Object[size];
        int i = 0;
        for (Entry<String, Object> entry : properties.entrySet()) {
            re[i++] = entry.getKey();
            re[i++] = entry.getValue();
        }
        return re;
    }

    public static Object[] assemble(Object[] arr, Object... arrs)
    {
        int length = arrs.length + arr.length;
        Object[] re = new Object[length];
        for (int i = 0; i < length; i++) {
            if (i < arrs.length) {
                re[i] = arrs[i];
            } else {
                re[i] = arr[i - arrs.length];
            }
        }
        return re;
    }

}
