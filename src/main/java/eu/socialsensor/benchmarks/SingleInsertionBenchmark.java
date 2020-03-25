package eu.socialsensor.benchmarks;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.insert.Custom;
import eu.socialsensor.insert.CustomData;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.parboiled.common.Preconditions;

/**
 * SingleInsertionBenchmak implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class SingleInsertionBenchmark extends PermutingBenchmarkBase implements InsertsGraphData
{
    public static final String INSERTION_TIMES_OUTPUT_FILE_NAME_BASE = "SINGLE_INSERTIONResults";
    private static final Logger LOG = LogManager.getLogger();

    public SingleInsertionBenchmark(BenchmarkConfiguration bench)
    {
        super(bench, BenchmarkType.SINGLE_INSERTION);
    }

    @Override
    public void post()
    {
        LOG.info("Write results to " + outputFile.getAbsolutePath());
        for (GraphDatabaseType type : bench.getSelectedDatabases())
        {
            String prefix = outputFile.getParentFile().getAbsolutePath() + File.separator
                + INSERTION_TIMES_OUTPUT_FILE_NAME_BASE + "." + type.getShortname();
            List<List<Double>> insertionTimesOfEachScenario = Utils.getDocumentsAs2dList(prefix, bench.getScenarios());
            times.put(type, Utils.calculateMeanList(insertionTimesOfEachScenario));
            Utils.deleteMultipleFiles(prefix, bench.getScenarios());
        }
        // use the logic of the superclass method after populating the times map
        super.post();
    }

    @Override
    public void benchmarkOne(GraphDatabaseType type, int scenarioNumber)
    {
        GraphDatabase<?,?,?,?> graphDatabase = Utils.createDatabaseInstance(bench, type);

        //custom dataset
        CustomData customData = null;
        File customDataset = null;
        if (bench.isCustomDataset()) {
            Class<Custom> customDataClass = bench.getCustomDataClass();
            customDataset = bench.getCustomDataset();
            Preconditions.checkArgNotNull(
                    customDataset, "dataset is null");
            Preconditions.checkArgNotNull(
                    customDataClass, "custom class is null");
            LOG.debug(String.format(
                    "Single insertion custom dataset %s",
                    bench.getCustomDataset().getName()));
            try {
                customData = new CustomData(customDataClass.newInstance());
            } catch (IllegalAccessException | InstantiationException e) {
                throw new RuntimeException("Single custom class instance faild", e);
            }
            graphDatabase.createGraphForSingleLoad(customData.getCustom());
            LOG.debug("Custom load graph in database type {}", type.getShortname());
        } else {
            // the following step includes provisioning in managed database
            // services. do not measure this time as
            // it is not related to the action of inserting.
            graphDatabase.createGraphForSingleLoad();
            LOG.debug("Single load graph in database type {}", type.getShortname());
        }

        // reset start time ,skip the time of totalTimeMap
        super.totalTimeMap.put(type, System.currentTimeMillis());
        try {
            if (bench.isCustomDataset()) {
                graphDatabase.singleModeLoading(customDataset, customData, bench.getResultsPath(), scenarioNumber);
            } else {
                graphDatabase.singleModeLoading(bench.getDataset(), bench.getResultsPath(), scenarioNumber);
            }
        } catch (Exception e) {
            LOG.error("single insertion benchmark error",e);
        }
        graphDatabase.shutdown();
    }
}
