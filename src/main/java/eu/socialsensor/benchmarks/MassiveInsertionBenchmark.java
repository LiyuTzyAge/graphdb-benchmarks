package eu.socialsensor.benchmarks;

import java.io.File;
import java.util.concurrent.TimeUnit;

import eu.socialsensor.insert.Custom;
import eu.socialsensor.insert.CustomData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Stopwatch;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;
import org.parboiled.common.Preconditions;

/**
 * MassiveInsertionBenchmark implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */

public class MassiveInsertionBenchmark extends PermutingBenchmarkBase implements InsertsGraphData
{
    private static final Logger logger = LogManager.getLogger();

    public MassiveInsertionBenchmark(BenchmarkConfiguration config)
    {
        super(config, BenchmarkType.MASSIVE_INSERTION);
    }

    @Override
    public void benchmarkOne(GraphDatabaseType type, int scenarioNumber)
    {
        logger.debug("Creating database instance for type " + type.getShortname());
        GraphDatabase<?,?,?,?> graphDatabase = Utils.createDatabaseInstance(bench, type);
        logger.debug("Prepare database instance for type {} for massive loading", type.getShortname());

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
            logger.debug(String.format(
                    "massive insertion custom dataset %s",
                    bench.getCustomDataset().getName()));
            try {
                customData = new CustomData(customDataClass.newInstance());
            } catch (IllegalAccessException | InstantiationException e) {
                throw new RuntimeException("massive custom class instance faild", e);
            }
            graphDatabase.createGraphForCustom(customData.getCustom());
            logger.debug("Custom load graph in database type {}", type.getShortname());
        } else {
            // the following step includes provisioning in managed database
            // services. do not measure this time as
            // it is not related to the action of inserting.
            graphDatabase.createGraphForMassiveLoad();
            logger.debug("Massive load graph in database type {}", type.getShortname());
        }

        // reset start time ,skip the time of totalTimeMap
        super.totalTimeMap.put(type, System.currentTimeMillis());
        Stopwatch watch = new Stopwatch();
        watch.start();
        try {
            if (bench.isCustomDataset()) {
//                customData.createGraph(customDataset,graphDatabase.massiveInsertion(),0);
                graphDatabase.massiveModeLoading(customDataset,customData);
            }else {
                graphDatabase.massiveModeLoading(bench.getDataset());
            }
        } catch (Exception e) {
            logger.error("massive insertion benchmark error",e.getMessage());
        }
        logger.debug("Shutdown massive graph in database type {}", type.getShortname());
        graphDatabase.shutdownMassiveGraph();
        times.get(type).add((double) watch.elapsed(TimeUnit.MILLISECONDS));
    }
}
