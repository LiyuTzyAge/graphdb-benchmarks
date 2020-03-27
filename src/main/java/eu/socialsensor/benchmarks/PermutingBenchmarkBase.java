package eu.socialsensor.benchmarks;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.iterators.PermutationIterator;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

/**
 * Base class abstracting the logic of permutations
 * 
 * @author Alexander Patrikalakis
 */
public abstract class PermutingBenchmarkBase extends BenchmarkBase
{
    protected final Map<GraphDatabaseType, List<Double>> times;
    private static final Logger LOG = LogManager.getLogger();

    protected PermutingBenchmarkBase(BenchmarkConfiguration bench, BenchmarkType typeIn)
    {
        super(bench, typeIn);
        times = new HashMap<GraphDatabaseType, List<Double>>();
        for (GraphDatabaseType type : bench.getSelectedDatabases())
        {
            times.put(type, new ArrayList<Double>(bench.getScenarios()));
        }
    }

    @Override
    public void startBenchmarkInternal()
    {
        LOG.info(String.format("Executing %s Benchmark . . . .", type.longname()));

        if (bench.permuteBenchmarks())
        {
            PermutationIterator<GraphDatabaseType> iter = new PermutationIterator<GraphDatabaseType>(
                bench.getSelectedDatabases());
            int cntPermutations = 1;
            while (iter.hasNext())
            {
                LOG.info("Scenario " + cntPermutations);
                startBenchmarkInternalOnePermutation(iter.next(), cntPermutations);
                cntPermutations++;
            }
        }
        else
        {
            startBenchmarkInternalOnePermutation(bench.getSelectedDatabases(), 1);
        }

        LOG.info(String.format("%s Benchmark finished", type.longname()));
        post();
    }

    protected final Map<GraphDatabaseType, Long> totalTimeMap = new HashMap<>();
    private void startBenchmarkInternalOnePermutation(Collection<GraphDatabaseType> types, int cntPermutations)
    {
        for (GraphDatabaseType type : types)
        {
            //start
            totalTimeMap.put(type, System.currentTimeMillis());
            try {
                benchmarkOne(type, cntPermutations);
                //end
                totalTimeMap.put(type, (System.currentTimeMillis() - totalTimeMap.get(type)));
            } catch (Exception e) {
                LOG.error("benchmark error !",e);
                throw e;
            }
        }
    }

    public abstract void benchmarkOne(GraphDatabaseType type, int scenarioNumber);

    public void post()
    {
        Utils.writeResults(outputFile, times, type.longname());
        File f = new File(outputFile.getParent(), this.getClass().getSimpleName() + "-total.csv");
        Utils.writeTotalTime(f,this.getClass().getSimpleName(),totalTimeMap);
    }
}
