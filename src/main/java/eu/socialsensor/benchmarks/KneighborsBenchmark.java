package eu.socialsensor.benchmarks;

import com.google.common.base.Stopwatch;
import eu.socialsensor.dataset.DatasetFactory;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *  @author: liyu04
 *  @date: 2020/3/2
 *  @version: V1.0
 *
 * @Description:
 */
public class KneighborsBenchmark extends PermutingBenchmarkBase implements RequiresGraphData
{
    private final Set<Integer> generatedNodes;
    private final int k ;
    private static final Logger LOG = LogManager.getLogger();

    public KneighborsBenchmark(BenchmarkConfiguration config)
    {
        super(config, BenchmarkType.K_OUT);
        generatedNodes = DatasetFactory.getInstance().getDataset(config.getDataset())
                .generateRandomNodes(config.getRandomNodes());
        this.k = config.getK();
    }

    @Override
    public void benchmarkOne(GraphDatabaseType type, int scenarioNumber)
    {
        GraphDatabase<?,?,?,?> graphDatabase = Utils.createDatabaseInstance(bench, type);
        graphDatabase.open();
        Stopwatch watch = new Stopwatch();
        watch.start();
        LOG.info(type+" k-out benchmarks k : "+k+" number of nodes : "+generatedNodes.size());
        double value = graphDatabase.kneighbors(k,generatedNodes);
        graphDatabase.shutdown();
        times.get(type).add((double) watch.elapsed(TimeUnit.MILLISECONDS));
        LOG.info(type+" k-neighbor avg count is "+value);
    }
}
