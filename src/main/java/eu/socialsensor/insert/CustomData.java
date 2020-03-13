package eu.socialsensor.insert;

import com.google.common.base.Stopwatch;
import eu.socialsensor.benchmarks.SingleInsertionBenchmark;
import eu.socialsensor.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 *  @author: liyu04
 *  @date: 2020/3/10
 *  @version: V1.0
 *
 * @Description:
 */
public class CustomData<U>
{

    private static final Logger LOG = LogManager.getLogger();

    private final Custom<U> custom;
    public CustomData(Custom<U> custom)
    {
        this.custom = custom;
    }

    public <T,E> void createGraph(File dataFile, InsertionBase<T,E> insertionBase,int scenarioNumber)
    {
        //1.读取文件，返回实体实例
        //2.记录时间
        //3.调用getOrCreateCust与relateNodesCust写入数据
        Iterator<U> iterator = null;
        try {
            iterator = custom.readLine(dataFile);
        } catch (IOException e) {
            LOG.error("data file {} can not read !",dataFile.getAbsolutePath());
        }

        Stopwatch thousandWatch = new Stopwatch(), watch = new Stopwatch();
        thousandWatch.start();
        watch.start();
        int i = 4;
        while (iterator.hasNext()) {
            U line = iterator.next();
            custom.writeData(line,insertionBase);
            if (i % 1000 == 0)
            {
                insertionBase.insertionTimes.add((double) thousandWatch.elapsed(TimeUnit.MILLISECONDS));
                thousandWatch.stop();
                thousandWatch = new Stopwatch();
                thousandWatch.start();
            }
            i++;
        }
        insertionBase.post();
        insertionBase.insertionTimes.add((double) watch.elapsed(TimeUnit.MILLISECONDS));

        if (insertionBase.single)
        {
            Utils.writeTimes(
                    insertionBase.insertionTimes,
                    new File(
                            insertionBase.resultsPath,
                            SingleInsertionBenchmark.INSERTION_TIMES_OUTPUT_FILE_NAME_BASE +
                                    "." +
                                    insertionBase.type.getShortname() +
                                    "." +
                                    Integer.toString(scenarioNumber)
                    )
            );
        }
    }
}
