package eu.socialsensor.insert;

import com.alibaba.fastjson.JSONException;
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
public class CustomData
{

    private static final Logger LOG = LogManager.getLogger();
    private static long error = 0L;

    private final Custom custom;
    public CustomData(Custom custom)
    {
        this.custom = custom;
    }

    public <T,E> void createGraph(File dataFile, InsertionBase<T,E> insertionBase,int scenarioNumber)
    {
        //1.读取文件，返回实体实例
        //2.记录时间
        //3.调用getOrCreateCust与relateNodesCust写入数据
        Iterator<Object> iterator = null;
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
            Object line =null;
            try {
                line = iterator.next();
                custom.writeData(line,insertionBase);
            } catch (JSONException | IllegalArgumentException e) {
                LOG.error("error data line ", e);
                error++;
                continue;
            }
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
        LOG.info("error data line num is {}",error);
    }

    public Custom getCustom()
    {
        return custom;
    }
}
