/*
 * Copyright (c) 2014 杭州端点网络科技有限公司
 */

package metric;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.io.File;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *   功能描述:
 * </pre>
 *
 * @author wanggen on 2015-05-08.
 */
public class Test {

    static final MetricRegistry metrics = new MetricRegistry();
    static final Timer responses = metrics.timer(MetricRegistry.name(Test.class, "responses"));

    public static void main(String args[]) throws InterruptedException {
        startReport();
        Meter requests = metrics.meter("requests");
        requests.mark();


        Histogram 访问柱状图 = metrics.histogram("访问柱状图");
        for(int i=1;i<=1000;i++)
            访问柱状图.update(new Random().nextInt(100));

        for(int i=1;i<=100;i++){
            final Timer.Context context = responses.time();
            Thread.sleep(new Random().nextInt(10));
            context.stop();
        }

        wait5Seconds();
    }

    static void startReport() {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);

        final CsvReporter reporter1 = CsvReporter.forRegistry(metrics)
                .formatFor(Locale.US)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(new File("csv-reporter/"));
        reporter1.start(1, TimeUnit.SECONDS);

    }

    static void wait5Seconds() {
        try {
            Thread.sleep(5 * 1000);
        }
        catch(InterruptedException e) {}
    }

}
