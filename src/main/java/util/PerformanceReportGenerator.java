package util;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PerformanceReportGenerator {
    public static void generatePerformanceReport(List<Long> latencyList, long totalTime) {
        Collections.sort(latencyList);
        int count = latencyList.size();
        long sum = latencyList.stream().mapToLong(Long::longValue).sum();
        OutputFormatter outputFormatter = new OutputFormatter();

        System.err.println(OutputFormatter.linebreak);
        System.err.println("Performance Report: ");
        System.err.printf(outputFormatter.formatTotalTransactions(count));
        System.err.printf(outputFormatter.formatTotalElapsedTime(totalTime));
        System.err.printf(outputFormatter.formatThroughput((double) count / totalTime));
        System.err.printf(outputFormatter.formatAverage(convertToMs(sum / count)));
        System.err.printf(outputFormatter.formatMedian(convertToMs(getMedian(latencyList))));
        System.err.printf(outputFormatter.formatPercentile(95, convertToMs(getByPercentile(latencyList, 95))));
        System.err.printf(outputFormatter.formatPercentile(99, convertToMs(getByPercentile(latencyList, 99))));
        System.err.println(OutputFormatter.linebreak);
    }

    private static long convertToMs(long nano) {
        return TimeUnit.MILLISECONDS.convert(nano, TimeUnit.NANOSECONDS);
    }

    private static long getMedian(List<Long> list) {
        long mid1 = list.get(list.size() / 2);
        if (list.size() % 2 != 0) {
            return mid1;
        } else {
            long mid2 = list.get(list.size() / 2 - 1);
            return (mid1 + mid2) / 2;
        }
    }

    private static long getByPercentile(List<Long> list, int percentile) {
        int i = list.size() * percentile / 100;
        return list.get(i);
    }
}
