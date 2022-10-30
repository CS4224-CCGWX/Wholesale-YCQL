package util;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

public class PerformanceReportGenerator {
    private static final String path = "./experiment/%d.csv";
    private static final String clientsPath = "./experiment/clients.csv";
    private static final String throughputPath = "./experiment/throughput.csv";

    public static void generatePerformanceReport(List<Long> latencyList, long totalTime, int fileNumber) {
        Collections.sort(latencyList);
        int count = latencyList.size();
        long sum = latencyList.stream().mapToLong(Long::longValue).sum();
        OutputFormatter outputFormatter = new OutputFormatter();
        List<String> arr = new ArrayList<>();
        arr.add(String.valueOf(fileNumber));

        System.err.println(OutputFormatter.linebreak);
        System.err.println("Performance Report: ");

        System.err.printf(outputFormatter.formatTotalTransactions(count));
        arr.add(String.valueOf(count));

        System.err.printf(outputFormatter.formatTotalElapsedTime(totalTime));
        arr.add(String.valueOf(totalTime));

        double throughput = (double) count / totalTime;
        System.err.printf(outputFormatter.formatThroughput(throughput));
        arr.add(String.format("%.2f", throughput));

        double average = (double) convertToMs(sum) / count;
        System.err.printf(outputFormatter.formatAverage(average));
        arr.add(String.format("%.2f", average));

        long median = convertToMs(getMedian(latencyList));
        System.err.printf(outputFormatter.formatMedian(median));
        arr.add(String.valueOf(median));

        long p95 = convertToMs(getByPercentile(latencyList, 95));
        System.err.printf(outputFormatter.formatPercentile(95, p95));
        arr.add(String.valueOf(p95));

        long p99 = convertToMs(getByPercentile(latencyList, 99));
        System.err.printf(outputFormatter.formatPercentile(99, p99));
        arr.add(String.valueOf(p99));

        System.err.printf("longest latency: %dms\n", convertToMs(latencyList.get(latencyList.size() - 1)));

        String filePath = String.format(path, fileNumber);
        File file = new File(filePath);

        try {
            file.createNewFile();
            FileWriter outputFile = new FileWriter(file, false);
            CSVWriter writer = new CSVWriter(outputFile);
            String[] csvOutput = new String[arr.size()];
            arr.toArray(csvOutput);
            writer.writeNext(csvOutput);
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.err.println(OutputFormatter.linebreak);
    }

    public static void generatePerformanceSummary() {
        List<String[]> clients = new ArrayList<>();

        for (int i = 0; i < 20; ++i) {
            String filePath = String.format(path, i);
            try {
                FileReader fileReader = new FileReader(filePath);
                CSVReader reader = new CSVReader(fileReader);
                String[] values = reader.readNext();
                if (values != null) {
                    clients.add(values);
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        // write clients.csv
        File file = new File(clientsPath);

        try {
            file.createNewFile();
            FileWriter outputFile = new FileWriter(file, false);
            CSVWriter writer = new CSVWriter(outputFile);
            writer.writeAll(clients);
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // write throughput.csv
        double min = Double.valueOf(clients.get(0)[2]);
        double max = min;
        double total = 0;
        for (String[] client : clients) {
            double cur = Double.valueOf(client[2]);
            min = Math.min(min, cur);
            max = Math.max(max, cur);
            total += cur;
        }
        double average = total / 20;
        String[] output = new String[3];
        output[0] = String.valueOf(min);
        output[1] = String.valueOf(max);
        output[2] = String.valueOf(average);

        file = new File(throughputPath);

        try {
            file.createNewFile();
            FileWriter outputFile = new FileWriter(file, false);
            CSVWriter writer = new CSVWriter(outputFile);
            writer.writeNext(output);
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
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
