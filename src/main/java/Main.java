import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import parser.DataLoader;
import parser.TransactionParser;
import transaction.AbstractTransaction;
import transaction.SummaryTransaction;
import util.OutputFormatter;
import util.PerformanceReportGenerator;

class Main {
    public static void main(String[] args) {
        String action = args[0];
        switch (action) {
        case "load_data": {
            loadData(args);
            break;
        }
        case "run": {
            run(args);
            break;
        }
        case "summary": {
            summary(args);
            break;
        }
        default: {
            System.err.printf("Action: %s not specified", action);
        }
        }
    }

    private static void loadData(String[] args) {
        String ip = args[1];
        String schemaPath = args[2];
        String dataPath = args[3];

        Cluster cluster = Cluster.builder()
                .addContactPoint(ip)
                .build();
        Session session = cluster.connect();

        DataLoader dataLoader = new DataLoader(session, schemaPath, dataPath);
        dataLoader.loadAll();
    }

    private static void run(String[] args) {
        String ip = args[1];
        String consistencyLevel = "";

        Cluster cluster = Cluster.builder()
                .addContactPoint(ip)
                .build();
        Session session = cluster.connect();
        session.execute("USE wholesale;");

        TransactionParser transactionParser = new TransactionParser(session);
        OutputFormatter outputFormatter = new OutputFormatter();

        List<Long> latencyList = new ArrayList<>();
        long fileStart, fileEnd, txStart, txEnd, elapsedTime;

        fileStart = System.nanoTime();
        while (transactionParser.hasNext()) {
            AbstractTransaction transaction = transactionParser.parseNextTransaction();
            System.out.println(OutputFormatter.linebreak);
            System.out.println(outputFormatter.formatTransactionID(latencyList.size()));
            if (args.length >= 2) {
                consistencyLevel = args[2];
                transaction.setDefaultConsistencyLevel(consistencyLevel);
            }
            txStart = System.nanoTime();
            transaction.execute();
            txEnd = System.nanoTime();
            System.out.println(OutputFormatter.linebreak);

            elapsedTime = txEnd - txStart;
            latencyList.add(elapsedTime);
        }
        fileEnd = System.nanoTime();

        long totalElapsedTime = TimeUnit.SECONDS.convert(fileEnd - fileStart, TimeUnit.NANOSECONDS);
        PerformanceReportGenerator.generatePerformanceReport(latencyList, totalElapsedTime);

        session.close();
        transactionParser.close();
    }

    private static void summary(String[] args) {
        String ip = args[1];
        Cluster cluster = Cluster.builder()
                .addContactPoint(ip)
                .build();
        Session session = cluster.connect();
        session.execute("USE wholesale;");

        AbstractTransaction summaryTransaction = new SummaryTransaction(session);
        summaryTransaction.execute();

        session.close();
    }
}
