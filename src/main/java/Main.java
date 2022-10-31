import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;

import parser.TransactionParser;
import transaction.AbstractTransaction;
import transaction.SummaryTransaction;
import util.OutputFormatter;
import util.PerformanceReportGenerator;

class Main {
    private static final String LOCAL_DC = "datacenter1";
    private static final String KEYSPACE = "wholesale";

    public static void main(String[] args) {
        String action = args[0];
        switch(action) {
        case "run": {
            run(args);
            break;
        }
        case "summary": {
            summary(args);
            break;
        }
        default: {
            System.err.printf("%s is not supported. Supported actions: {run, summary}.", action);
        }
        }
    }

    private static void run(String[] args) {
        String ip = args[1];
        int port = Integer.parseInt(args[2]);
        int inputNumber = Integer.parseInt(args[3]);
        String consistencyLevel = "";

        CqlSession session = getSessionByIp(ip, port);

        TransactionParser transactionParser = new TransactionParser(session);
        OutputFormatter outputFormatter = new OutputFormatter();

        List<Long> latencyList = new ArrayList<>();
        long fileStart, fileEnd, txStart, txEnd, elapsedTime;

        fileStart = System.nanoTime();
        while (transactionParser.hasNext()) {
            AbstractTransaction transaction = transactionParser.parseNextTransaction();
            System.out.println(OutputFormatter.linebreak);
            System.out.println(outputFormatter.formatTransactionID(latencyList.size()));
            if (args.length >= 5) {
                consistencyLevel = args[4];
                transaction.setDefaultConsistencyLevel(consistencyLevel);
            }
            txStart = System.nanoTime();
            try {
                transaction.execute();
            } catch (Exception e) {
                System.err.println("**************************************");
                e.printStackTrace();
                System.err.println(transaction);
//                exit(-1);
            }

            txEnd = System.nanoTime();
            System.out.println(OutputFormatter.linebreak);

            elapsedTime = txEnd - txStart;
            latencyList.add(elapsedTime);
        }
        fileEnd = System.nanoTime();

        long totalElapsedTime = TimeUnit.SECONDS.convert(fileEnd - fileStart, TimeUnit.NANOSECONDS);
        PerformanceReportGenerator.generatePerformanceReport(latencyList, totalElapsedTime, inputNumber);

        session.close();
        transactionParser.close();
    }

    private static void summary(String[] args) {
        String ip = args[1];
        int port = Integer.parseInt(args[2]);
        CqlSession session = getSessionByIp(ip, port);

        AbstractTransaction summaryTransaction = new SummaryTransaction(session);
        summaryTransaction.execute();

        PerformanceReportGenerator.generatePerformanceSummary();

        session.close();
    }

    private static CqlSession getSessionByIp(String ip, int port) {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(ip, port))
                .withLocalDatacenter(LOCAL_DC)
                .withKeyspace(CqlIdentifier.fromCql(KEYSPACE))
                .build();
    }
}
