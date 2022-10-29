import static java.lang.System.exit;

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

import parser.DataLoader;
import parser.TransactionParser;
import transaction.AbstractTransaction;
import transaction.SummaryTransaction;
import util.OutputFormatter;
import util.PerformanceReportGenerator;

class Main {

    public static void main(String[] args) {
        String action = args[0];
        switch(action) {
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
        // Load partial data on the cloud
        System.out.println("Loading partial data to cloud");
        CqlSession session = getCloudSession();
        defSchema(session);
        insertSomeData(session);
        System.out.println("Finished");
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
            if (args.length >= 4) {
                consistencyLevel = args[4];
                transaction.setDefaultConsistencyLevel(consistencyLevel);
            }
            txStart = System.nanoTime();
            try {
                transaction.execute();
            } catch (Exception e) {
                System.err.println("**************************************");
                e.printStackTrace();
                System.err.println(transaction.toString());
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
        return CqlSession
                .builder()
                .addContactPoint(new InetSocketAddress(ip, port))
                .withLocalDatacenter("datacenter1")
                .withKeyspace(CqlIdentifier.fromCql("wholesale"))
                .build();
    }

    private static CqlSession getCloudSession() {
        String HOST = "ap-southeast-1.586de502-1a37-4886-b28a-3c7f12766c5f.aws.ybdb.io";
        String LOCAL_DATA_CENTER = "ap-southeast-1";
        String USER = "wangpei";
        String PASSWORD = "123456Ab";
        String SSL_CERT_PATH = "~/Desktop/Wholesale-YCQL/src/main/resources/root.crt";

        CqlSession session = CqlSession
                .builder()
                .addContactPoint(new InetSocketAddress(HOST, 9042))
                .withSslContext(createSSLHandler(SSL_CERT_PATH))
                .withAuthCredentials(USER, PASSWORD)
                .withLocalDatacenter(LOCAL_DATA_CENTER)
                .build();

        return session;
    }

    private static SSLContext createSSLHandler(String certfile) {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            FileInputStream fis = new FileInputStream(certfile);
            X509Certificate ca;
            try {
                ca = (X509Certificate) cf.generateCertificate(fis);
            } catch (Exception e) {
                System.err.println("Exception generating certificate from input file: " + e);
                return null;
            } finally {
                fis.close();
            }

            // Create a KeyStore containing our trusted CAs
            String keyStoreType = KeyStore.getDefaultType();
            KeyStore keyStore = KeyStore.getInstance(keyStoreType);
            keyStore.load(null, null);
            keyStore.setCertificateEntry("ca", ca);

            // Create a TrustManager that trusts the CAs in our KeyStore
            String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
            tmf.init(keyStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            return sslContext;
        } catch (Exception e) {
            System.err.println("Exception creating sslContext: " + e);
            return null;
        }
    }

    private static void defSchema(CqlSession session) {
        // def schema
        session.execute("CREATE KEYSPACE IF NOT EXISTS wholesale;");
        session.execute("USE wholesale;");
        session.execute("DROP TABLE IF EXISTS warehouse;");
        session.execute("CREATE TABLE IF NOT EXISTS warehouse (\n" +
                "    W_ID int,\n" +
                "    W_NAME varchar,\n" +
                "    W_STREET_1 varchar,\n" +
                "    W_STREET_2 varchar,\n" +
                "    W_CITY varchar,\n" +
                "    W_STATE text,\n" +
                "    W_ZIP text,\n" +
                "    W_TAX decimal,\n" +
                "    W_YTD decimal,\n" +
                "    PRIMARY KEY (W_ID)\n" +
                ");");
        session.execute("DROP TABLE IF EXISTS district;");
        session.execute("CREATE TABLE IF NOT EXISTS district (\n" +
                "    D_W_ID int,\n" +
                "    D_ID int,\n" +
                "    D_NAME varchar,\n" +
                "    D_STREET_1 varchar,\n" +
                "    D_STREET_2 varchar,\n" +
                "    D_CITY varchar,\n" +
                "    D_STATE text,\n" +
                "    D_ZIP text,\n" +
                "    D_TAX decimal,\n" +
                "    D_YTD decimal,\n" +
                "    D_NEXT_O_ID int,\n" +
                "    D_NEXT_DELIVER_O_ID int,\n" +
                "    PRIMARY KEY ((D_W_ID, D_ID))\n" +
                ");");
        session.execute("DROP TABLE IF EXISTS customer;");
        session.execute("CREATE TABLE IF NOT EXISTS customer (\n" +
                "    C_W_ID int,\n" +
                "    C_D_ID int,\n" +
                "    C_ID int,\n" +
                "    C_FIRST varchar,\n" +
                "    C_MIDDLE text,\n" +
                "    C_LAST varchar,\n" +
                "    C_STREET_1 varchar,\n" +
                "    C_STREET_2 varchar,\n" +
                "    C_CITY varchar,\n" +
                "    C_STATE text,\n" +
                "    C_ZIP text,\n" +
                "    C_PHONE text,\n" +
                "    C_SINCE timestamp,\n" +
                "    C_CREDIT text,\n" +
                "    C_CREDIT_LIM decimal,\n" +
                "    C_DISCOUNT decimal,\n" +
                "    C_BALANCE decimal,\n" +
                "    C_YTD_PAYMENT float,\n" +
                "    C_PAYMENT_CNT int,\n" +
                "    C_DELIVERY_CNT int,\n" +
                "    C_DATA varchar,\n" +
                "    PRIMARY KEY ((C_W_ID, C_D_ID, C_ID))\n" +
                ");");
        session.execute("DROP TABLE IF EXISTS \"order\";");
        session.execute("CREATE TABLE IF NOT EXISTS \"order\" (\n" +
                "    O_W_ID int,\n" +
                "    O_D_ID int,\n" +
                "    O_ID int,\n" +
                "    O_C_ID int,\n" +
                "    O_CARRIER_ID int,\n" +
                "    O_OL_CNT int,\n" +
                "    O_ALL_LOCAL int,\n" +
                "    O_ENTRY_D timestamp,\n" +
                "    PRIMARY KEY ((O_W_ID, O_D_ID), O_ID)\n" +
                ");");
        session.execute("DROP TABLE IF EXISTS item;");
        session.execute("CREATE TABLE IF NOT EXISTS item (\n" +
                "    I_ID int,\n" +
                "    I_NAME varchar,\n" +
                "    I_PRICE decimal,\n" +
                "    I_IM_ID int,\n" +
                "    I_DATA varchar,\n" +
                "    PRIMARY KEY (I_ID)\n" +
                ");");
        session.execute("DROP TABLE IF EXISTS order_line;");
        session.execute("CREATE TABLE IF NOT EXISTS order_line (\n" +
                "    OL_W_ID int,\n" +
                "    OL_D_ID int,\n" +
                "    OL_O_ID int,\n" +
                "    OL_NUMBER int,\n" +
                "    OL_C_ID int,\n" +
                "    OL_I_ID int,\n" +
                "    OL_DELIVERY_D timestamp,\n" +
                "    OL_AMOUNT decimal,\n" +
                "    OL_SUPPLY_W_ID int,\n" +
                "    OL_QUANTITY decimal,\n" +
                "    OL_DIST_INFO varchar,\n" +
                "    PRIMARY KEY ((OL_W_ID, OL_D_ID), OL_O_ID, OL_NUMBER)\n" +
                ");");
        session.execute("DROP TABLE IF EXISTS stock;");
        session.execute("CREATE TABLE IF NOT EXISTS stock (\n" +
                "    S_W_ID int,\n" +
                "    S_I_ID int,\n" +
                "    S_QUANTITY decimal,\n" +
                "    S_YTD decimal,\n" +
                "    S_ORDER_CNT int,\n" +
                "    S_REMOTE_CNT int,\n" +
                "    S_DIST_01 text,\n" +
                "    S_DIST_02 text,\n" +
                "    S_DIST_03 text,\n" +
                "    S_DIST_04 text,\n" +
                "    S_DIST_05 text,\n" +
                "    S_DIST_06 text,\n" +
                "    S_DIST_07 text,\n" +
                "    S_DIST_08 text,\n" +
                "    S_DIST_09 text,\n" +
                "    S_DIST_10 text,\n" +
                "    S_DATA varchar,\n" +
                "    PRIMARY KEY ((S_W_ID, S_I_ID))\n" +
                ");");
    }

    private static void insertSomeData(CqlSession session) {
        session.execute("USE wholesale;");
        session.execute("INSERT INTO warehouse (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) VALUES (1,sxvnjhpd,dxvcrastvybcwvmgnyk,xvzxkgxtspsjdgylue,qflaqlocfljbepowfn,OM,123456789,0.0384,300000.0);");
        session.execute("INSERT INTO district ((D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID) VALUES (1,1,byiavt,tbbvflmyew,fpezdooohykpmx,oelrbuwtpmf,JV,123456789,0.1687,30000.0,3001);");
    }
}
