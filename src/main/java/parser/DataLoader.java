package parser;

import com.datastax.oss.driver.api.core.CqlSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class DataLoader {
    private final String DELIM = ",";
    private final String WAREHOUSE_FILE = "warehouse.csv";
    private final String CUSTOMER_FILE = "customer.csv";
    private final String DISTRICT_FILE = "district.csv";
    private final String ITEM_FILE = "item.csv";
    private final String ORDER_FILE = "order.csv";
    private final String ORDER_LINE_FILE = "order-line.csv";
    private final String STOCK_FILE = "stock.csv";

    private final String YCQLSH_PATH = "/temp/yugabyte-2.14.1.0/bin/ycqlsh";

    CqlSession session;
    String schemaPath;
    String dataDir;

    public DataLoader(CqlSession session, String schemaPath, String dataDir) {
        this.session = session;
        this.schemaPath = schemaPath;
        this.dataDir = dataDir;
    }

    public void loadAll() {
        defSchema();
        loadWarehouse();
        loadDistrict();
        loadCustomer();
        loadOrder();
        loadItem();
        loadOrderLine();
        loadStock();
    }

    protected void runInShell(String command) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(command);

        BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()));

        String line = in.readLine();
        while(line != null) {
            System.out.println(line);
            line = in.readLine();
        }
        line = err.readLine();
        while(line != null) {
            System.err.println(line);
            line = err.readLine();
        }
        process.waitFor();

        in.close();
        err.close();
    }

    public void defSchema() {
        System.out.println("defSchema: \n");
        try {
            this.runInShell(String.format("%s -f %s --request-timeout=3600", YCQLSH_PATH, schemaPath));
        } catch(Exception e) {
            System.err.println(e.toString());
        }
    }

    public void loadWarehouse() {
        System.out.println("loadWarehouse: \n");
        try {
            String path = dataDir + "/" + WAREHOUSE_FILE;

            String script = String.format(
                // "%s -e \"USE wholesale;"
                // + "COPY warehouse "
                "%s -e \"COPY wholesale.warehouse (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) FROM '%s' WITH DELIMITER = '%s';\" "
                ,
                YCQLSH_PATH, path, DELIM);
            System.out.println(script);
            this.runInShell(script);
            this.runInShell(String.format("%s -e \"COPY warehouse; "
            + "(W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD)"
            + "FROM %s WITH DELIMITER = %s;\"", YCQLSH_PATH, path, DELIM));
        } catch(Exception e) {
            System.err.println(e.toString());
        }
    }

    public void loadDistrict() {
        System.out.println("loadDistrict: \n");
        try {
            String path = dataDir + "/" + DISTRICT_FILE;
            this.runInShell(String.format(
                    "%s -e \"USE wholesale;"
                    + "COPY district "
                    + "(D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2,"
                    + "D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID)"
                    + "FROM %s WITH DELIMITER = %s;\""
                    ,
                    YCQLSH_PATH, path, DELIM));
        } catch(Exception e) {
            System.err.println(e.toString());
        }
    }

    public void loadCustomer() {
        try {
            String path = dataDir + "/" + CUSTOMER_FILE;
            this.runInShell(String.format(
                    "%s -e \"USE wholesale;"
                    + "COPY customer "
                    + "(C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST,"
                    + "C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,"
                    + "C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,"
                    + "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA)"
                    + "FROM %s WITH DELIMITER = %s;\""
                    ,
                    YCQLSH_PATH, path, DELIM));
        } catch(Exception e) {
            System.err.println(e.toString());
        }
    }

    public void loadOrder() {
        try {
            String path = dataDir + "/" + ORDER_FILE;
            this.runInShell(String.format(
                    "%s -e \"USE wholesale;"
                    + "COPY \"order\" "
                    + "(O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID,"
                    + "O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D)"
                    + "FROM %s WITH DELIMITER = %s;\""
                    ,
                    YCQLSH_PATH, path, DELIM));
        } catch(Exception e) {
            System.err.println(e.toString());
        }
    }

    public void loadItem() {
        try {
            String path = dataDir + "/" + ITEM_FILE;
            this.runInShell(String.format(
                    "%s -e \"USE wholesale;"
                    + "COPY item "
                    + "(I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA)"
                    + "FROM %s WITH DELIMITER = %s;\""
                    ,
                    YCQLSH_PATH, path, DELIM));
        } catch(Exception e) {
            System.err.println(e.toString());
        }
    }

    public void loadOrderLine() {
        try {
            String path = dataDir + "/" + ORDER_LINE_FILE;
            this.runInShell(String.format(
                    "%s -e \"USE wholesale;"
                    + "COPY order_line "
                    + "(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER,"
                    + "OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID,"
                    + "OL_QUANTITY, OL_DIST_INFO)"
                    + "FROM %s WITH DELIMITER = %s;\""
                    ,
                    YCQLSH_PATH, path, DELIM));
        } catch(Exception e) {
            System.err.println(e.toString());
        }
    }

    public void loadStock() {
        try {
            String path = dataDir + "/" + STOCK_FILE;
            this.runInShell(String.format(
                    "%s -e \"USE wholesale;"
                    + "COPY stock "
                    + "(S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT,"
                    + "S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10,"
                    + "S_DATA)"
                    + "FROM %s WITH DELIMITER = %s;\""
                    ,
                    YCQLSH_PATH, path, DELIM));
        } catch(Exception e) {
            System.err.println(e.toString());
        }
    }

}
