package transaction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.opencsv.CSVWriter;

import util.PreparedQueries;

public class SummaryTransaction extends AbstractTransaction {
    private final static String filePath = "./experiment/dbstate.csv";
    private final static int timeout = 100;
    private final static int numOfWarehouses = 10;
    private final static int numOfDistricts = 10;

    /**
     * i. select sum(W YTD) from Warehouse
     * ii. select sum(D YTD), sum(D NEXT O ID) from District
     * iii. select sum(C BALANCE), sum(C YTD PAYMENT), sum(C PAYMENT CNT), sum(C DELIVERY CNT) from Customer
     * iv. select max(O ID), sum(O OL CNT) from Order
     * v. select sum(OL AMOUNT), sum(OL QUANTITY) from Order-Line
     * vi. select sum(S QUANTITY), sum(S YTD), sum(S ORDER CNT), sum(S REMOTE CNT) from Stock
     */
    public SummaryTransaction(CqlSession session) {
        super(session);
    }

    @Override
    public void execute() {
        File file = new File(filePath);

        try {
            file.createNewFile();
            FileWriter outputfile = new FileWriter(file, false);
            CSVWriter writer = new CSVWriter(outputfile);

            System.out.println("Warehouse Summary");
            Row result = executeQuery(PreparedQueries.getWarehouseYtdSummary).get(0);
            String[] temp = new String[1];
            temp[0] = String.valueOf(result.getBigDecimal(0).doubleValue());
            writer.writeNext(temp);

            System.out.println("District Summary");
            result = executeQuery(PreparedQueries.getDistrictSummary).get(0);
            temp[0] = String.valueOf(result.getBigDecimal(0).doubleValue());
            writer.writeNext(temp);
            temp[0] = String.valueOf(result.getInt(1));
            writer.writeNext(temp);

            System.out.println("Customer Summary");
            result = executeQuery(PreparedQueries.getCustomerSummary).get(0);
            temp[0] = String.valueOf(result.getBigDecimal(0).doubleValue());
            writer.writeNext(temp);
            temp[0] = String.valueOf(result.getFloat(1));
            writer.writeNext(temp);
            temp[0] = String.valueOf(result.getInt(2));
            writer.writeNext(temp);
            temp[0] = String.valueOf(result.getInt(3));
            writer.writeNext(temp);

            System.out.println("Order Summary");
            List<String[]> arr = new ArrayList<>();
            int maxOId = 0;
            long oolCount = 0;
            for (int i = 1; i <= numOfWarehouses; ++i) {
                result = executeQueryWithTimeout(PreparedQueries.getOrderSummary, timeout, i).get(0);
                maxOId = Math.max(maxOId, result.getInt(0));
                oolCount += result.getInt(1);
                System.out.printf("Finish Order Summary at warehouse: %d\n", i);
            }
            arr.add(new String[]{String.valueOf(maxOId)});
            arr.add(new String[]{String.valueOf(oolCount)});
            writer.writeAll(arr);

            System.out.println("Order Line Summary");
            arr = new ArrayList<>();
            double olAmount = 0, olQuantity = 0;
            for (int i = 1; i <= numOfWarehouses; ++i) {
                for (int j = 1; j <= numOfDistricts; ++j) {
                    result = executeQueryWithTimeout(PreparedQueries.getOrderLineSummary, timeout, i, j).get(0);
                    olAmount += result.getBigDecimal(0).doubleValue();
                    olQuantity += result.getBigDecimal(1).doubleValue();
                    System.out.printf("Finish Order Line Summary at warehouse: %d, district: %d\n", i, j);
                }
            }
            arr.add(new String[]{String.valueOf(olAmount)});
            arr.add(new String[]{String.valueOf(olQuantity)});
            writer.writeAll(arr);

            System.out.println("Stock Summary");
            arr = new ArrayList<>();
            double sAmount = 0, sQuantity = 0;
            long soCount = 0, srCount = 0;
            for (int i = 1; i <= numOfWarehouses; ++i) {
                result = executeQueryWithTimeout(PreparedQueries.getStockSummary, timeout, i).get(0);
                sAmount += result.getBigDecimal(0).doubleValue();
                sQuantity += result.getBigDecimal(1).doubleValue();
                soCount += result.getInt(2);
                srCount += result.getInt(3);
                System.out.printf("Finish Stock Summary at warehouse: %d\n", i);
            }
            arr.add(new String[]{String.valueOf(sAmount)});
            arr.add(new String[]{String.valueOf(sQuantity)});
            arr.add(new String[]{String.valueOf(soCount)});
            arr.add(new String[]{String.valueOf(srCount)});
            writer.writeAll(arr);

            // closing writer connection
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
