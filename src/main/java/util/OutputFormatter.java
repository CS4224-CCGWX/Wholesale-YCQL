package util;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import com.datastax.oss.driver.api.core.cql.Row;

public class OutputFormatter {
    private final static String delimiter = "\n";
    public final static String linebreak = "=======================================";

    public String formatCustomerName(Row customerInfo) {
        return String.format("Customer Name: (%s, %s, %s)",
                customerInfo.getString("C_FIRST"),
                customerInfo.getString("C_MIDDLE"),
                customerInfo.getString("C_LAST"));
    }

    public String formatFullCustomerInfo(Row customerInfo, double balance) {
        StringBuilder sb = new StringBuilder();
        sb.append("Customer info: ");
        sb.append(delimiter);

        sb.append(String.format("Identifier: (%d, %d, %d)",
                customerInfo.getInt("C_W_ID"),
                customerInfo.getInt("C_D_ID"),
                customerInfo.getInt("C_ID")));
        sb.append(delimiter);

        sb.append(String.format("Name: (%s, %s, %s)",
                customerInfo.getString("C_FIRST"),
                customerInfo.getString("C_MIDDLE"),
                customerInfo.getString("C_LAST")));
        sb.append(delimiter);

        sb.append(String.format("Address: (%s, %s, %s, %s, %s)",
                customerInfo.getString("C_STREET_1"),
                customerInfo.getString("C_STREET_2"),
                customerInfo.getString("C_CITY"),
                customerInfo.getString("C_STATE"),
                customerInfo.getString("C_ZIP")));
        sb.append(delimiter);

        sb.append(String.format("Phone: %s", customerInfo.getString("C_PHONE")));
        sb.append(delimiter);

        sb.append(String.format("Since: %s", customerInfo.getInstant("C_SINCE")));
        sb.append(delimiter);

        sb.append(String.format("Credit status: %s", customerInfo.getString("C_CREDIT")));
        sb.append(delimiter);

        sb.append(String.format("Credit limit: %s", customerInfo.getBigDecimal("C_CREDIT_LIM").doubleValue()));
        sb.append(delimiter);

        sb.append(String.format("Discount: %s", customerInfo.getBigDecimal("C_DISCOUNT").doubleValue()));
        sb.append(delimiter);

        sb.append(String.format("Balance: %s", balance));

        return sb.toString();
    }

    public String formatWarehouseAddress(Row warehouseAddress) {
        return String.format("Warehouse address: (%s, %s, %s, %s, %s)",
                warehouseAddress.getString("W_STREET_1"),
                warehouseAddress.getString("W_STREET_2"),
                warehouseAddress.getString("W_CITY"),
                warehouseAddress.getString("W_STATE"),
                warehouseAddress.getString("W_ZIP"));
    }

    public String formatDistrictAddress(Row districtAddress) {
        return String.format("district address: (%s, %s, %s, %s, %s)",
                districtAddress.getString("D_STREET_1"),
                districtAddress.getString("D_STREET_2"),
                districtAddress.getString("D_CITY"),
                districtAddress.getString("D_STATE"),
                districtAddress.getString("D_ZIP"));
    }

    public String formatCustomerFullNameAndBalance(Row cInfo) {
        return String.format("Customer name: (%s, %s, %s), balance: %.2f",
                cInfo.getString("C_FIRST"),
                cInfo.getString("C_MIDDLE"),
                cInfo.getString("C_LAST"),
                cInfo.getBigDecimal("C_BALANCE").doubleValue());
    }

    public String formatLastOrderInfo(int lastOrderId, int carrierId, Instant datetime) {
        String time = "";
        if(datetime == null) {
            time = "Not available";
        } else {
            time = TimeFormatter.formatTime(datetime);
        }
        return String.format("Last order ID: %d, Carrier ID: %d, Datetime: %s",
                lastOrderId, carrierId, time);
    }

    public String formatItemInfo(Row itemInfo) {
        Instant time = itemInfo.getInstant("OL_DELIVERY_D");
        String deliverTime = null;
        if (time == null) {
            deliverTime = "Not delivered";
        } else {
            deliverTime = TimeFormatter.formatTime(time);
        }
        return String.format("- Item number: %d, Supply warehouse ID: %d, Quantity: %d, Price: %.2f, Delivery time: %s",
                itemInfo.getInt("OL_I_ID"),
                itemInfo.getInt("OL_SUPPLY_W_ID"),
                itemInfo.getBigDecimal("OL_QUANTITY").intValue(),
                itemInfo.getBigDecimal("OL_AMOUNT").doubleValue(),
                deliverTime);
    }

    public String formatStockLevelTransactionOutput(long result, String transactionInfo) {
        StringBuilder sb = new StringBuilder(transactionInfo);
        sb.append(delimiter);
        sb.append("Result: ");
        sb.append(result);
        return sb.toString();
    }

    public String formatTopBalanceCustomerInfo(Row cInfo, String warehouseName, String districtName) {
        /*
        (a) Name of customer (C FIRST, C MIDDLE, C LAST)
        (b) Balance of customer???s outstanding payment C BALANCE
        (c) Warehouse name of customer W NAME
        (d) District name of customer D NAME
         */
        return String.format("Customer: (%s, %s, %s), Balance: %.2f, Warehouse: %s, District: %s",
                cInfo.getString("C_FIRST"), cInfo.getString("C_MIDDLE"), cInfo.getString("C_LAST"),
                cInfo.getBigDecimal("C_BALANCE").doubleValue(), warehouseName, districtName);
    }

    public String formatRelatedCustomerOutput(List<Row> resultRows) {
        StringBuilder sb = new StringBuilder();
        for (Row row : resultRows) {
            sb.append(row.getString("customer_id"));
            sb.append("\n");
        }
        return sb.toString();
    }

    public String formatOrderIdAndTimestamp(int id, Instant timestamp) {
        if (timestamp == null) {
            return String.format("order id: %d, entry date and time: not available", id);
        }
        return String.format("order id: %d, entry date and time: %s", id, TimeFormatter.formatTime(timestamp));
    }

    public String formatPopularItemQuantity(String name, BigDecimal quantity) {
        return String.format("Item name: %s, quantity: %.2f", name, quantity);
    }

    public String formatPopularItemRatio(String name, double ratio) {
        return String.format("Item name: %s, The percentage of orders in S that contain the popular item t: %.2f", name, ratio);
    }

    public String formatTransactionID(int i) {
        return String.format("Transaction ID: %d", i);
    }

    public String formatTotalTransactions(int count) {
        return String.format("Total number of transactions: %d\n", count);
    }

    public String formatTotalElapsedTime(long totalTime) {
        return String.format("Total elapsed time: %ds\n", totalTime);
    }

    public String formatThroughput(double throughput) {
        return String.format("Transaction throughput: %.2f per second\n", throughput);
    }

    public String formatAverage(double latency) {
        return String.format("Average latency: %.2fms\n", latency);
    }

    public String formatMedian(long latency) {
        return String.format("Median latency: %dms\n", latency);
    }

    public String formatPercentile(int percentile, long latency) {
        return String.format("%dth percentile transaction latency: %dms\n", percentile, latency);
    }
}
