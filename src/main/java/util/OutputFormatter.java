package util;

import com.datastax.driver.core.Row;

public class OutputFormatter {
    private final static String delimiter = "\n";

    public String formatFullCustomerInfo(Row customerInfo) {
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

        sb.append(String.format("Since: %s", customerInfo.getString("C_SINCE")));
        sb.append(delimiter);

        sb.append(String.format("Credit status: %s", customerInfo.getString("C_CREDIT")));
        sb.append(delimiter);

        sb.append(String.format("Credit limit: %f", customerInfo.getDecimal("C_CREDIT_LIM").doubleValue()));
        sb.append(delimiter);

        sb.append(String.format("Discount: %f", customerInfo.getDecimal("C_DISCOUNT").doubleValue()));
        sb.append(delimiter);

        sb.append(String.format("Balance: %f", customerInfo.getDecimal("C_BALANCE").doubleValue()));

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

    public String formatStockLevelTransactionOutput(long result, String transactionInfo) {
        StringBuilder sb = new StringBuilder(transactionInfo);
        sb.append(delimiter);
        sb.append(result);
        return sb.toString();
    }
}
