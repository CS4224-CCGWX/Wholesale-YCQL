package transaction;

import java.math.BigDecimal;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.CqlSession;

import util.OutputFormatter;
import util.PreparedQueries;

public class PaymentTransaction extends AbstractTransaction {
    private int warehouseId;
    private int districtId;
    private int customerId;
    private double payment;
    private static OutputFormatter outputFormatter = new OutputFormatter();
    private static final String delimiter = "\n";

    public PaymentTransaction(CqlSession session, int warehouseId, int districtId, int customerId, double payment) {
        super(session);
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.customerId = customerId;
        this.payment = payment;
    }
    /**
     * This transaction processes a payment made by a customer.
     * Inputs:
     * 1. Customer identifier (C W ID, C D ID, C ID)
     * 2. Payment amount PAYMENT
     * Processing steps:
     * 1. Update the warehouse C W ID by incrementing W YTD by PAYMENT
     * 2. Update the district (C W ID,C D ID) by incrementing D YTD by PAYMENT
     * 3. Update the customer (C W ID, C D ID, C ID) as follows:
     * • Decrement C BALANCE by PAYMENT
     * • Increment C YTD PAYMENT by PAYMENT
     * • Increment C PAYMENT CNT by 1
     * Output the following information:
     * 1. Customer’s identifier (C W ID, C D ID, C ID), name (C FIRST, C MIDDLE, C LAST), address
     * (C STREET 1, C STREET 2, C CITY, C STATE, C ZIP), C PHONE, C SINCE, C CREDIT,
     * C CREDIT LIM, C DISCOUNT, C BALANCE
     * 2. Warehouse’s address (W STREET 1, W STREET 2, W CITY, W STATE, W ZIP)
     * 3. District’s address (D STREET 1, D STREET 2, D CITY, D STATE, D ZIP)
     * 4. Payment amount PAYMENT
     */
    public void execute() {
        // 1. Update the warehouse C W ID by incrementing W YTD by PAYMENT
        Row warehouseResult = executeQuery(PreparedQueries.getWarehouseAddressAndYtd, warehouseId).get(0);
        double warehouseYtd = warehouseResult.getBigDecimal("W_YTD").doubleValue();
        warehouseYtd += payment;
        // executeQuery(PreparedQueries.formatUpdateWarehouseYearToDateAmount(warehouseYtd), warehouseId);
        executeQuery(PreparedQueries.updateWarehouseYearToDateAmount, BigDecimal.valueOf(warehouseYtd), warehouseId);

        // 2. Update the district (C W ID,C D ID) by incrementing D YTD by PAYMENT
        Row districtResult = executeQuery(PreparedQueries.getDistrictAddressAndYtd, warehouseId, districtId).get(0);
        double districtYtd = districtResult.getBigDecimal("D_YTD").doubleValue();
        districtYtd += payment;
        // executeQuery(PreparedQueries.formatUpdateDistrictYearToDateAmount(districtYtd), warehouseId, districtId);
        executeQuery(PreparedQueries.updateDistrictYearToDateAmount, BigDecimal.valueOf(districtYtd), warehouseId, districtId);

        // 3. Update the customer (C W ID, C D ID, C ID) as follows:
        // • Decrement C BALANCE by PAYMENT
        // • Increment C YTD PAYMENT by PAYMENT
        // • Increment C PAYMENT CNT by 1
        Row customerResult = executeQuery(PreparedQueries.getFullCustomerInfo, warehouseId, districtId, customerId).get(0);
        double customerBalance = customerResult.getBigDecimal("C_BALANCE").doubleValue();
        customerBalance -= payment;
        float customerYtd = customerResult.getFloat("C_YTD_PAYMENT");
        customerYtd += payment;
        executeQuery(PreparedQueries.updateCustomerPaymentInfo, BigDecimal.valueOf(customerBalance), customerYtd, warehouseId, districtId, customerId);

        // Output
        StringBuilder sb = new StringBuilder();

        /*
         *  1. Customer’s identifier (C W ID, C D ID, C ID), name (C FIRST, C MIDDLE, C LAST), address
         *    (C STREET 1, C STREET 2, C CITY, C STATE, C ZIP), C PHONE, C SINCE, C CREDIT,
         *     C CREDIT LIM, C DISCOUNT, C BALANCE
         */
//        result = executeQuery(queryFormatter.getFullCustomerInfo(warehouseId, districtId, customerId));
        sb.append(outputFormatter.formatFullCustomerInfo(customerResult, customerBalance));
        sb.append(delimiter);

        // 2. Warehouse’s address (W STREET 1, W STREET 2, W CITY, W STATE, W ZIP)
        sb.append(outputFormatter.formatWarehouseAddress(warehouseResult));
        sb.append(delimiter);

        // 3. District’s address (D STREET 1, D STREET 2, D CITY, D STATE, D ZIP)
        sb.append(outputFormatter.formatDistrictAddress(districtResult));
        sb.append(delimiter);

        // 4. Payment amount PAYMENT
        sb.append(String.format("Payment: %.2f", payment));
        sb.append(delimiter);

        System.out.print(sb);
    }
}
