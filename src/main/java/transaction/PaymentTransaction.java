package transaction;

import java.util.List;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import util.OutputFormatter;
import util.QueryFormatter;

public class PaymentTransaction extends AbstractTransaction {
    private int warehouseId;
    private int districtId;
    private int customerId;
    private static QueryFormatter queryFormatter = new QueryFormatter();
    private static OutputFormatter outputFormatter = new OutputFormatter();
    private static final String delimiter = "\n";

    public PaymentTransaction(Session session, int warehouseId, int districtId, int customerId) {
        super(session);
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.customerId = customerId;
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
    public void execute(double payment) {
        // 1. Update the warehouse C W ID by incrementing W YTD by PAYMENT
        executeQuery(queryFormatter.updateWarehouseYearToDateAmount(warehouseId, payment));

        // 2. Update the district (C W ID,C D ID) by incrementing D YTD by PAYMENT
        executeQuery(queryFormatter.updateDistrictYearToDateAmount(warehouseId, districtId, payment));

        // 3. Update the customer (C W ID, C D ID, C ID) as follows:
        // • Decrement C BALANCE by PAYMENT
        // • Increment C YTD PAYMENT by PAYMENT
        // • Increment C PAYMENT CNT by 1
        executeQuery(queryFormatter.updateCustomerPaymentInfo(warehouseId, districtId, customerId, payment));

        // Output
        StringBuilder sb = new StringBuilder();
        List<Row> result;

        /*
         *  1. Customer’s identifier (C W ID, C D ID, C ID), name (C FIRST, C MIDDLE, C LAST), address
         *    (C STREET 1, C STREET 2, C CITY, C STATE, C ZIP), C PHONE, C SINCE, C CREDIT,
         *     C CREDIT LIM, C DISCOUNT, C BALANCE
         */
        result = executeQuery(queryFormatter.getFullCustomerInfo(warehouseId, districtId, customerId));
        sb.append(outputFormatter.formatFullCustomerInfo(result.get(0)));
        sb.append(delimiter);

        // 2. Warehouse’s address (W STREET 1, W STREET 2, W CITY, W STATE, W ZIP)
        result = executeQuery(queryFormatter.getWarehouseAddress(warehouseId));
        sb.append(outputFormatter.formatWarehouseAddress(result.get(0)));
        sb.append(delimiter);

        // 3. District’s address (D STREET 1, D STREET 2, D CITY, D STATE, D ZIP)
        result = executeQuery(queryFormatter.getDistrictAddress(warehouseId, districtId));
        sb.append(outputFormatter.formatDistrictAddress(result.get(0)));
        sb.append(delimiter);

        // 4. Payment amount PAYMENT
        sb.append(String.format("Payment: %f", payment));
        sb.append(delimiter);

        System.out.print(sb.toString());
    }
}
