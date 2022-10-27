package transaction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.CqlSession;

import util.OutputFormatter;
import util.PreparedQueries;
import util.QueryFormatter;
import util.TimeFormatter;

public class OrderStatusTransaction extends AbstractTransaction {
    private final int customerWarehouseId;
    private final int customerDistrictId;
    private final int customerId;

    public OrderStatusTransaction(CqlSession session, int cWId, int cDId, int cId) {
        super(session);
        customerWarehouseId = cWId;
        customerDistrictId = cDId;
        customerId = cId;
    }

    public void execute() {
        /*
        This transaction queries the status of the last order of a customer.
        Input: Customer identifier (C W ID, C D ID, C ID)
        Output the following information:
        1. Customer’s name (C FIRST, C MIDDLE, C LAST), balance C BALANCE
        2. For the customer’s last order
            (a) Order number O ID
            (b) Entry date and time O ENTRY D
            (c) Carrier identifier O CARRIER ID
        3. For each item in the customer’s last order
            (a) Item number OL I ID
            (b) Supplying warehouse number OL SUPPLY W ID
            (c) Quantity ordered OL QUANTITY
            (d) Total price for ordered item OL AMOUNT
            (e) Data and time of delivery OL DELIVERY D
         */
        OutputFormatter outputFormatter = new OutputFormatter();

        Row cInfo = this.executeQuery(PreparedQueries.getCustomerFullNameAndBalance, customerWarehouseId, customerDistrictId, customerId).get(0);
//        String getCustomerFullNameAndBalance = String.format(
//                "SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE "
//                        + "FROM customer "
//                        + "WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;",
//                customerWarehouseId, customerDistrictId, customerId
//        );
//        Row cInfo = this.executeQuery(getCustomerFullNameAndBalance).get(0);
        System.out.println("*** Order Status Transaction Summary ***");
        System.out.println(outputFormatter.formatCustomerFullNameAndBalance(cInfo));

        Row lastOrderInfo = this.executeQuery(PreparedQueries.getCustomerLastOrderInfo, customerWarehouseId, customerDistrictId, customerId).get(0);
//        String getCustomerLastOrderInfo = String.format(
//                "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D "
//                        + "FROM \"order\" "
//                        + "WHERE O_W_ID = %d AND O_D_ID = %d AND O_C_ID = %d "
//                        + "ORDER BY O_ID DESC "
//                        + "LIMIT 1;",
//                customerWarehouseId, customerDistrictId, customerId
//        );
//        Row lastOrderInfo = this.executeQuery(getCustomerLastOrderInfo).get(0);
        int lastOrderId = lastOrderInfo.getInt("O_ID");
        int carrierId = lastOrderInfo.getInt("O_CARRIER_ID");
        Instant orderDateTime = lastOrderInfo.getInstant("O_ENTRY_D");
        System.out.println(outputFormatter.formatLastOrderInfo(lastOrderId, carrierId, orderDateTime));

        List<Row> itemsInfo = this.executeQuery(PreparedQueries.getCustomerLastOrderItemsInfo, customerWarehouseId, customerDistrictId, lastOrderId);
//        String getCustomerLastOrderItemsInfo = String.format(
//                "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D "
//                        + "FROM order_line "
//                        + "WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d;",
//                customerWarehouseId, customerDistrictId, lastOrderId
//        );
//        List<Row> itemsInfo = this.executeQuery(getCustomerLastOrderItemsInfo);
        System.out.println("Items of last order:");
        for (Row itemInfo : itemsInfo) {
            System.out.println(outputFormatter.formatItemInfo(itemInfo));
        }
    }
}
