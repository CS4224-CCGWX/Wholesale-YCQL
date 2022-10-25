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
        System.out.println(outputFormatter.formatCustomerFullNameAndBalance(cInfo));

        Row lastOrderInfo = this.executeQuery(PreparedQueries.getCustomerLastOrderInfo, customerWarehouseId, customerDistrictId, customerId).get(0);
        int lastOrderId = lastOrderInfo.getInt("O_ID");
        int carrierId = lastOrderInfo.getInt("O_CARRIER_ID");
        Date orderDateTime =  lastOrderInfo.getTimestamp("O_ENTRY_D");
        System.out.println(outputFormatter.formatLastOrderInfo(lastOrderId, carrierId, orderDateTime));

        List<Row> itemsInfo = this.executeQuery(PreparedQueries.getCustomerLastOrderItemsInfo, customerWarehouseId, customerDistrictId, lastOrderId);
        System.out.println("*** Order Status Transaction Summary ***");
        for (Row itemInfo : itemsInfo) {
            System.out.println(outputFormatter.formatItemInfo(itemInfo));
        }
    }
}
