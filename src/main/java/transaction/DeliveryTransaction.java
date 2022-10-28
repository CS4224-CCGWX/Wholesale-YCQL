package transaction;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.CqlSession;

import util.PreparedQueries;
import util.TimeFormatter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class DeliveryTransaction extends AbstractTransaction {
    private int warehouseId;
    private int carrierId;

    final int DISTRICT_NUM = 10;

    public DeliveryTransaction(CqlSession session, int warehouseId, int carrierId) {
        super(session);
        this.warehouseId = warehouseId;
        this.carrierId = carrierId;
    }

    public void execute() {
        List<Row> res;
        /*
        (a) Let N denote the value of the smallest order number O ID for district (W ID,DISTRICT NO)
        with O CARRIER ID = null; i.e.,
                N = min{t.O ID ∈ Order | t.O W ID = W ID, t.D ID = DISTRICT NO, t.O CARRIER ID = null}
        Let X denote the order corresponding to order number N, and let C denote the customer
        who placed this order
         */

        for (int districtNo = 1; districtNo <= DISTRICT_NUM; districtNo++) {
            res = executeQuery(PreparedQueries.getOrderIdToDeliver, warehouseId, districtNo);
            executeQuery(PreparedQueries.updateOrderIdToDeliver, warehouseId, districtNo);

            int orderId = res.get(0).getInt("D_NEXT_DELIVER_O_ID");
            print("********** Delivery Transaction *********\n");
            print(String.format("The next order to deliver in (%d, %d) is %d", warehouseId, districtNo, orderId));

            /*
            (b) Update the order X by setting O CARRIER ID to CARRIER ID
             */
            executeQuery(PreparedQueries.updateCarrierIdInOrder, carrierId, warehouseId, districtNo, orderId);

            /*
            (c) Update all the order-lines in X by setting OL DELIVERY D to the current date and time
            (d) Update customer C as follows:
            • Increment C BALANCE by B, where B denote the sum of OL AMOUNT for all the
            items placed in order X
            • Increment C DELIVERY CNT by 1
             */
            double orderAmount = 0;
            ArrayList<Integer> orderLineNums = new ArrayList<>();
            List<Row> orderLines = executeQuery(PreparedQueries.getOrderLineInOrder, warehouseId, districtNo, orderId);

            int customerId = orderLines.get(0).getInt("OL_C_ID");
            for (Row orderLine : orderLines) {
                orderAmount += orderLine.getBigDecimal("OL_AMOUNT").doubleValue();
                orderLineNums.add(orderLine.getInt("OL_NUMBER"));
            }

            // (c)
            for (int olNum : orderLineNums) {
                executeQuery(PreparedQueries.updateDeliveryDateInOrderLine, TimeFormatter.getCurrentTimestamp().toInstant(), warehouseId, districtNo, orderId, olNum);
                print(String.format("Updated order line (%d, %d, %d, %d)", warehouseId, districtNo, orderId, olNum));
            }

            // (d)
            List<Row> customers = executeQuery(PreparedQueries.getCustomerBalance, warehouseId, districtNo, customerId);

            double updatedBalance = customers.get(0).getBigDecimal(0).doubleValue() + orderAmount;
            executeQuery(PreparedQueries.updateCustomerBalanceAndDcount, BigDecimal.valueOf(updatedBalance), warehouseId, districtNo, customerId);
            print(String.format("Updated the info of customer (%d, %d, %d)", warehouseId, districtNo, customerId));
        }
    }

    @Override 
    public String toString() {
        return String.format("Delivery Transaction info: warehouseId: %d, carrierId: %d", warehouseId, carrierId);
    }

}
