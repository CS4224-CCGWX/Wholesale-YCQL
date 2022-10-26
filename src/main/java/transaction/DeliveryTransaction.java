package transaction;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.CqlSession;
import util.PreparedQueries;
import util.QueryFormatter;
import util.TimeFormatter;

import java.util.ArrayList;
import java.util.List;

public class DeliveryTransaction extends AbstractTransaction {

    private int warehouseId;
    private int carrierId;

    final int DISTRICT_NUM = 10;
    final String getOrderIdToDeliver = "SELECT D_NEXT_DELIVER_O_ID FROM district WHERE D_W_ID = %d AND D_ID = %d;";
    final String updateOrderIdToDeliver = "UPDATE district SET D_NEXT_DELIVER_O_ID = D_NEXT_DELIVER_O_ID + 1 WHERE D_W_ID = %d AND D_ID = %d;";
    final String updateCarrierIdInOrder = "UPDATE \"order\" SET O_CARRIER_ID = %d WHERE O_W_ID = %d AND O_D_ID = %d AND O_ID = %d;";
    final String updateDeliveryDateInOrderLine = "UPDATE order_line SET OL_DELIVERY_D ='%s' WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d AND OL_NUMBER = %d;";
    final String GET_ORDER_LINE_UNDER_ORDER = "SELECT OL_AMOUNT, OL_C_ID, OL_NUMBER FROM order_line WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d";
    final String GET_CUSTOMER_BALANCE_OF_ORDER = "SELECT C_BALANCE FROM customer WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d";
    final String UPDATE_CUSTOMER_BALANCE_AND_DCOUNT = "UPDATE customer SET C_BALANCE = %f, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d";


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
            res = executeQuery(String.format(getOrderIdToDeliver, warehouseId, districtNo));
            executeQuery(String.format(updateOrderIdToDeliver, warehouseId, districtNo));

            int orderId = res.get(0).getInt("D_NEXT_DELIVER_O_ID");
            print(String.format("The next order to deliver in (%d, %d) is %d", warehouseId, districtNo, orderId));

            /*
            (b) Update the order X by setting O CARRIER ID to CARRIER ID
             */
            executeQuery(String.format(updateCarrierIdInOrder, carrierId, warehouseId, districtNo, orderId));


            /*
            (c) Update all the order-lines in X by setting OL DELIVERY D to the current date and time
            (d) Update customer C as follows:
            • Increment C BALANCE by B, where B denote the sum of OL AMOUNT for all the
            items placed in order X
            • Increment C DELIVERY CNT by 1
             */
            double orderAmount = 0;
            ArrayList<Integer> orderLineNums = new ArrayList<>();
            List<Row> orderLines = executeQuery(String.format(GET_ORDER_LINE_UNDER_ORDER, warehouseId, districtNo, orderId));
            if (orderLines.size() == 0) {
                print(String.format("No order lines in order (%d, %d, %d)", warehouseId, districtNo, orderId));
            }
            int customerId = orderLines.get(0).getInt("OL_C_ID");
            for (Row orderLine : orderLines) {
                orderAmount += orderLine.getBigDecimal("OL_AMOUNT").doubleValue();
                orderLineNums.add(orderLine.getInt("OL_NUMBER"));
            }

            // (c)
            for (int olNum : orderLineNums) {
                executeQuery(String.format(updateDeliveryDateInOrderLine, TimeFormatter.getCurrentTimestamp(), warehouseId, districtNo, orderId, olNum));
                print(String.format("Updated order line (%d, %d, %d, %d)", warehouseId, districtNo, orderId, olNum));
            }

            // (d)
            List<Row> customers = executeQuery(String.format(GET_CUSTOMER_BALANCE_OF_ORDER, warehouseId, districtNo, customerId));
            double updatedBalance = customers.get(0).getBigDecimal(0).doubleValue() + orderAmount;
            executeQuery(String.format(UPDATE_CUSTOMER_BALANCE_AND_DCOUNT, updatedBalance, warehouseId, districtNo, customerId));
            print(String.format("Updated the info of customer (%d, %d, %d)", warehouseId, districtNo, customerId));

        }
    }

}
