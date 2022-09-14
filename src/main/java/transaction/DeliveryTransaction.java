package transaction;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import util.QueryFormatter;

import java.util.List;

public class DeliveryTransaction extends AbstractTransaction {

    private int warehouseId;
    private int carrierId;

    public DeliveryTransaction(Session session, int warehouseId, int carrierId) {
        super(session);
        this.warehouseId = warehouseId;
        this.carrierId = carrierId;
    }

    public void execute() {
        QueryFormatter queryFormatter = new QueryFormatter();

        List<Row> res;
        /*
        (a) Let N denote the value of the smallest order number O ID for district (W ID,DISTRICT NO)
        with O CARRIER ID = null; i.e.,
                N = min{t.O ID ∈ Order | t.O W ID = W ID, t.D ID = DISTRICT NO, t.O CARRIER ID = null}
        Let X denote the order corresponding to order number N, and let C denote the customer
        who placed this order
         */

        for (int districtNo = 1; districtNo <= 10; districtNo++) {
            res = executeQuery(queryFormatter.getOrderToDeliverInDistrict(warehouseId, districtNo));
            int orderId = res.get(0).getInt("O_ID");
            int customerId = res.get(0).getInt("O_C_ID");

            /*
            (b) Update the order X by setting O CARRIER ID to CARRIER ID
             */
            executeQuery(queryFormatter.updateCarrierIdInOrder(warehouseId, districtNo, orderId, carrierId));

            /*
            (c) Update all the order-lines in X by setting OL DELIVERY D to the current date and time
             */
            executeQuery(queryFormatter.updateDeliveryDateInOrderLine(warehouseId, districtNo, orderId));

            /*
            (d) Update customer C as follows:
            • Increment C BALANCE by B, where B denote the sum of OL AMOUNT for all the
            items placed in order X
            • Increment C DELIVERY CNT by 1
             */

            res = executeQuery(queryFormatter.getOrderTotalPrice(warehouseId, districtNo, orderId));
            double totalPrice = res.get(0).getDouble("total_price");

            executeQuery(queryFormatter.updateCustomerDeliveryInfo(warehouseId, districtNo, orderId, totalPrice))

        }
    }

}
