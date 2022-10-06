package transaction;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import jnr.ffi.annotations.In;
import util.FieldConstants;
import util.OutputFormatter;
import util.PreparedQueries;

public class PopularItemTransaction extends AbstractTransaction {
    /**
     * This transaction finds the most popular item(s) in each of the last L orders at a specified warehouse
     * district. Given two items X and Y in the same order O, X is defined to be more popular than Y in O
     * if the quantity ordered for X in O is greater than the quantity ordered for Y in O.
     * Inputs:
     * 1. Warehouse number W ID
     * 2. District number D ID
     * 3. Number of last orders to be examined L
     * Processing steps:
     * 1. Let N denote the value of the next available order number D NEXT O ID for district (W ID,D ID)
     * 2. Let S denote the set of last L orders for district (W ID,D ID); i.e.,
     * S = {t.O ID | t ∈ Order, t.O D ID = D ID, t.O W ID = W ID, t.O ID ∈ [N − L, N)}
     * 3. For each order number x in S
     * (a) Let Ix denote the set of order-lines for this order; i.e.,
     * Ix = {t ∈ Order-Line | t.OL O ID = x, t.OL D ID = D ID, t.OL W ID = W ID}
     * (b) Let Px ⊆ Ix denote the subset of popular items in Ix; i.e.,
     * t ∈ Px ⇐⇒ ∀ t′ ∈ Ix, t′.OL QUANTITY ≤ t.OL QUANTITY
     * Output the following information:
     * 1. District identifier (W ID, D ID)
     * 2. Number of last orders to be examined L
     * 3. For each order number x in S
     * (a) Order number O ID & entry date and time O ENTRY D
     * (b) Name of customer who placed this order (C FIRST, C MIDDLE, C LAST),
     * (c) For each popular item in Px
     * i. Item name I NAME
     * ii. Quantity ordered OL QUANTITY
     * 4. The percentage of examined orders that contain each popular item
     * (a) For each distinct popular item t ∈ Px, x ∈ S
     * i. Item name I NAME
     * ii. The percentage of orders in S that contain the popular item t
     */
    private int warehouseId;
    private int districtId;
    private int lastOrderToBeExamined;
    private OutputFormatter outputFormatter = new OutputFormatter();
    private static final String delimiter = "\n";

    public PopularItemTransaction (Session session, int warehouseId, int districtId, int lastOrderToBeExamined) {
        super(session);
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.lastOrderToBeExamined = lastOrderToBeExamined;
    }

    public void execute() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.toString());
        builder.append(delimiter);
        // 1. Let N denote the value of the next available order number D_NEXT_O_ID for district (W ID,D ID)
        List<Row> result = executeQuery(PreparedQueries.getNextAvailableOrderNumber, warehouseId, districtId);
        int N = result.get(0).getInt(FieldConstants.nextAvailableOrderNumberField);

        // 2. Let S denote the set of last L orders for district (W ID,D ID); i.e.,
        //     S = {t.O ID | t ∈ Order, t.O D ID = D ID, t.O W ID = W ID, t.O ID ∈ [N − L, N)}
        List<Row> resultS = executeQuery(PreparedQueries.getLastOrdersInfoForDistrict, warehouseId, districtId,
                N - lastOrderToBeExamined, N);

        Map<Integer, String> popularItemsMap = new HashMap<>();

        for (Row orderInfo : resultS) {
            int orderId = orderInfo.getInt(FieldConstants.orderIdField);
            Date timestamp = orderInfo.getTimestamp(FieldConstants.orderEntryTimestampField);
            builder.append(outputFormatter.formatOrderIdAndTimestamp(orderId, timestamp));
            builder.append(delimiter);

            int customerId = orderInfo.getInt(FieldConstants.orderCustomerIdField);
            Row customerInfo = executeQuery(PreparedQueries.getCustomerName, warehouseId, districtId, customerId).get(0);
            builder.append(outputFormatter.formatCustomerName(customerInfo));
            builder.append(delimiter);

            double maxQuantity = executeQuery(PreparedQueries.getMaxOLQuantity, orderId, districtId, warehouseId)
                    .get(0).getDecimal(0).doubleValue();
            List<Row> getPopularItemsResult = executeQuery(PreparedQueries.getPopularItems, orderId, districtId, warehouseId, maxQuantity);

            builder.append("Popular items:");
            builder.append(delimiter);
            for (Row popularItem : getPopularItemsResult) {
                int itemId = popularItem.getInt(FieldConstants.orderLineItemIdField);
                String itemName = executeQuery(PreparedQueries.getItemNameById, itemId).get(0).getString(FieldConstants.itemNameField);
                popularItemsMap.put(itemId, itemName);
                builder.append("\t");
                builder.append(outputFormatter.formatPopularItemQuantity(itemName, maxQuantity));
                builder.append(delimiter);
            }
        }

        System.out.print(builder.toString());
        builder = new StringBuilder();

        int totalOrders = resultS.size();
        for (Map.Entry<Integer, String> entry : popularItemsMap.entrySet()) {
            int itemId = entry.getKey();
            int count = 0;
            for (Row orderInfo : resultS) {
                int orderId = orderInfo.getInt(FieldConstants.orderIdField);
                Row r = executeQuery(PreparedQueries.checkItemExistInOrder, warehouseId, districtId, orderId, itemId).get(0);
                boolean isItemExistInOrder = r.getBool(0);
                if (isItemExistInOrder) {
                    ++count;
                }
            }
            double ratio = (double) (count) / totalOrders;
            builder.append(outputFormatter.formatPopularItemRatio(entry.getValue(), ratio));
            builder.append(delimiter);
        }

        System.out.print(builder.toString());
    }

    public String toString() {
        return String.format("warehouseId: %d, districtId: %d, number of last orders to be examined: %d", warehouseId, districtId, lastOrderToBeExamined);
    }
}
