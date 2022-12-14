package transaction;

import java.util.List;
import java.util.StringJoiner;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import util.FieldConstants;
import util.OutputFormatter;
import util.PreparedQueries;

public class StockLevelTransaction extends AbstractTransaction {
    /**
     * This transaction examines the items from the last L orders at a specified warehouse district and reports
     * the number of those items that have a stock level below a specified threshold.
     * Inputs:
     * 1. Warehouse number W ID
     * 2. District number D ID
     * 3. Stock threshold T
     * 4. Number of last orders to be examined L
     * Processing steps:
     * 1. Let N denote the value of the next available order number D_NEXT_O_ID for district (W_ID,D_ID)
     * 2. Let S denote the set of items from the last L orders for district (W_ID,D_ID); i.e.,
     * S = {t.OL I ID | t ā Order-Line, t.OL D ID = D ID, t.OL W ID = W ID, t.OL O ID ā [NāL, N)}
     * 3. Output the total number of items in S where its stock quantity at W ID is below the threshold;
     * i.e., S QUANT IT Y < T
     */
    private final int warehouseId;
    private final int districtId;
    private final double threshold;
    private final int lastOrderToBeExamined;
    private final OutputFormatter outputFormatter = new OutputFormatter();

    public StockLevelTransaction(CqlSession session, int warehouseId, int districtId, double threshold, int lastOrderToBeExamined) {
        super(session);
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.threshold = threshold;
        this.lastOrderToBeExamined = lastOrderToBeExamined;
    }

    public void execute() {
        // 1. Let N denote the value of the next available order number D_NEXT_O_ID for district (W_ID,D_ID)
        List<Row> result = executeQuery(PreparedQueries.getNextAvailableOrderNumber, warehouseId, districtId);
        int N = result.get(0).getInt(FieldConstants.nextAvailableOrderNumberField);

        // 2. Let S denote the set of items from the last L orders for district (W_ID,D_ID); i.e.,
        //     *  S = {t.OL_I_ID | t ā Order-Line, t.OL_D_ID = D_ID, t.OL_W_ID = W_ID, t.OL_O_ID ā [NāL, N)}
        result = executeQuery(PreparedQueries.getLastLOrderLinesItemIdForDistrict, warehouseId, districtId, N - lastOrderToBeExamined, N);

        // 3. Output the total number of items in S where its stock quantity at W ID is below the threshold;
        long res = 0;
        StringJoiner itemIds = new StringJoiner(",");
        for (Row orderLineInfo : result) {
            int itemId = orderLineInfo.getInt(FieldConstants.orderLineItemIdField);
            itemIds.add(String.valueOf(itemId));
        }

        result = executeQueryString(String.format(PreparedQueries.getStockQuantitiesFromWarehouseItems, warehouseId, itemIds.toString()));
        for (Row stockInfo : result) {
            double quantity = stockInfo.getBigDecimal(FieldConstants.stockQuantityField).doubleValue();
            if (quantity < threshold) {
                ++res;
            }
        }

        String sb = "********** Stock Level Transaction *********\n" +
                outputFormatter.formatStockLevelTransactionOutput(res, this.toString());
        System.out.println(sb);
    }

    public String toString() {
        return String.format("Stock Level Transaction info: W_ID: %d, D_ID: %d. threshold: %f, L: %d",
                warehouseId, districtId, threshold, lastOrderToBeExamined);
    }
}
