package transaction;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import util.PreparedQueries;
import util.QueryFormatter;
import util.TimeFormatter;


public class NewOrderTransaction extends AbstractTransaction {
    private static final int STOCK_REFILL_THRESHOLD = 10;
    private static final int STOCK_REFILL_QTY = 100;

    private final int customerId;
    private final int warehouseId;
    private final int districtId;
    private final int nOrderLines;
    private final List<Integer> itemIds;
    private final List<Integer> quantities;
    private final List<Integer> supplyWarehouseIds;

    public NewOrderTransaction(CqlSession session, int cid, int wid, int did, int n,
                               List<Integer> itemIds, List<Integer> supplyWarehouseIds, List<Integer> quantities) {
        super(session);
        customerId = cid;
        warehouseId = wid;
        districtId = did;
        nOrderLines = n;
        this.itemIds = itemIds;
        this.quantities = quantities;
        this.supplyWarehouseIds = supplyWarehouseIds;
    }

    public void execute() {
        QueryFormatter queryFormatter = new QueryFormatter();
        List<Row> res;
        /*
          1. N denotes the next available order number D_NEXT_O_ID for district (W_ID, D_ID)
          Update district (W_ID, D_ID) by incrementing D_NEXT_O_ID by 1.
         */
        res = this.executeQuery(PreparedQueries.getDistrictNextOrderIdAndTax, warehouseId, districtId);
        Row districtInfo = res.get(0);
        int orderId = districtInfo.getInt("D_NEXT_O_ID");
        this.executeQuery(PreparedQueries.incrementDistrictNextOrderId, warehouseId, districtId);

        /*
          2. Create new order with:
          O_ID = N
          O_D_ID = D_ID
          O_W_ID = W_ID
          O_C_ID = C_ID
          O_ENTRY_D = Current date and time
          O_CARRIER_ID = null
          O_OL_CNT = NUM_ITEMS
          O_ALL_LOCAL = 0 if exists i in [1, NUM_ITEMS] such that SUPPLIER_WAREHOUSE[i] != W_ID;
                          otherwise O_ALL_LOCAL = 1
         */
        int isAllLocal = 1;
        for (int supplyWarehouseId : supplyWarehouseIds) {
            if (supplyWarehouseId != warehouseId) {
                isAllLocal = 0;
                break;
            }
        }
        Date orderDateTime = TimeFormatter.getCurrentDate();
        this.executeQuery(PreparedQueries.createNewOrder, orderId, districtId, warehouseId, customerId, orderDateTime.toInstant(), nOrderLines, isAllLocal);

        /*
          3. Initialize TOTAL_AMOUNT = 0
          For i = [1...NUM_ITEMS],
         */
        double totalAmount = 0;
        List<Integer> adjustQuantities = new ArrayList<>();
        List<Double> itemAmounts = new ArrayList<>();
        List<String> itemNames = new ArrayList<>();
        for (int i = 0; i < nOrderLines; ++i) {
            int itemId = itemIds.get(i);
            int supplyWarehouseId = supplyWarehouseIds.get(i);
            int quantity = quantities.get(i);

            /*
              3.1. S_QUANTITY = stock quantity of itemIds[i] and supplyWarehouseIds[i]
              ADJUST_QTY = S_QUANTITY - quantities[i]
              if ADJUST_QTY < 10, then ADJUST_QTY += 100
             */
            Row qtyInfo = this.executeQuery(PreparedQueries.getStockQty, supplyWarehouseId, itemId).get(0);
            int stockQty = qtyInfo.getBigDecimal("S_QUANTITY").intValue();
            int stockYtd = qtyInfo.getBigDecimal("S_YTD").intValue();
            int adjustQty = stockQty - quantity;
            if (adjustQty < STOCK_REFILL_THRESHOLD) {
                adjustQty += STOCK_REFILL_QTY;
            }
            adjustQuantities.add(adjustQty);

            /*
            3.2. Update stock for (itemIds[i], supplyWarehouseIds[i]):
              - update S_QUANTITY to ADJUST_QUANTITY
              - increment S_YTD by quantities[i]
              - increment S_ORDER_CNT by 1
              - Increment S_REMOTE_CNT by 1 if supplyWarehouseIds[i] != warehouseID
             */
            stockYtd += quantity;
            if (supplyWarehouseId != warehouseId) {
                this.executeQuery(PreparedQueries.updateStockQtyIncrRemoteCnt,
                        BigDecimal.valueOf(adjustQty), BigDecimal.valueOf(stockYtd), supplyWarehouseId, itemId);
            } else {
                this.executeQuery(PreparedQueries.updateStockQty,
                        BigDecimal.valueOf(adjustQty), BigDecimal.valueOf(stockYtd), supplyWarehouseId, itemId);
            }

            /*
              3.3. ITEM_AMOUNT = quantities[i] * I_PRICE, where I_PRICE is price of itemIds[i]
              TOTAL_AMOUNT += ITEM_AMOUNT
             */
            Row itemInfo = this.executeQuery(PreparedQueries.getItemPriceAndName, itemId).get(0);
            double price = itemInfo.getBigDecimal("I_PRICE").doubleValue();
            double itemAmount = quantity * price;
            itemAmounts.add(itemAmount);
            itemNames.add(itemInfo.getString("I_NAME"));
            totalAmount += itemAmount;

            /*
              3.4. Create a new order line
              - OL_O_ID = N
              - OL_D_ID = D_ID
              - OL_W_ID = W_ID
              - OL_NUMBER = i
              - OL_I_ID = itemIds[i]
              - OL_SUPPLY_W_ID = supplyWarehouseIds[i]
              - OL_QUANTITY = quantities[i]
              - OL_AMOUNT = ITEM_AMOUNT
              - OL_DELIVERY_D = null
              - OL_DIST_INFO = S_DIST_xx where xx=D_ID
             */
            String distIdStr = queryFormatter.distIdStr(districtId);
            res = this.executeQuery(String.format(PreparedQueries.getStockDistInfo, distIdStr), warehouseId, itemId);
            String distInfo = res.get(0).getString(0);
            this.executeQuery(PreparedQueries.createNewOrderLine,
                    orderId, districtId, warehouseId, customerId,
                    i, itemId, supplyWarehouseId, BigDecimal.valueOf(quantity),
                    BigDecimal.valueOf(itemAmount), distInfo);
        }

        /*
          4. TOTAL_AMOUNT = TOTAL_AMOUNT × (1+D_TAX +W_TAX) × (1−C_DISCOUNT),
          where W_TAX is the tax rate for warehouse W_ID,
          D_TAX is the tax rate for district (W_ID, D_ID),
          and C_DISCOUNT is the discount for customer C_ID.
         */
        double dTax = districtInfo.getBigDecimal("D_TAX").doubleValue();
        res = this.executeQuery(PreparedQueries.getWarehouseTax, warehouseId);
        double wTax = res.get(0).getBigDecimal("W_TAX").doubleValue();
        res = this.executeQuery(PreparedQueries.getCustomerLastAndCreditAndDiscount, warehouseId, districtId, customerId);
        Row cInfo = res.get(0);
        double cDiscount = cInfo.getBigDecimal("C_DISCOUNT").doubleValue();
        totalAmount = totalAmount * (1 + dTax + wTax) * (1 - cDiscount);

        /*
        Output following info:
        1. Customer identifier (W ID, D ID, C ID), lastname C LAST, credit C CREDIT, discount C DISCOUNT
        2. Warehouse tax rate W TAX, District tax rate D TAX
        3. Order number O ID, entry date O ENTRY D
        4. Number of items NUM ITEMS, Total amount for order TOTAL AMOUNT
        5. For each ordered item ITEM NUMBER[i], i ∈ [1,NUM ITEMS]
        (a) ITEM NUMBER[i] (b) I NAME
        (c) SUPPLIER WAREHOUSE[i] (d) QUANTITY[i]
        (e) OL AMOUNT (f) S QUANTITY
         */
        String cLast = cInfo.getString("C_LAST");
        String cCredit = cInfo.getString("C_CREDIT");
        System.out.println("*** New Order Transaction Summary ***");
        System.out.printf(
                "Customer ID: (%d, %d, %d), Last name:%s, Credit:%s, Discount:%.4f\n",
                warehouseId, districtId, customerId, cLast, cCredit, cDiscount);
        System.out.printf("Warehouse tax:%.4f, District tax:%.4f\n", wTax, dTax);
        System.out.printf("Order ID:%d, Order entry date:%s\n", orderId, TimeFormatter.formatTime(orderDateTime));
        System.out.printf("#Items:%d, Total amount:%.2f\n", nOrderLines, totalAmount);
        System.out.println("Items information:");
        for (int i = 0; i < nOrderLines; ++i) {
            System.out.printf(
                    "- Item number: %d, Name: %s, Supply warehouse: %d, Quantity: %d, Order-line amount: %.2f, Adjusted quantity: %d\n",
                    itemIds.get(i), itemNames.get(i), supplyWarehouseIds.get(i), quantities.get(i), itemAmounts.get(i), adjustQuantities.get(i));
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("*** New Order Transaction Information ***\n");
        sb.append(String.format("CID:%d, WID:%d, DID:%d, num order-lines:%d\n", customerId, warehouseId, districtId, nOrderLines));
        sb.append("Item IDs:");
        for (int id : itemIds) {
            sb.append(",").append(id);
        }
        sb.append("\n");
        sb.append("Quantities:");
        for (int qty : quantities) {
            sb.append(",").append(qty);
        }
        sb.append("\n");
        sb.append("Supply warehouse IDs:");
        for (int id : supplyWarehouseIds) {
            sb.append(",").append(id);
        }
        sb.append("\n");

        return sb.toString();
    }
}
