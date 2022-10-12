package transaction;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

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
    public NewOrderTransaction(Session session, int cid, int wid, int did, int n,
                               List<Integer> itemIds, List<Integer> quantities, List<Integer> supplyWarehouseIds) {
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
        /*
          1. N denotes the next available order number D_NEXT_O_ID for district (W_ID, D_ID)
          Update district (W_ID, D_ID) by incrementing D_NEXT_O_ID by 1.
         */

        QueryFormatter queryFormatter = new QueryFormatter();

        List<Row> res;
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
        for(int supplyWarehouseId : supplyWarehouseIds) {
            if(supplyWarehouseId != warehouseId) {
               isAllLocal = 0;
               break;
            }
        }
        String orderDateTime = TimeFormatter.getCurrentTimestamp();
        this.executeQuery(PreparedQueries.createNewOrder,orderId, districtId, warehouseId, customerId, orderDateTime, nOrderLines, isAllLocal);

        /*
          3. Initialize TOTAL_AMOUNT = 0
          For i = [1...NUM_ITEMS],
         */
        double totalAmount = 0;
        List<Integer> adjustQuantities = new ArrayList<>();
        List<Double> itemAmounts = new ArrayList<>();
        List<String> itemNames = new ArrayList<>();
        for(int i=0; i < nOrderLines; ++i) {
            int itemId = itemIds.get(i);
            int supplyWarehouseId = supplyWarehouseIds.get(i);
            int quantity = quantities.get(i);

            /*
              3.1. S_QUANTITY = stock quantity of itemIds[i] and supplyWarehouseIds[i]
              ADJUST_QTY = S_QUANTITY - quantities[i]
              if ADJUST_QTY < 10, then ADJUST_QTY += 100
             */
            res = this.executeQuery(PreparedQueries.getStockQty, supplyWarehouseId, itemId);
            int stockQty = res.get(0).getInt(0);

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
            if(supplyWarehouseId != warehouseId) {
                this.executeQuery(PreparedQueries.updateStockQtyIncrRemoteCnt, adjustQty, quantity, supplyWarehouseId, itemId);
            } else {
                this.executeQuery(PreparedQueries.updateStockQty, adjustQty, quantity, supplyWarehouseId, itemId);
            }
            /*
              3.3. ITEM_AMOUNT = quantities[i] * I_PRICE, where I_PRICE is price of itemIds[i]
              TOTAL_AMOUNT += ITEM_AMOUNT
             */
            Row itemInfo = this.executeQuery(PreparedQueries.getItemPriceAndName, itemId).get(0);
            double price = itemInfo.getDouble("I_PRICE");
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
            res = this.executeQuery(PreparedQueries.getStockDistInfo, distIdStr, warehouseId, itemId);
            String distInfo = res.get(0).getString(0);
            this.executeQuery(PreparedQueries.createNewOrderLine,
                    orderId, districtId, warehouseId, i, itemId, supplyWarehouseId, quantity, itemAmount, distInfo);
        }

        /*
          4. TOTAL_AMOUNT = TOTAL_AMOUNT × (1+D_TAX +W_TAX) × (1−C_DISCOUNT),
          where W_TAX is the tax rate for warehouse W_ID,
          D_TAX is the tax rate for district (W_ID, D_ID),
          and C_DISCOUNT is the discount for customer C_ID.
         */
        double dTax = districtInfo.getDecimal("D_TAX").doubleValue();
        res = this.executeQuery(PreparedQueries.getWarehouseTax, warehouseId);
        double wTax = res.get(0).getDouble(0);
        res = this.executeQuery(PreparedQueries.getCustomerLastAndCreditAndDiscount, warehouseId, districtId, customerId);
        Row cInfo = res.get(0);
        double cDiscount = cInfo.getDecimal("C_DISCOUNT").doubleValue();

        totalAmount = totalAmount*(1 + dTax + wTax) * (1 - cDiscount);


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
                "1. (%d, %d, %d), C_LAST:%s, C_CREDIT:%s, C_DISCOUNT:%.2f\n",
                warehouseId, districtId, customerId, cLast, cCredit, cDiscount);
        System.out.printf("2. W_TAX:%.2f, D_TAX:%.2f\n", wTax, dTax);
        System.out.printf("3. O_ID:%d, O_ENTRY_D:%s\n", orderId, orderDateTime);
        System.out.printf("4. NUM_ITEMS:%d, TOTAL_AMOUNT:%.2f\n", nOrderLines, totalAmount);
        System.out.println("5. Item Info:");
        for (int i = 0; i < nOrderLines; ++i) {
            System.out.printf(
                    "\t ITEM_NUMBER: %d, I_NAME: %s, SUPPLIER_WAREHOUSE: %d, QUANTITY: %d, OL_AMOUNT: %.2f, S_QUANTITY: %d\n",
                    itemIds.get(i), itemNames.get(i), supplyWarehouseIds.get(i), quantities.get(i), itemAmounts.get(i), adjustQuantities.get(i));
        }
    }
}
