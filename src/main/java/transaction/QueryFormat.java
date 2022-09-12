package transaction;

public class QueryFormat {

    // For NewOrderTransaction
    public String getDistrictNextOrderId(int warehouseId, int districtId) {
        return String.format(
                "SELECT D_NEXT_O_ID from district WHERE D_W_ID=%d AND D_ID=%d;",
                warehouseId, districtId);
    };

    public String getIncrementDistrictNextOrderId(int warehouseId, int districtId) {
        return String.format(
                "UPDATE district SET D_NEXT_O_ID=D_NEXT_O_ID+1 WHERE D_W_ID=%d AND D_ID=%d;",
                warehouseId, districtId);
    }

    public String createNewOrder(
            int orderId, int districtId, int warehouseId, int customerId,
            String date, int nOrderLines, int isAllLocal) {
        return String.format(
                "INSERT INTO \"order\" (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL) " +
                        "VALUES (%d, %d, %d, %d, %s, %d, %d);",
                orderId, districtId, warehouseId, customerId, date, nOrderLines, isAllLocal);
    }

    public String getStockQty(int supplyWarehouseId, int itemId) {
        return String.format(
                "SELECT S_QUANTITY FROM stock WHERE S_W_ID=%d, S_I_ID=%d",
                supplyWarehouseId, itemId);
    }

    public String updateStockQty(int adjustQty, int quantity, int supplyWarehouseId, int itemId, boolean incrementRemoteCnt) {
        if(incrementRemoteCnt) {
            return String.format(
                    "UPDATE stock SET S_QUANTITY=%d, S_YTD=S_YTD+%d, S_ORDER_CNT=S_ORDER_CNT+1, S_REMOTE_CNT=S_REMOTE_CNT+1 WHERE S_W_ID=%d, S_I_ID=%d",
                    adjustQty, quantity, supplyWarehouseId, itemId);
        } else {
            return String.format(
                    "UPDATE stock SET S_QUANTITY=%d, S_YTD=S_YTD+%d, S_ORDER_CNT=S_ORDER_CNT+1 WHERE S_W_ID=%d, S_I_ID=%d",
                    adjustQty, quantity, supplyWarehouseId, itemId);
        }
    }

    public String getItemInfo(int itemId) {
        return String.format(
                "SELECT I_PRICE, I_NAME FROM item WHERE I_ID=%d",
                itemId
        );
    }

    public String distIdStr(int distId) {
        assert(distId >= 1 && distId <= 10);

        if(distId < 10) {
            return "0" + Integer.toString(distId);
        } else {
            return Integer.toString(distId);
        }
    }

    public String getStockDistInfo(int distId, int warehouseId, int itemId) {
        return String.format(
                "SELECT S_DIST_%s FROM district WHERE S_W_ID=%d, S_I_ID=%d",
                distIdStr(distId), warehouseId, itemId
        );
    }

    public String createNewOrderLine(
            int districtId, int warehouseId, int orderLineNumber, int itemId,
            int supplyWarehouseId, double itemAmount, String distInfo) {
        return String.format(
                "INSERT INTO order_line (OL_O_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_AMOUNT, OL_DIST_INFO) " +
                        "VALUES (%d, %d, %d, %d, %d, %f, %s)",
                districtId, warehouseId, orderLineNumber, itemId, supplyWarehouseId, itemAmount, distInfo
                );
    }

    public String getDistrictTax(int warehouseId, int districtId) {
        return String.format(
                "SELECT D_TAX FROM district WHERE D_W_ID=%d, D_ID=%d",
                warehouseId, districtId
        );
    }

    public String getWarehouseTax(int warehouseId) {
        return String.format(
                "SELECT W_TAX FROM warehouse WHERE W_ID=%d",
                warehouseId
        );
    }

    public String getCustomerInfo(int warehouseId, int districtId, int customerId) {
        return String.format(
                "SELECT C_LAST, C_CREDIT, C_DISCOUNT FROM customer WHERE C_W_ID=%d, C_D_ID=%d, C_ID=%d",
                warehouseId, districtId, customerId
        );
    }
}
