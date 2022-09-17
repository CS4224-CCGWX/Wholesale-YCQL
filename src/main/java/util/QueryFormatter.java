package util;

public class QueryFormatter {

    // For NewOrderTransaction
    public String getDistrictNextOrderId(int warehouseId, int districtId) {
        return String.format(
                """
                SELECT D_NEXT_O_ID
                FROM district
                WHERE D_W_ID=%d AND D_ID=%d;
                """,
                warehouseId, districtId);
    }

    public String getDistrictTax(int warehouseId, int districtId) {
        return String.format(
                """
                SELECT D_TAX
                FROM district
                WHERE D_W_ID=%d, D_ID=%d;
                """,
                warehouseId, districtId
        );
    }

    public String getDistrictNextOrderIdAndTax(int warehouseId, int districtId) {
        // combine getting D_NEXT_O_ID and D_TAX into 1 query.
        return String.format(
                """
                SELECT D_NEXT_O_ID, D_TAX
                FROM district
                WHERE D_W_ID=%d AND D_ID=%d;
                """,
                warehouseId, districtId);
    }

    public String getIncrementDistrictNextOrderId(int warehouseId, int districtId) {
        return String.format(
                """
                UPDATE district
                SET D_NEXT_O_ID=D_NEXT_O_ID+1
                WHERE D_W_ID=%d AND D_ID=%d;
                """,
                warehouseId, districtId);
    }

    public String createNewOrder(
            int orderId, int districtId, int warehouseId, int customerId,
            String dateTime, int nOrderLines, int isAllLocal) {
        return String.format(
                """
                INSERT INTO "order"
                (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)
                VALUES (%d, %d, %d, %d, %s, %d, %d);
                """,
                orderId, districtId, warehouseId, customerId, dateTime, nOrderLines, isAllLocal);
    }

    public String getStockQty(int supplyWarehouseId, int itemId) {
        return String.format(
                """
                SELECT S_QUANTITY
                FROM stock
                WHERE S_W_ID=%d, S_I_ID=%d;
                """,
                supplyWarehouseId, itemId);
    }

    public String updateStockQty(int adjustQty, int quantity, int supplyWarehouseId, int itemId, boolean incrementRemoteCnt) {
        if(incrementRemoteCnt) {
            return String.format(
                    """
                    UPDATE stock
                    SET S_QUANTITY=%d, S_YTD=S_YTD+%d, S_ORDER_CNT=S_ORDER_CNT+1, S_REMOTE_CNT=S_REMOTE_CNT+1
                    WHERE S_W_ID=%d, S_I_ID=%d;
                    """,
                    adjustQty, quantity, supplyWarehouseId, itemId);
        } else {
            return String.format(
                    """
                    UPDATE stock
                    SET S_QUANTITY=%d, S_YTD=S_YTD+%d, S_ORDER_CNT=S_ORDER_CNT+1
                    WHERE S_W_ID=%d, S_I_ID=%d;
                    """,
                    adjustQty, quantity, supplyWarehouseId, itemId);
        }
    }

    public String getItemInfo(int itemId) {
        return String.format(
                """
                SELECT I_PRICE, I_NAME
                FROM item
                WHERE I_ID=%d;
                """,
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
                """
                SELECT S_DIST_%s
                FROM stock
                WHERE S_W_ID=%d, S_I_ID=%d;
                """,
                distIdStr(distId), warehouseId, itemId
        );
    }

    public String createNewOrderLine(
            int districtId, int warehouseId, int orderLineNumber, int itemId,
            int supplyWarehouseId, double itemAmount, String distInfo) {
        return String.format(
                """
                INSERT INTO order_line
                (OL_O_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_AMOUNT, OL_DIST_INFO)
                VALUES (%d, %d, %d, %d, %d, %f, %s);
                """,
                districtId, warehouseId, orderLineNumber, itemId, supplyWarehouseId, itemAmount, distInfo
                );
    }

    public String getWarehouseTax(int warehouseId) {
        return String.format(
                """
                SELECT W_TAX
                FROM warehouse
                WHERE W_ID=%d;
                """,
                warehouseId
        );
    }

    public String getCustomerInfo(int warehouseId, int districtId, int customerId) {
        return String.format(
                """
                SELECT C_LAST, C_CREDIT, C_DISCOUNT
                FROM customer
                WHERE C_W_ID=%d, C_D_ID=%d, C_ID=%d;
                """,
                warehouseId, districtId, customerId
        );
    }

    // For DeliveryTransaction
    public String getOrderToDeliverInDistrict(int warehouseId, int districtId) {
        // get order_id, customer_id
        return String.format("""
                        SELECT O_ID, O_C_ID from "order"
                        WHERE O_W_ID = %d AND O_D_ID = %d AND O_CARRIER_ID IS NULL
                        ORDER BY O_ID
                        LIMIT 1;
                        """,
                warehouseId, districtId);
    }

    public String updateCarrierIdInOrder(int warehouseId, int districtId, int orderId, int newCarrierId) {
        return String.format("""
                UPDATE "order"
                SET O_CARRIER_ID = %d
                WHERE O_W_ID = %d AND O_D_ID = %d AND O_ID = %d;
                """,
                newCarrierId, warehouseId, districtId, orderId);
    }

    public String updateDeliveryDateInOrderLine(int warehouseId, int districtId, int orderId) {
        return String.format("""
                UPDATE order_line
                SET OL_DELIVERY_D = %s
                WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d;
                """,
                TimeFormatter.getCurrentTimestamp(), warehouseId, districtId, orderId);
    }

    public String getOrderTotalPrice(int warehouseId, int districtId, int orderId) {
        return String.format("""
                SELECT sum(OL_AMOUNT) as total_price
                FROM order_line
                WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d;
                """,
                warehouseId, districtId, orderId);
    }

    public String updateCustomerDeliveryInfo(int warehouseId, int districtId, int customerId, double totalCost) {
        return String.format("""
                UPDATE customer
                SET C_DELIVERY_CNT = C_DELIVERY_CNT + 1, C_BALANCE = C_BALANCE + %f
                WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;
                """,
                totalCost, warehouseId, districtId, customerId);
    }

    // For payment transaction
    public String updateWarehouseYearToDateAmount(int warehouseId, double payment) {
        return String.format("""
                UPDATE warehouse
                SET W_YTD = W_YTD + %f
                WHERE W_ID = %d;
                """, payment, warehouseId);
    }

    public String updateDistrictYearToDateAmount(int warehouseId, int districtId, double payment) {
        return String.format("""
                UPDATE district
                SET D_YTD = D_YTD + %f
                WHERE D_W_ID = %d AND D_ID = %d;
                """, payment, warehouseId, districtId);
    }

    public String updateCustomerPaymentInfo(int warehouseId, int districtId, int customerId, double payment) {
        return String.format("""
                UPDATE customer
                SET C_BALANCE = C_BALANCE - %f, C_YTD_PAYMENT = C_YTD_PAYMENT + %f, C_PAYMENT_CNT = C_PAYMENT_CNT + 1
                WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;
                """, payment, payment, warehouseId, districtId, customerId);
    }

    public String getFullCustomerInfo(int warehouseId, int districtId, int customerId) {
        return String.format("""
                SELECT C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,
                C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE
                FROM customer WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;
                """, warehouseId, districtId, customerId);
    }

    public String getWarehouseAddress(int warehouseId) {
        return String.format("""
                SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP
                FROM warehouse WHERE W_ID = %d;
                """, warehouseId);
    }

    public String getDistrictAddress(int warehouseId, int districtId) {
        return String.format("""
                SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP
                FROM district WHERE D_W_ID = %d AND D_ID = %d;
                """, warehouseId, districtId);
    }
}
