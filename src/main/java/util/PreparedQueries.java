package util;

public class PreparedQueries {
    // For NewOrderTransaction
    public final static String getDistrictNextOrderId = """
                SELECT D_NEXT_O_ID
                FROM district
                WHERE D_W_ID = ? AND D_ID = ?;
                """;

    public final static String getDistrictTax = """
                SELECT D_TAX
                FROM district
                WHERE D_W_ID = ?, D_ID = ?;
                """;

    public final static String getDistrictNextOrderIdAndTax = """
                SELECT D_NEXT_O_ID, D_TAX
                FROM district
                WHERE D_W_ID = ? AND D_ID = ?;
                """;

    public final static String incrementDistrictNextOrderId = """
                UPDATE district
                SET D_NEXT_O_ID=D_NEXT_O_ID+1
                WHERE D_W_ID = ? AND D_ID = ?;
                """;

    public final static String createNewOrder = """
                INSERT INTO "order"
                (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """;

    public final static String getStockQty = """
                SELECT S_QUANTITY
                FROM stock
                WHERE S_W_ID = ?, S_I_ID = ?;
                """;

    // update stock qty that increments remote count
    public final static String updateStockQtyIncrRemoteCnt = """
                UPDATE stock
                SET S_QUANTITY = ?, S_YTD = S_YTD + ?, S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + 1
                WHERE S_W_ID = ?, S_I_ID = ?;
                """;

    // update stock qty that NOT increments remote count
    public final static String updateStockQty = """
                UPDATE stock
                SET S_QUANTITY = ?, S_YTD = S_YTD + ?, S_ORDER_CNT = S_ORDER_CNT + 1
                WHERE S_W_ID = ?, S_I_ID = ?;
                """;

    public final static String getItemPriceAndName = """
                SELECT I_PRICE, I_NAME
                FROM item
                WHERE I_ID = ?;
                """;

    public final static String getStockDistInfo = """
                SELECT S_DIST_?
                FROM stock
                WHERE S_W_ID = ?, S_I_ID = ?;
                """;

    public final static String createNewOrderLine = """
                INSERT INTO order_line
                (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """;

    public final static String getWarehouseTax = """
                SELECT W_TAX
                FROM warehouse
                WHERE W_ID = ?;
                """;

    public final static String getCustomerLastAndCreditAndDiscount = """
                SELECT C_LAST, C_CREDIT, C_DISCOUNT
                FROM customer
                WHERE C_W_ID = ?, C_D_ID = ?, C_ID = ?;
                """;

    // For payment transactions
    public final static String updateWarehouseYearToDateAmount = """
                BEGIN TRANSACTION
                UPDATE warehouse
                SET W_YTD = W_YTD + ?
                WHERE W_ID = ?
                END TRANSACTION;
                """;

    public final static String updateDistrictYearToDateAmount = """
                BEGIN TRANSACTION
                UPDATE district
                SET D_YTD = D_YTD + ?
                WHERE D_W_ID = ? AND D_ID = ?
                END TRANSACTION;
                """;

    public final static String updateCustomerPaymentInfo = """
                BEGIN TRANSACTION
                UPDATE customer
                SET C_BALANCE = C_BALANCE - ?, C_YTD_PAYMENT = C_YTD_PAYMENT + ?, C_PAYMENT_CNT = C_PAYMENT_CNT + 1
                WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
                END TRANSACTION;
                """;

    public final static String getFullCustomerInfo = """
                SELECT C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,
                C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE
                FROM customer WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;
                """;

    public final static String getWarehouseAddress = """
                SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP
                FROM warehouse WHERE W_ID = ?;
                """;

    public final static String getDistrictAddress = """
                SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP
                FROM district WHERE D_W_ID = ? AND D_ID = ?;
                """;

    // For order status transaction
    public final static String getCustomerFullNameAndBalance = """
            SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE
            FROM customer
            WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;
            """;

    public final static String getCustomerLastOrderInfo = """
            SELECT O_ID, O_CARRIER_ID, O_ENTRY_D
            FROM "order"
            WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ?
            ORDER BY O_ID DESC
            LIMIT 1;
            """;  // "order" is partitioned by O_W_ID and O_D_ID, and sort by O_ID.

    public final static String getCustomerLastOrderItemsInfo = """
            SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D
            FROM order_line
            WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?;
            """;

    // For stock level transactions
    public final static String getNextAvailableOrderNumber = """
                SELECT D_NEXT_O_ID
                FROM district WHERE D_W_ID = ? AND D_ID = ?;
                """;

    public final static String getLastLOrderLinesItemIdForDistrict = """
                SELECT OL_I_ID
                FROM order-line WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID >= ? AND OL_O_ID < ?;
                """;

    public final static String getStockQuantityForWarehouseItem = """
                SELECT S_QUANTITY
                FROM stock WHERE S_W_ID = ? AND S_I_ID = ?;
                """;

    // For top balance transaction
    public final static String getTopKBalanceCustomers = """
            SELECT C_W_ID, C_D_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST
            FROM customer
            ORDER BY C_BALANCE DESC
            LIMIT ?;
            """;

    public final static String getWarehouseName = """
            SELECT W_NAME
            FROM warehouse
            WHERE W_ID = ?;
            """;

    public final static String getDistrictName = """
            SELECT D_NAME
            FROM district
            WHERE D_W_ID = ?, D_ID = ?;
            """;

    // For popular item transactions
    public final static String getLastOrdersInfoForDistrict = """
                SELECT O_ID, O_ENTRY_D, O_C_ID
                FROM order WHERE O_W_ID = ? AND O_D_ID = ? AND O_ID >= ? AND O_ID < ?;
                """;

    public final static String getPopularItems = """
                SELECT DISTINCT OL_I_ID
                FROM order-line
                WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ? AND OL_QUANTITY = ?;
                """;

    public final static String getMaxOLQuantity = """
                SELECT max(OL_QUANTITY)
                FROM order-line
                WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?;
                """;

    public final static String getItemNameById = """
                SELECT I_NAME
                FROM item WHERE I_ID = ?;
                """;

    public final static String getCustomerName = """
                SELECT C_FIRST, C_MIDDLE, C_LAST
                FROM customer WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;
                """;

    public final static String checkItemExistInOrder = """
                SELECT 1
                FROM order-line
                WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ? AND OL_I_ID = ?;
                """;
}
