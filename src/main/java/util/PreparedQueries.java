package util;

public class PreparedQueries {
    // For payment transactions
    public final static String updateWarehouseYearToDateAmount = """
                UPDATE warehouse
                SET W_YTD = W_YTD + ?
                WHERE W_ID = ?;
                """;

    public final static String updateDistrictYearToDateAmount = """
                UPDATE district
                SET D_YTD = D_YTD + ?
                WHERE D_W_ID = ? AND D_ID = ?;
                """;

    public final static String updateCustomerPaymentInfo = """
                UPDATE customer
                SET C_BALANCE = C_BALANCE - ?, C_YTD_PAYMENT = C_YTD_PAYMENT + ?, C_PAYMENT_CNT = C_PAYMENT_CNT + 1
                WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;
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
}
