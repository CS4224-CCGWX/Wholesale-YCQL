package transaction;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import util.OutputFormatter;
import util.PreparedQueries;
import util.QueryFormatter;
import util.TimeFormatter;

public class TopBalanceTransaction extends AbstractTransaction {
    static final int K = 10;

    public TopBalanceTransaction(Session session) {
        super(session);
    }

    public void execute() {
        /*
        This transaction finds the top-10 customers ranked in descending order of their outstanding balance payments.
        Processing steps:
            1. Let C ⊆ Customer denote the subset of 10 customers (i.e., |C| = 10)
            such that for each pair of customers (x, y), where x ∈ C and y ∈ Customer - C, we have x.C BALANCE ≥ y.C BALANCE.

        Output the following information:
            1. For each customer in C ranked in descending order of C BALANCE:
                (a) Name of customer (C FIRST, C MIDDLE, C LAST)
                (b) Balance of customer’s outstanding payment C BALANCE
                (c) Warehouse name of customer W NAME
                (d) District name of customer D NAME
         */
        OutputFormatter outputFormatter = new OutputFormatter();
        List<Row> customersInfo = this.executeQuery(PreparedQueries.getTopKBalanceCustomers, K);
        System.out.println("*** Top Balance Transaction Summary ***");
        for(Row cInfo : customersInfo) {
            int warehouseId = cInfo.getInt("C_W_ID");
            int districtId = cInfo.getInt("C_D_ID");
            String warehouseName = this.executeQuery(PreparedQueries.getWarehouseName, warehouseId).get(0).getString("W_NAME");
            String districtName = this.executeQuery(PreparedQueries.getDistrictName, warehouseId, districtId).get(0).getString("D_NAME");
            System.out.println(outputFormatter.formatTopBalanceCustomerInfo(cInfo, warehouseName, districtName));
        }

    }
}