package transaction;

import java.util.List;
import java.util.PriorityQueue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.Row;

import util.Customer;
import util.OutputFormatter;
import util.PreparedQueries;


public class TopBalanceTransaction extends AbstractTransaction {
    static final int RANK_LIMIT = 10;
    static final int N_WAREHOUSE = 10;
    static final int N_DISTRICT = 10;

    public TopBalanceTransaction(CqlSession session) {
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

        PriorityQueue<Customer> customersPQ = new PriorityQueue<>();
        BatchStatement batchQueries = BatchStatement.newInstance(DefaultBatchType.LOGGED);
        // Go to every partition and use the local index to get local top 10, put in MAX PQ.
        for (int w_id = 1; w_id < N_WAREHOUSE; ++w_id) {
            for (int d_id = 1; d_id < N_DISTRICT; ++d_id) {
//                List<Row> results = this.executeQuery(PreparedQueries.getTopKBalanceCustomers, w_id, d_id, RANK_LIMIT);
//                for (Row row : results) {
//                    customersPQ.add(new Customer(row));
//                }
                batchQueries.add(this.bindPreparedQuery(PreparedQueries.getTopKBalanceCustomers, w_id, d_id, RANK_LIMIT));
            }
        }
        List<Row> results = this.executeBatch(batchQueries);
        for(Row row : results) {
            customersPQ.add(new Customer(row));
        }
        // Get global top 10.
        Customer[] topCustomers = new Customer[RANK_LIMIT];
        for (int i = 0; i < RANK_LIMIT; ++i) {
            topCustomers[i] = customersPQ.poll();
        }

        System.out.println("*** Top Balance Transaction Summary ***");
        System.out.printf("Top %d balance customers:%n", RANK_LIMIT);
        for (Customer customer : topCustomers) {
            int warehouseId = customer.w_id;
            int districtId = customer.d_id;
            String warehouseName = this.executeQuery(PreparedQueries.getWarehouseName, warehouseId).get(0).getString("W_NAME");
            String districtName = this.executeQuery(PreparedQueries.getDistrictName, warehouseId, districtId).get(0).getString("D_NAME");
            /*
            (a) Name of customer (C FIRST, C MIDDLE, C LAST)
            (b) Balance of customer’s outstanding payment C BALANCE
            (c) Warehouse name of customer W NAME
            (d) District name of customer D NAME
             */
            String cInfo = String.format("- Customer: (%s, %s, %s), Balance: %.2f, Warehouse: %s, District: %s",
                    customer.first, customer.middle, customer.last,
                    customer.balance, warehouseName, districtName);
            System.out.println(cInfo);
        }
    }

    public String toString() {
        return "*** Top Balance Transaction ***";
    }
}
