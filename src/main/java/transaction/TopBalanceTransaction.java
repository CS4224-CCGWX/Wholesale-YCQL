package transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.CqlSession;

import util.OutputFormatter;
import util.PreparedQueries;


public class TopBalanceTransaction extends AbstractTransaction {
    static final int K = 10;
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

        class Customer implements Comparable<Customer> {
            public int w_id;
            public int d_id;
            public String first;
            public String middle;
            public String last;
            public double balance;

            public Customer(Row cInfo) {
                w_id = cInfo.getInt("C_W_ID");
                d_id = cInfo.getInt("C_D_ID");
                first = cInfo.getString("C_FIRST");
                middle = cInfo.getString("C_MIDDLE");
                last = cInfo.getString("C_LAST");
                balance = cInfo.getBigDecimal("C_BALANCE").doubleValue();
            }

            @Override
            public int compareTo(Customer other) {
                // Want MAX PQ
                return -Double.compare(this.balance, other.balance);
            }
        }

        PriorityQueue<Customer> customersPQ = new PriorityQueue<>();
        for(int w_id = 1; w_id < N_WAREHOUSE; ++w_id) {
            for(int d_id = 1; d_id < N_DISTRICT; ++d_id) {
                String getTopKBalanceCustomers = String.format(
                        "SELECT C_W_ID, C_D_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST " +
                                "FROM customer " +
                                "WHERE C_W_ID=%d and C_D_ID=%d "+
                                "ORDER BY C_BALANCE DESC " +
                                "LIMIT %d",
                        w_id, d_id, K
                );
                List<Row> results = this.executeQuery(getTopKBalanceCustomers);
                for(Row row : results) {
                    customersPQ.add(new Customer(row));
                }
            }
        }

        Customer[] topCustomers = new Customer[K];
        for(int i=0; i < K; ++i) {
            topCustomers[i] = customersPQ.poll();
        }

        // List<Row> customersInfo = this.executeQuery(PreparedQueries.getTopKBalanceCustomers, K);
        System.out.println("*** Top Balance Transaction Summary ***");
        for (Customer customer : topCustomers) {
            int warehouseId = customer.w_id;
            int districtId = customer.d_id;
            String warehouseName = this.executeQuery(PreparedQueries.getWarehouseName, warehouseId).get(0).getString("W_NAME");
            String districtName = this.executeQuery(PreparedQueries.getDistrictName, warehouseId, districtId).get(0).getString("D_NAME");
            // System.out.println(outputFormatter.formatTopBalanceCustomerInfo(cInfo, warehouseName, districtName));
            /*
            (a) Name of customer (C FIRST, C MIDDLE, C LAST)
            (b) Balance of customer’s outstanding payment C BALANCE
            (c) Warehouse name of customer W NAME
            (d) District name of customer D NAME
             */
            String cInfo = String.format("Customer: (%s, %s, %s), Balance: %.2f, Warehouse: %s, District: %s",
                        customer.first, customer.middle, customer.last,
                        customer.balance, warehouseName, districtName);
            System.out.println(cInfo);
        }
    }
}
