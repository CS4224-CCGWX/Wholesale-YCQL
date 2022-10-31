package util;

import com.datastax.oss.driver.api.core.cql.Row;

public class Customer implements Comparable<Customer> {
    // Wraps customer related data, and implements reverse compareTo()
    public final int w_id;
    public final int d_id;
    public final String first;
    public final String middle;
    public final String last;
    public final double balance;

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
