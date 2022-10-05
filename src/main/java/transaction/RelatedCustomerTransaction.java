package transaction;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import util.OutputFormatter;
import util.PreparedQueries;

import java.util.List;

public class RelatedCustomerTransaction extends AbstractTransaction {

    private final int warehouseId;
    private final int districtId;
    private final int customerId;

    private final OutputFormatter outputFormatter = new OutputFormatter();

    public RelatedCustomerTransaction(Session session, int wid, int did, int cid) {
        super(session);
        warehouseId = wid;
        districtId = did;
        customerId = cid;
    }

    /*
        This transaction finds all the customers who are related to a specific customer. Given a customer C,
        another customer C′ is defined to be related to C if all the following conditions hold:
        • C and C′ are associated with different warehouses; i.e., C.C W ID 6= C′.C W ID, and
        • C and C′ each has placed some order, O and O′, respectively, where both O and O′ contain at
        least two items in common.
     */
    public void execute() {
        List<Row> res;

        res = executeQuery(PreparedQueries.getRelatedCustomers, warehouseId, districtId, customerId);

        System.out.println(outputFormatter.formatRelatedCustomerOutput(res));

    }
}
