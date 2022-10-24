package transaction;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import jnr.ffi.Struct;
import util.OutputFormatter;
import util.PreparedQueries;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class RelatedCustomerTransaction extends AbstractTransaction {

    public static final String GET_ITEM_IDS = "SELECT OL_O_ID FROM order_line WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_C_ID = %d ALLOW FILTERING";
    public static final String GET_POSSIBLE_CUSTOMERS = "SELECT OL_W_ID, OL_D_ID, OL_C_ID, OL_O_ID FROM order_line WHERE OL_W_ID <> %d ALLOW FILTERING";
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
        StringBuilder resultString = new StringBuilder();
        res = executeQuery(String.format(GET_ITEM_IDS, warehouseId, districtId, customerId));

        HashSet<Integer> itemIds = new HashSet<>();
        HashMap<String, HashSet<Integer>> cIdToItemSet = new HashMap<>();

        for (Row row : res) {
            int currItemId = row.getInt("OL_O_ID");
            itemIds.add(currItemId);
        }
        res = executeQuery(String.format(GET_POSSIBLE_CUSTOMERS, warehouseId));

        for (Row row : res) {
            int currItemId = row.getInt("OL_O_ID");
            if (!itemIds.contains(currItemId)) {
                continue;
            }
            int currWId = row.getInt("OL_W_ID");
            int currDId = row.getInt("OL_D_ID");
            int currCId = row.getInt("OL_C_ID");

            String identifier = String.format("%d, %d, %d", currWId, currDId, currCId);
            if (!cIdToItemSet.containsKey(identifier)) {
                cIdToItemSet.put(identifier, new HashSet<>());
            }
            HashSet<Integer> itemSet = cIdToItemSet.get(identifier);
            if (itemSet.size() >= 2) {
                continue;
            }
            itemSet.add(currItemId);
            if (itemSet.size() == 2) {
                resultString.append(identifier + "\n");
            }
        }
//        res = executeQuery(PreparedQueries.getRelatedCustomers, warehouseId, districtId, customerId);
//        System.out.println(outputFormatter.formatRelatedCustomerOutput(res));
        System.out.println(resultString.toString());

    }
}
