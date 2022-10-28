package transaction;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.CqlSession;

import com.datastax.oss.protocol.internal.request.Prepare;
import util.OutputFormatter;
import util.PreparedQueries;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


public class RelatedCustomerTransaction extends AbstractTransaction {

//    public static final String GET_ITEM_IDS = "SELECT OL_I_ID FROM order_line WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_C_ID = %d ALLOW FILTERING";
//    public static final String GET_POSSIBLE_CUSTOMERS = "SELECT OL_W_ID, OL_D_ID, OL_O_ID, OL_C_ID, OL_I_ID FROM order_line WHERE OL_W_ID <> %d ALLOW FILTERING";
    private final int warehouseId;
    private final int districtId;
    private final int customerId;

    public RelatedCustomerTransaction(CqlSession session, int wid, int did, int cid) {
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

//        res = executeQuery(String.format(GET_ITEM_IDS, warehouseId, districtId, customerId));
        res = executeQuery(PreparedQueries.getItemIds , warehouseId, districtId, customerId);

        print(String.format("Main customer: (%d, %d, %d)", warehouseId, districtId, customerId));

        HashSet<Integer> itemIds = new HashSet<>();
        HashMap<String, HashSet<Integer>> cusAndItemToItemSet = new HashMap<>();
        
        for (Row row : res) {
            int currItemId = row.getInt("OL_I_ID");
            itemIds.add(currItemId);
        }
        print("Total distinct items ordered by customer: " + itemIds.size());

//        res = executeQuery(String.format(GET_POSSIBLE_CUSTOMERS, warehouseId));
        res = executeQuery(PreparedQueries.getPossibleCustomers, warehouseId);

        HashSet<String> relatedCustomers = new HashSet<>();

        for (Row row : res) {
            int currItemId = row.getInt("OL_I_ID");

            // skip if the item is not in the main customers' item set
            if (!itemIds.contains(currItemId)) {
                continue;
            }
            int currWId = row.getInt("OL_W_ID");
            int currDId = row.getInt("OL_D_ID");
            int currCId = row.getInt("OL_C_ID");
            int currOId = row.getInt("OL_O_ID");
            
            String fullCustomerId = String.format("%d,%d,%d", currWId, currDId, currCId);
            String customerAndOrder = String.format("%d,%d,%d,%d", currWId, currDId, currCId, currOId);

            // skip if the customer is already added to result
            if (relatedCustomers.contains(fullCustomerId)) {
                continue;
            }

            // print(String.format("Current customer and itemid: %s", customerAndOrder));
            if (!cusAndItemToItemSet.containsKey(customerAndOrder)) {
                cusAndItemToItemSet.put(customerAndOrder, new HashSet<>());
            }
            HashSet<Integer> itemSet = cusAndItemToItemSet.get(customerAndOrder);
            // print(String.format("Item Set size before adding " + itemSet.size()));

            itemSet.add(currItemId);
            if (itemSet.size() >= 2) {
                relatedCustomers.add(fullCustomerId);
                resultString.append(fullCustomerId + "\n");
            }
            // print(String.format("Item Set size after adding " + itemSet.size()));
        }
        

        if (resultString.length() == 0) {
            print("No related customers found");
        } else {
            print(resultString.toString());
        }
    }
}
