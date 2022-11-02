package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import util.PreparedQueries;


public class RelatedCustomerTransaction extends AbstractTransaction {

    final int DISTRICT_NUM = 10;
    final int WAREHOUSE_NUM = 10;
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
        StringBuilder builder = new StringBuilder();

        res = executeQuery(PreparedQueries.getOrderAndItemIds, warehouseId, districtId, customerId);
        print("********** Related Customer Transaction *********\n");
        print(String.format("Main customer: (%d, %d, %d)", warehouseId, districtId, customerId));

        HashMap<Integer, HashSet<Integer>> oIdToItemIds = new HashMap<>();

        HashSet<Integer> allOrderedItems = new HashSet<>();

        // get order id and item ids in one order
        for (Row row : res) {
            int currItemId = row.getInt("OL_I_ID");
            int currOId = row.getInt("OL_O_ID");
            if (!oIdToItemIds.containsKey(currOId)) {
                oIdToItemIds.put(currOId, new HashSet<>());
            }
            oIdToItemIds.get(currOId).add(currItemId);
            allOrderedItems.add(currItemId);
        }

        // each element is a set of items in one order of main customer
        ArrayList<HashSet<Integer>> itemIdSetList = new ArrayList<>(oIdToItemIds.values());
        int numOfMainOrders = itemIdSetList.size();

        HashSet<String> relatedCustomers = new HashSet<>();
        for (int currWId = 1; currWId <= WAREHOUSE_NUM; currWId++) {
            // ensure C and C′ are associated with different warehouses
            if (currWId == warehouseId) {
                continue;
            }

            for (int currDId = 1; currDId <= DISTRICT_NUM; currDId++) {

                // for each (warehouse, district)
                List<Row> orderLines = executeQuery(PreparedQueries.getOrderLinesInDistrict, currWId, currDId);
                HashMap<Integer, ArrayList<HashSet<Integer>>> orderToItemSetList = new HashMap<>();

                for (Row orderLine : orderLines) {
                    int currItemId = orderLine.getInt("OL_I_ID");

                    // skip if the item is not ordered by main customer
                    if (!allOrderedItems.contains(currItemId)) {
                        continue;
                    }
                    int currCId = orderLine.getInt("OL_C_ID");
                    int currOId = orderLine.getInt("OL_O_ID");

                    String fullCustomerId = String.format("%d,%d,%d", currWId, currDId, currCId);

                    // skip if the customer is already added to result
                    if (relatedCustomers.contains(fullCustomerId)) {
                        continue;
                    }

                    // initialize the order to set list if not exists
                    if (!orderToItemSetList.containsKey(currOId)) {
                        ArrayList<HashSet<Integer>> itemSetList = new ArrayList<>();
                        for (int i = 0; i < numOfMainOrders; i++) {
                            itemSetList.add(new HashSet<>());
                        }
                        orderToItemSetList.put(currOId, itemSetList);
                    }

                    ArrayList<HashSet<Integer>> currItemSetList = orderToItemSetList.get(currOId);

                    for (int idx = 0; idx < numOfMainOrders; idx++) {
                        if (itemIdSetList.get(idx).contains(currItemId)) {
                            currItemSetList.get(idx).add(currItemId);
                            if (currItemSetList.get(idx).size() >= 2) {
                                relatedCustomers.add(fullCustomerId);
                                builder.append(fullCustomerId + "\n");
                                break;
                            }
                        }
                    }

                }
            }
        }

        if (builder.length() == 0) {
            print("No related customers found");
        } else {
            print("Related Customer: (warehouse, distrinct, customer id)");
            print(builder.toString());
        }
    }

    @Override
    public String toString() {
        return String.format("Related Customer Transaction info: warehouseId: %d, districtId: %d, customerId: %d", warehouseId, districtId, customerId);
    }

}
