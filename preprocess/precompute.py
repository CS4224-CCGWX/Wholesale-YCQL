# This script will get the latest 
# require pandas packages
import pandas as pd 
import os
dataDir="/home/stuproj/cs4224i/Wholesale-YCQL/project_files/data_files/"

# source file
order_path = dataDir + "order.csv"
order_line_path = dataDir + "order-line.csv"
district_path = dataDir + "district.csv"

# target file
order_lines_with_cid_path = dataDir+ "order-line-with-cid.csv"
district_with_delivery_path = dataDir+ "district-with-delivery.csv"

def get_cid_in_order_line():
    
    if (os.path.exists(order_lines_with_cid_path)): 
        return

    O_C_ID_idx = 3
    W_ID_idx = 0
    D_ID_idx = 1
    O_ID_idx = 2

    orders = pd.read_csv(order_path, header = None)
    orders = orders.loc[:, [W_ID_idx, D_ID_idx, O_ID_idx, O_C_ID_idx]]
    # print(orders.loc[:10, :])
    order_lines = pd.read_csv(order_line_path, header = None)
    # print(order_lines.loc[:10, :])


    order_lines_with_cid = order_lines.merge(orders, on = [W_ID_idx, D_ID_idx, O_ID_idx])
    order_lines_with_cid.to_csv(order_lines_with_cid_path, header = False, index = False)

def compute_next_order_to_deliver():
    print("t")
    if (os.path.exists(district_with_delivery_path)): 
        return
    print("t")
    W_ID_idx = 0
    D_ID_idx = 1
    O_CARRIER_ID_idx = 4
    O_ID_idx = 2

    orders = pd.read_csv(order_path, header = None)
    orders = orders[orders[O_CARRIER_ID_idx].notnull() == False] # remove not null rows

    district_with_next_delivery_id = orders.groupby([W_ID_idx, D_ID_idx], as_index = False)[O_ID_idx].min()

    districts = pd.read_csv(district_path, header = None)
    district_with_delivery = districts.merge(district_with_next_delivery_id, on = [W_ID_idx, D_ID_idx])

    district_with_delivery.to_csv(district_with_delivery_path, header = False, index = False)

get_cid_in_order_line()
compute_next_order_to_deliver()
