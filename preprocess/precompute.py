# This script will get the latest 
# require pandas packages
import pandas as pd 
import os
dataDir="/home/stuproj/cs4224i/Wholesale-YCQL/project_files/data_files/"
order_lines_with_cid_path = dataDir+ "order-line-with-cid.csv"

def __main__():
    get_cid_in_order_line()



def get_cid_in_order_line():
    if (os.path.exists(order_lines_with_cid_path)): 
        return

    O_C_ID_idx = 3
    W_ID_idx = 0
    D_ID_idx = 1
    O_ID_idx = 2

    orders = pd.read_csv(dataDir + "order.csv", header = None)
    orders = orders.loc[:, [W_ID_idx, D_ID_idx, O_ID_idx, O_C_ID_idx]]
    print(orders.loc[:10, :])
    order_lines = pd.read_csv(dataDir + "order-line.csv", header = None)
    print(order_lines.loc[:10, :])

    # print(orders.loc[0,:])


    order_lines_with_cid = order_lines.merge(orders, on = [W_ID_idx, D_ID_idx, O_ID_idx])
    order_lines_with_cid.to_csv(order_lines_with_cid_path, header = False, index = False)