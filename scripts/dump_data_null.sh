schema="/home/stuproj/cs4224i/Wholesale-YCQL/src/main/resources/schema0.ycql"
DELIM=","
YCQLSH="/temp/yugabyte-2.14.1.0/bin/ycqlsh"
dataDir="/home/stuproj/cs4224i/Wholesale-YCQL/project_files/data_files"
bsz=500

echo "***** Start dump data *****"
echo "Defining schema"

echo "Load order table"
$YCQLSH -e "USE wholesale; COPY \"order\" (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) FROM '$dataDir/order_with_null.csv' WITH DELIMITER='$DELIM' AND MAXBATCHSIZE=$bsz;"
