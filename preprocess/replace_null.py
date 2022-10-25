# the script replace the null value with empty string
dataDir="/home/stuproj/cs4224i/Wholesale-YCQL/project_files/data_files/"

file_names = [dataDir + "order.csv", dataDir + "order-line.csv"]

for file_name in file_names:
    #read input file
    fin = open(file_name, "rt")
    #read file contents to string
    data = fin.read()
    #replace all occurrences of the required string
    data = data.replace('null', '')

    #close the input file
    fin.close()
    #open the input file in write mode
    fin = open(file_name, "wt")
    #overrite the input file with the resulting data
    fin.write(data)
    #close the file
    fin.close()