fd = open('algorithms/udfs.sql', 'r')
sqlFile = fd.read()
fd.close()

udf_list = sqlFile.split('\\')


