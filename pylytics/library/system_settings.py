# The version of MySQL below which we need to add triggers to support 
# timestamp fields with CURRENT_TIMESTAMP defaults.
MYSQL_MIN_VERSION = '5.6.5'

# For now the batch size is hardcoded (i.e. the number of rows inserted per
# insert statement). Eventually this will be dynamically sized based on the 
# max packet size.
BATCH_SIZE = 1000
