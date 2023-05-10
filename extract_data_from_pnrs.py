import sys

sys.path.append("..")
from util.db import operations

# Extract data
start_date = '2023-04-18 16:00:00'
end_date = '2023-04-19 23:59:59'

data_file = operations.extract_pnr_data(start_date, end_date)
