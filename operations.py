import csv
import os
from pathlib import Path
import getpass
import oracledb
import pandas

def extract_test_data(start_date, end_date):
    '''
    oracle database example

    '''

    user = getpass.getpass(prompt='User: ')
    pwd = getpass.getpass(prompt='password: ', stream=None)

    dir_path = os.path.dirname(os.path.realpath(__file__))
    parent_dir = Path(dir_path).parent.parent
    output_directory = os.path.join(parent_dir, 'output')
    os.makedirs(output_directory, exist_ok=True)

    connection = oracledb.connect(
        user=user,
        password=pwd,
        service_name='',
        host='')

    print("Successfully connected to Oracle Database")



    query = f"""
        select distinct
            b.test,
            bn.raw_name,
            TO_CHAR(bni.test_date_time, 'YYYY-MM-DD HH24:MI:SS'),
            bni.operating_test_num,
            bni.test,
            bni.test
        from test b
        join test_name bn on b.test_id = bn.test_id
        left join test_name_item bni on bn.test_name_id = bni.test_name_id
        where bni.test_date_time at time zone 'Pacific/Honolulu' between timestamp'{start_date}' and timestamp'{end_date}'
            and bni.test = 'Test'
            and Bni.item_status in ( 1,2,3,4 )
        order by TO_CHAR(bni.date_time, 'YYYY-MM-DD HH24:MI:SS') asc
    """

    cursor = connection.cursor()
    cursor.execute("ALTER SESSION SET CURRENT_SCHEMA = name")
    cursor.execute(query)
    page_size = 10
    rows = cursor.fetchmany(page_size)

    output_file_name = os.path.join(output_directory, 'test_info.csv')

    data_file = open(output_file_name, 'w', newline='')

    csv_writer = csv.writer(data_file)
    header = ['test', 'Last Name', 'First Name', 'test Date', 'test#', 'Orig', 'Dest']
    csv_writer.writerow(header)
    print("Extracting data...")
    while rows:
        for row in rows:
            test = row[0]
            last_name = row[1].split('/')[0]
            if '## NONAME ##' in last_name:
                continue
            first_name = row[1].split('/')[1]
            test_date = row[2]
            test_number = row[3]
            test = row[4]
            if 'GRP' in test:
                continue
            test = row[5]
            entry = [test, last_name, first_name, test_date, test_number, test, test]
            csv_writer.writerow(entry)
        rows = cursor.fetchmany(page_size)
    cursor.close()
    connection.close()
    print(f'Database connection closed')
    print(f'Extracting data completed, please check file {output_file_name}')
    csv_df = pandas.read_csv(output_file_name)
    output_file_xls = os.path.join(output_directory, 'test_info_all.xlsx')
    csv_df.to_excel(output_file_xls, sheet_name='All', index=False)

    return output_file_name
