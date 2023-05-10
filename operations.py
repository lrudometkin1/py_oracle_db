import csv
import os
from pathlib import Path
import getpass
import oracledb
import pandas

def extract_pnr_data(start_date, end_date):
    '''
    Runs a query to extract pnr information from AIX between specific dates
    :param start_date: string representing the starting date in the format YYYY-MM-DD. example: 2023-04-18
    :param end_date: string representing the end date in the format YYYY-MM-DD. example: 2023-04-19
    :return: The absolute path of the file containing the extracted data
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
        service_name='haprod',
        host='10.112.217.237')

    print("Successfully connected to Oracle Database")



    query = f"""
        select distinct
            b.rloc,
            bn.raw_name,
            TO_CHAR(bni.departure_date_time, 'YYYY-MM-DD HH24:MI:SS'),
            bni.operating_flight_num,
            bni.origin,
            bni.destination
        from booking b
        join booking_name bn on b.booking_id = bn.booking_id
        left join booking_name_item bni on bn.booking_name_id = bni.booking_name_id
        where bni.departure_date_time at time zone 'Pacific/Honolulu' between timestamp'{start_date}' and timestamp'{end_date}'
            and bni.operating_carrier = 'HA'
            and Bni.item_status in ( 1,2,3,4 )
        order by TO_CHAR(bni.departure_date_time, 'YYYY-MM-DD HH24:MI:SS') asc
    """

    cursor = connection.cursor()
    cursor.execute("ALTER SESSION SET CURRENT_SCHEMA = HA_CUR_OWNR")
    cursor.execute(query)
    page_size = 10
    rows = cursor.fetchmany(page_size)

    output_file_name = os.path.join(output_directory, 'pnr_info.csv')

    data_file = open(output_file_name, 'w', newline='')

    csv_writer = csv.writer(data_file)
    header = ['PNR', 'Last Name', 'First Name', 'Flight Date', 'Flight#', 'Orig', 'Dest']
    csv_writer.writerow(header)
    print("Extracting data...")
    while rows:
        for row in rows:
            pnr = row[0]
            last_name = row[1].split('/')[0]
            if '## NONAME ##' in last_name:
                continue
            first_name = row[1].split('/')[1]
            flight_date = row[2]
            flight_number = row[3]
            origin = row[4]
            if 'GRP' in origin:
                continue
            destination = row[5]
            entry = [pnr, last_name, first_name, flight_date, flight_number, origin, destination]
            csv_writer.writerow(entry)
        rows = cursor.fetchmany(page_size)
    cursor.close()
    connection.close()
    print(f'Database connection closed')
    print(f'Extracting data completed, please check file {output_file_name}')
    csv_df = pandas.read_csv(output_file_name)
    output_file_xls = os.path.join(output_directory, 'pnr_info_all.xlsx')
    csv_df.to_excel(output_file_xls, sheet_name='All', index=False)

    return output_file_name
