# util tools 
import json

from datetime import datetime, timedelta

def parse_jobs(row):
    
    columns = ['job', 'actual_job_start_date', 'item_code','item_description','units_expected' ,'units_produced']
    data_dict = dict(zip(columns, row[:-1]))
    return data_dict

    # # check to send raw data 
    # json_data = json.dumps(data_dict, indent=2)

    # print(json_data)
    # return json_data

def get_units_details(rows):
    print(" calculating actual and expected units ")
    actual_quantity=0
    expected_quantity =0
    for row in rows:
        actual_quantity+=row[-2]
        expected_quantity+=row[-3]
    return actual_quantity,expected_quantity


def is_bulk_item(code,desc):
    '''
    filter out the bulk items from all the items of the work_orders_subcomponents
    '''

    bulk_item_codes = ["bulk", "supersack"] # 92
  
    is_bulk = False


    item_code = int(code.split('-')[1])
    


    # debugs 
    # print(" ---- ITEM CODE AND DESC ------")
    # print(row[0])
    # print(str(item_code)+" ......."+  desc )



    # filters 
    if(item_code==12):
        is_bulk= True


    if(item_code==92):
        is_bulk= True


    if(item_code == 2):
        for item_code in bulk_item_codes:
            if item_code in desc.lower():
                #print(" found 02 bulk blend")
                is_bulk = True
    
    return is_bulk

def filter_bulk_item(bulk_items):
    filtered_bulk_items =[]
    for bulk_item in bulk_items:
        if is_bulk_item(bulk_item["bulk_item_code"], bulk_item["bulk_item_desc"]):
            filtered_bulk_items.append(bulk_item)

    return filtered_bulk_items

def get_work_oder_no(work_order):
    # print(" parsing work order ")
    work_oder_no = work_order.split("_")[-1]
    return '%'+str(work_oder_no)

def valid_dates(start_date_str, end_date_str):
    if not start_date_str and not end_date_str:
        return False
    
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    # Calculate the difference in days
    date_diff = (end_date - start_date).days

    # Check if it's more than 7 days
    if start_date>end_date or date_diff > 8:
        return False
    else:
        return True

def get_default_dates():

    today = datetime.now()
    start_date = today - timedelta(days=7)
    end_date = today
    # Find last week's Monday and Sunday
    today_weekday = today.weekday()
    last_monday = today - timedelta(days=today_weekday + 7)
    last_sunday = last_monday + timedelta(days=6)
    start_date = last_monday
    end_date = last_sunday

    # current week's dates
    # Find this week's Monday and Sunday
    this_monday = today - timedelta(days=today_weekday)
    this_day = today  # Use current date as end date instead of Sunday
    this_week_start = this_monday.strftime("%Y-%m-%d")
    this_week_end = this_day.strftime("%Y-%m-%d")
    
    # Return current week's Monday and Sunday in "YYYY-MM-DD" format
    this_week_monday = this_monday.strftime("%Y-%m-%d")
    this_week_sunday = (this_monday + timedelta(days=6)).strftime("%Y-%m-%d")
    return this_week_monday, this_week_sunday
    # return this_week_start, this_week_end
   

    # return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

def get_room(room_id):
    #print(room_id)
    # process proper query format and return it 
    room_id = room_id.split(' ')[1]
    #print(room_id)

    room_no = int(room_id)
    if room_no <10:
        return '0'+str(room_no)
    else:
        return str(room_no)

def get_dates(req_params):
    """
    Get the start and end dates from the request parameters.
    If not provided, return default dates.
    """
    start_date = req_params.get('start_date')
    end_date = req_params.get('end_date')

    if not start_date or not end_date:
        start_date, end_date = get_default_dates()

    
    return start_date, end_date


def generate_bulk_item_dates(start_date, end_date, suffix):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    possible_codes = []
    current = start_date
    while current <= end_date:
        # Full year format
        full_date_str = current.strftime("%m/%d/%Y")
        full_code = f"{full_date_str}-{suffix}"
        
        # Short year format
        short_date_str = current.strftime("%m/%d/%y")
        short_code = f"{short_date_str}-{suffix}"
        
        # Add both
        possible_codes.append(full_code)
        possible_codes.append(short_code)

        current += timedelta(days=1)

    return possible_codes

