# this is process data from data base and format it to be used by the client

from flask import request
import json
from utils import *
from connection_manager import *


def fetch_bulk_item_work_order(start_date,end_date,bulk_item_code,cur):
    # filter on wo.code and date 
    item_work_order=None

   

    possible_codes = generate_bulk_item_dates(start_date,end_date,bulk_item_code)
    placeholders = ', '.join(['%s'] * len(possible_codes))

    query = f"""
    SELECT ndp_source_row_id
    FROM SHOP_FLOOR_CONTROL_PROD_MSI_EXPRESS_SHARE.LATEST.WORK_ORDERS wo
    WHERE wo.code IN ({placeholders})
    """

    # print("--------------EXECUTING FETCH BULK ITEM WORK ORDER QUERY -----------------")
    # print("-------------- QUERY END -----------------")

    params = tuple(possible_codes)

    # print("Query to execute:", query)
    # print("Possible codes:", params)

    
    try:
        cur.execute(query, params)
        rows = cur.fetchall()
    except Exception as e:
        print(f"Error executing query: {e}")
        rows = []
    if len(rows)>0:
     
     item_work_order = rows[0][0]





    return item_work_order



def format_data(data):
    """
    Format the data to be used by the client.
    This function is a placeholder for actual data formatting logic.
    """
    # Example formatting logic
    data = sorted(data, key=lambda x: x.get("line_name", ""))
    
    return data

def populate_bulk_wo(start_date, end_date, data,cur):

# process this data and populate bulk item WO
  

    for prod_item in data:
        if "bulk_items" not in prod_item or not prod_item["bulk_items"]:
            continue
        # print(prod_item["bulk_items"])

        bulk_item_code =prod_item["bulk_items"][0]["bulk_item_code"]
        prod_item["bulk_items"][0]["bulk_item_work_order"] = fetch_bulk_item_work_order(start_date,end_date,bulk_item_code,cur)



    return data



def parse_db_data(rows):
    result = []

    print(len(rows))
    if len(rows)==0:
        return result    # work_order_id 
    result_item_base={
         "Mon": 0,
        "Tue": 0,
        "Wed": 0,
        "Thu": 0,
        "Fri": 0,
        "Sat": 0,
        "Sun": 0,
        "work_order_id":'',
        "line_name":'',
        "plant_location":'',
        "work_order_code":'',
        "work_order_desc":'',
        "work_order_item_desc":'',
        "work_order_item_code":'',
        "job_number":'',
        "work_order_ref":'',
        "notes":'',
        "bulk_items":[]


    }
    work_orders_set =set()
    bulk_item_set = set()
    is_first = True

    days=[ "Mon",
        "Tue",
        "Wed",
        "Thu",
        "Fri",
        "Sat",
        "Sun"]


    bulk_item_set=set()
    for row in rows:
        day_of_week = row[0]
        exp_quantity = float(row[1])  # Assuming the quantity is in a format that can be converted to float
        work_order_id = row[2]
        line_name = row[3]
        plant_location = row[4]
        work_order_code = row[5]
        work_order_description = row[6]
        work_order_item_description = row[7]
        work_order_item_code = row[8]
        job_number = row[9]
        work_order_reference=row[10]
        notes = row[11]

        # bulk item details start
        bulk_item_code = row[12]
        bulk_item_reference = row[13]
        bulk_item_desc = row[14]

        




        # a new row of work order will process this 
        if work_order_id not in work_orders_set:
            #print(work_order_id+" wo")
            if not is_first:
                # process work_orders and save them 
                total =0
                distinct_bulk_items = len(bulk_item_set)
                for day in days:
                    actual_day=result_item[day]/distinct_bulk_items
                    actual_day =float(format(actual_day, ".2f"))
                    
                    result_item[day] = actual_day
                    total+=actual_day
                result_item["total"]=total

                result_item["bulk_items"]=filter_bulk_item(result_item["bulk_items"])
                #print(json.dumps(result_item, indent=2))
                result.append(result_item)

            work_orders_set.add(work_order_id)
            result_item=result_item_base.copy()
            result_item["bulk_items"]=[]
            bulk_item_set = set()
           
            is_first = False

        result_item[day_of_week]+=exp_quantity
        result_item['line_name'] = line_name
        result_item['plant_location'] = plant_location
        result_item['work_order_code'] = work_order_code
        result_item['work_order_description'] = work_order_description
        result_item['work_order_item_description'] = work_order_item_description
        result_item['work_order_item_code'] = work_order_item_code
        result_item['job_number'] = job_number
        result_item['work_order_id'] = work_order_id
        result_item['work_order_reference'] = work_order_reference
        result_item['notes'] = notes

        if bulk_item_code not in bulk_item_set:
            bulk_item_set.add(bulk_item_code)
            result_item["bulk_items"].append({
                "bulk_item_code":bulk_item_code,
                "bulk_item_reference":bulk_item_reference,
                "bulk_item_desc":bulk_item_desc })
    

    # add last wo details into result 
    total =0
    distinct_bulk_items = len(bulk_item_set)
    for day in days:
        actual_day = result_item[day] / distinct_bulk_items
        actual_day =float(format(actual_day, ".2f"))
        total+=actual_day
                    
        result_item[day] = actual_day
    result_item["total"]=total
    #result_item["bulk_items"]=result_item["bulk_items"]
    result_item["bulk_items"]=filter_bulk_item(result_item["bulk_items"])
    #print(json.dumps(result_item, indent=2))
    result.append(result_item)





        
    #print(json.dumps(result, indent=2))
    return result