from collections import defaultdict
from flask import Flask, jsonify, request
import snowflake.connector
from flask_cors import CORS
import json
from config import conn_params
from data_adaptor import *
from utils import *


app = Flask(__name__)
CORS(app)


@app.get('/')
def start_server():
    print("Server running ** ")
    return {"response":"Welcome to Production scheduling module.. "}

@app.get('/test')
def test_server():
    print("Tests running")
    return {"response":"Hello World"}




@app.route('/schedule-data')
def get_schedule_data(test=False):
    # Connect to Snowflake
    
    ctx = snowflake.connector.connect(**conn_params)
    cursor = ctx.cursor()

    try:
        query_filters=[]
        query_params=[]
        # Execute a query
        query = """
                SELECT
                    DAYNAME(a.STARTS_AT) AS day_of_week,
                    a.EXPECTED_QUANTITY as exp_quantity,
                    e.WORK_ORDER_ID as work_order_id,
                    l.name as line_name,
                    b.name as plant_location,
                    wo.code as work_order,
                    wo.description as work_order_description,
                    wo.ITEM_DESCRIPTION as work_order_item_description,
                    wo.ITEM_CODE as work_order_item_code,
                    e.ndp_source_row_id as job_number,
                    wo.reference1 as work_order_reference,
                    wo.DESCRIPTION as notes
                FROM
                    "PRODUCTION_SCHEDULING_PROD_MSI_EXPRESS_SHARE"."LATEST"."WORK_BLOCKS" a
                LEFT JOIN "PRODUCTION_SCHEDULING_PROD_MSI_EXPRESS_SHARE"."LATEST".sites b ON
                    a.site_id = b.site_id
                LEFT JOIN "PRODUCTION_SCHEDULING_PROD_MSI_EXPRESS_SHARE"."LATEST".lines l 
                    ON a.line_id = l.line_id AND a.site_id = b.site_id
                LEFT JOIN "PRODUCTION_SCHEDULING_PROD_MSI_EXPRESS_SHARE"."LATEST".work_orders wo 
                    ON a.WORK_ORDER_ID = wo.work_order_id
                LEFT JOIN
                    SHOP_FLOOR_CONTROL_PROD_MSI_EXPRESS_SHARE.LATEST.JOBS e 
                    ON a.sfc_job_uuid = e.uuid

                WHERE 1=1 
            """
        
        # req params 

        location = request.args.get('location')
        

        start_date = request.args.get('startDate')

        end_date = request.args.get('endDate')

        work_order = request.args.get('workOrder')
        line_id = request.args.get('lineId')
        room_id = request.args.get('roomId')
        
        test = request.args.get('test')

        if(test):
            print("Test mode is ON, not executing query")
            test = True

        if not location:
            return jsonify({
                "message" : "Location required ... ",
                "quick_fix":"Please enter a location .. "
            }),400
        
        else:
            location_pattern = '%' + location + '%'
            query_filters.append("AND b.name LIKE %s ")
            query_params.append(location_pattern)

        if not start_date or  not end_date :
            start_date,end_date = get_default_dates()
        if  valid_dates(start_date, end_date):
            query_filters.append("AND a.STARTS_AT >= %s AND a.ENDS_AT < %s")
            query_params.append(start_date)
            query_params.append(end_date)
            
        
        else:
            return jsonify({
                "message" : "Date range too large/Invalid ... ",
                "quick_fix":"Please select 1 week date Ranges.. "
            }),400
            

        if work_order:
            work_order_pattern =  '%' + work_order 
            query_filters.append(" AND e.work_order_id LIKE %s ")
            query_params.append(work_order_pattern)

        if line_id:
            # line filter query 
            query_filters.append(" AND l.name = %s ")
            query_params.append(line_id)
        if room_id:
            # line filter query 
            room_pattern = '%' + room_id + '%'
            query_filters.append(" AND l.name LIKE  %s ")
            query_params.append(room_pattern)


        query_filters.append(" LIMIT 10 ")

        final_query = query + "\n" + "\n".join(query_filters)


        
        print(" -------- 1 ** EXECUTING QUERY -------------")
        # print(final_query)
        # for param in query_params:
        #     print(param)
        debug_query = final_query % tuple(f"'{p}'" for p in query_params)
        print(debug_query)
        print(" -------- QUERY END  -------------")



        if test: 
            return{ "DONE":"Test mode is ON, not executing query"}




        cursor.execute(final_query,query_params)
   
        rows = cursor.fetchall()

        # Initialize a dictionary for the days of the week
        default_days = {
            "Mon": "0",
            "Tue": "0",
            "Wed": "0",
            "Thu": "0",
            "Fri": "0",
            "Sat": "0",
            "Sun": "0"
        }

        # Process rows to aggregate data
        orders = defaultdict(lambda: {
            "days": default_days.copy(),
            "total": "0",
            "details": {}
        })
        work_order_id = None

        for row in rows:
           # work_order_id = None
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

            order = orders[work_order_id]
            order['days'][day_of_week] = f"{float(order['days'][day_of_week]) + exp_quantity:.5f}"
            order['total'] = f"{float(order['total']) + exp_quantity:.5f}"

            # Store additional details
            details = order['details']
            details['line_name'] = line_name
            details['plant_location'] = plant_location
            details['work_order_code'] = work_order_code
            details['work_order_description'] = work_order_description
            details['work_order_item_description'] = work_order_item_description
            details['work_order_item_code'] = work_order_item_code
            details['job_number'] = job_number
            details['work_order_id'] = work_order_id
            details['work_order_reference'] = work_order_reference
            details['notes'] = notes


            #TODO fetching bulk item details for every day need to get only once per work order 
            # TODO change the logic handler 
        
            if 'bulk_item_details' not in details:
                # TODO start_date and end_date should be passed from the request
                details['bulk_item_details'] = fetch_bulk_item_details(start_date,end_date,work_order_id,cursor)



        flattened_results = []
        for key, val in orders.items():
            merged_data = {}
            merged_data.update(val['days'])
            merged_data.update(val['details'])
            merged_data['total'] = val['total']
            
            flattened_results.append(merged_data)
        print(jsonify(flattened_results))

        return jsonify(flattened_results)
    finally:
        cursor.close()
        ctx.close()
if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True, port=5000)
