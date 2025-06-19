# use this to get any data from DB 
from connection_manager import get_redshift_connection,get_snowflake_connection
from utils import *
from flask import jsonify
from collections import defaultdict
from data_processor import *

def fetch_bulk_item_details(start_date,end_date,work_order_id,cur):
    if work_order_id is None:
        return {}
    # conn=get_snowflake_connection()
    # cur = conn.cursor()

    work_order_no = get_work_oder_no(work_order_id)
    params=(work_order_no,)
    print("-- fetching for work order "+ work_order_no)
    try:
        query='''
                select
                
                items.code, items.alternate_code_2, items.description,items.is_finished_good


                from SHOP_FLOOR_CONTROL_PROD_MSI_EXPRESS_SHARE.LATEST.WORK_ORDERS wo
                inner join SHOP_FLOOR_CONTROL_prod_msi_express_SHARE.LATEST.bills_of_materials_bom_versions bom
                on wo.item_id = bom.item_id
                    and wo.BOM_VERSION_ID = bom.BILLS_OF_MATERIALS_BOM_VERSION_ID
                inner join SHOP_FLOOR_CONTROL_prod_msi_express_SHARE.LATEST.bills_of_materials_bom_items bomi
                on bomi.bills_of_materials_bom_version_id = bom.bills_of_materials_bom_version_id and bom.version_status = 'active'
                    
                inner join  SHOP_FLOOR_CONTROL_PROD_MSI_EXPRESS_SHARE.LATEST.ITEMS items
                on items.item_id = bomi.subcomponent_item_id
                


                where wo.work_order_id like %s
               

        
            '''
        print(" -------- EXECUTING BULK IETM DETAILS QUERY   -------------")
        debug_query = query % tuple(f"'{p}'" for p in params)
        print(debug_query)
        print(" -------- QUERY END  -------------")
        cur.execute(query,params)

        rows=cur.fetchall()


        bulk_item_details={}
        if(len(rows)>0):
            for row in rows:
               # print("Applying filters ...")
                if is_bulk_item(row):
                    bulk_item_code = row[0]
                    columns = ['bulk_item_code', 'bulk_item_reference', 'bulk_item_description']
                    bulk_item_detail = dict(zip(columns, row[:-1]))
                    bulk_wo= fetch_bulk_item_work_order(start_date,end_date,bulk_item_code,cur)
                    if bulk_wo:
                        bulk_item_detail['bulk_item_wo_code'] =bulk_wo

                    # debug
                    # print(" bulk item detail  "+ str(bulk_item_detail))
                    bulk_item_details.update(bulk_item_detail)

        return bulk_item_details
    finally:
        # TODO change finally to proper catch 
        print(" done ")


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

    print("--------------EXECUTING FETCH BULK ITEM WORK ORDER QUERY -----------------")
    print(query % tuple(repr(code) for code in possible_codes))
    print("-------------- QUERY END -----------------")




    # Execute with your DB cursor
    cur.execute(query, possible_codes)
    rows = cur.fetchall()
    total_work_orders=len(rows)
    print(" found {0} bulk item work orders ".format(total_work_orders))
    if total_work_orders >0:
        item_work_order=rows[0][0]


    return item_work_order


def fetch_schedule_data(req_params):
    

    

    conn= get_snowflake_connection()
    cur = conn.cursor()
    # Execute a query
    final_query,query_params = prepare_sql_query(req_params)

    # we are using dates to find the bulk item details
    # start_date,end_date = get_dates(req_params)

    if not final_query:
        return jsonify({"error": "Invalid parameters provided"}), 400
    


    




    cur.execute(final_query,query_params)

    rows = cur.fetchall()

    return parse_db_data(rows)

    


def prepare_sql_query(req_params):

    print("Preparing SQL query with params: ", req_params)
    query_filters = []
    query_params =[]
    query = """
              SELECT
                DAYNAME(a.STARTS_AT) AS day_of_week,
                a.EXPECTED_QUANTITY as exp_quantity,
                wo_shop.WORK_ORDER_ID as work_order_id,
                l.name as line_name,
                b.name as plant_location,
                wo.code as work_order_code,
                wo.description as work_order_description,
                wo.ITEM_DESCRIPTION as work_order_item_description,
                 wo.ITEM_CODE as work_order_item_code,
                j.ndp_source_row_id as job_number,
                wo.reference1 as work_order_reference,
                wo.DESCRIPTION as notes,
                items.code, items.alternate_code_2, items.description,items.is_finished_good
            FROM
                "PRODUCTION_SCHEDULING_PROD_MSI_EXPRESS_SHARE"."LATEST"."WORK_BLOCKS" a
            LEFT JOIN "PRODUCTION_SCHEDULING_PROD_MSI_EXPRESS_SHARE"."LATEST".sites b ON
                a.site_id = b.site_id
            LEFT JOIN "PRODUCTION_SCHEDULING_PROD_MSI_EXPRESS_SHARE"."LATEST".lines l
                ON a.line_id = l.line_id AND a.site_id = b.site_id
            LEFT JOIN "PRODUCTION_SCHEDULING_PROD_MSI_EXPRESS_SHARE"."LATEST".work_orders wo
                ON a.WORK_ORDER_ID = wo.work_order_id

                
            LEFT JOIN
                SHOP_FLOOR_CONTROL_PROD_MSI_EXPRESS_SHARE.LATEST.JOBS j 
                ON a.sfc_job_uuid = j.uuid
                
            INNER JOIN SHOP_FLOOR_CONTROL_PROD_MSI_EXPRESS_SHARE.LATEST.WORK_ORDERS wo_shop
              on  wo_shop.work_order_id= j.work_order_id

            INNER join SHOP_FLOOR_CONTROL_prod_msi_express_SHARE.LATEST.bills_of_materials_bom_versions bom
                on wo_shop.item_id = bom.item_id and wo_shop.BOM_VERSION_ID = bom.BILLS_OF_MATERIALS_BOM_VERSION_ID
            INNER join SHOP_FLOOR_CONTROL_prod_msi_express_SHARE.LATEST.bills_of_materials_bom_items bomi
                on bomi.bills_of_materials_bom_version_id = bom.bills_of_materials_bom_version_id and bom.version_status = 'active'
                    
            INNER join  SHOP_FLOOR_CONTROL_PROD_MSI_EXPRESS_SHARE.LATEST.ITEMS items
                on items.item_id = bomi.subcomponent_item_id

            WHERE 1=1
        """
    
    # req params 

    location = req_params.get('location')
    

    start_date = req_params.get('startDate')

    end_date = req_params.get('endDate')

    work_order = req_params.get('workOrder')
    line_id = req_params.get('lineId')
    room_id = req_params.get('roomId')
    

    

    if not location:
        return '',''
    
    else:
        location_pattern = '%' + location + '%'
        query_filters.append("AND b.name LIKE %s ")
        query_params.append(location_pattern)

    if not start_date or  not end_date :
        start_date,end_date = get_default_dates()
    if  start_date and  end_date:
        query_filters.append("AND wo_shop.PLANNED_START >= %s AND wo_shop.PLANNED_END < %s")
        query_params.append(start_date)
        query_params.append(end_date)
        
    
    else:
        return '',''
        

    if work_order:
        #work_order_pattern =  '%' + work_order 
        query_filters.append(" AND wo_shop.ndp_source_row_id = %s ")
        query_params.append(work_order)

    if line_id:
        # line filter query 
        query_filters.append(" AND l.name = %s ")
        query_params.append(line_id)
    if room_id:
        # line filter query 
        room_pattern = '%' + room_id + '%'
        query_filters.append(" AND l.name LIKE  %s ")
        query_params.append(room_pattern)


    query_filters.append("order by work_order_id")

    final_query = query + "\n" + "\n".join(query_filters)


    
    print(" -------- 1 ** EXECUTING QUERY -------------")
    # print(final_query)
    # for param in query_params:
    #     print(param)
    debug_query = final_query % tuple(f"'{p}'" for p in query_params)
    print(debug_query)
    print(" -------- QUERY END  -------------")

    return final_query, query_params
