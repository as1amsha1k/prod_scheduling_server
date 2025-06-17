# use this to get any data from DB 
from connection_manager import get_redshift_connection,get_snowflake_connection
from utils import *
from flask import jsonify







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


