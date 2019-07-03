from pymongo import MongoClient
client = MongoClient(host='13.232.224.2',port=27017,username='root',password='profsnapeisdon')
db = client['droom']
collection = db['coupon_history']
import datetime

def savelogs(coupon_id='',logs_for='',exclude_lids=''):
    time = datetime.datetime.now()
    my_dict = {"logs_for": logs_for, "user_id": "cron","created_at":time,"updated_at":time}
    logged_coupon = collection.find_one({"_id":coupon_id})
    if logged_coupon:
        my_dict['post_data'] = logged_coupon
        if exclude_lids:
            db.coupons.update_one({"_id": coupon_id}, {"$set": {"approval_lids": exclude_lids}})

        db.coupon_history.insert_one(my_dict)

    return
