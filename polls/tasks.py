from celery import shared_task
import bson
from polls.models import Category,CouponApprOnListing
from pymongo import MongoClient
import string
from django.db.models import manager
from django.db import connection
import datetime
cursor = connection.cursor()

def maxDFCLimitCheck(coupon_code, coupon_id, coupon_end_date, listing , discount, coupon_discount_type, funded_by, droom_portion):
    if(funded_by=='seller' and droom_portion==0):
        return []

    deleted_listing_ids = []
    category_id = listing['category_id']
    lid = listing['lid']
    listing_id = listing['_id']
    totalpayout = 0
    if(listing['total_payout_value']):
        totalpayout = listing['total_payout_value']
    sellingprice = 0
    if(listing['selling_price']):
        sellingprice = listing['selling_price']
    sfc = sellingprice - totalpayout
    if(coupon_discount_type=='percentage'):
        discount = int(discount*sellingprice/100)
    dfc = round(discount-sfc)
    dfc_percentage = round(dfc/sellingprice*100,2)
    category_data = {}
    to_remove_lids = []
    category = Category.objects.filter(id=category_id).values('max_dfc_percentage','max_dfc_value','name','id')
    if category:
        for i in category:
            category_data['id'] = i['id']
            category_data['max_dfc_percentage'] = i['max_dfc_percentage']
            category_data['max_dfc_value'] = i['max_dfc_value']
            category_data['category_name'] = i['name']

    max_dfc_percentage = category_data['max_dfc_percentage']
    max_dfc_value = category_data['max_dfc_value']
    category_name = category_data['category_name']
    diff = 0
    if max_dfc_percentage > 0 or max_dfc_value > 0:
        count = CouponApprOnListing.objects.filter(coupon_code=coupon_code).filter(listing_id=listing_id).filter(dfc__gte=0).filter(status='approved').count()
        if count==0:
            count1 = CouponApprOnListing.objects.filter(coupon_code=coupon_code).filter(listing_id=listing_id).count()
        else:
            return []

        timestamp = datetime.datetime.now()
        params = {}
        params['coupon_id'] = coupon_id
        params['coupon_code'] = coupon_code
        params['listing_id'] = listing_id
        params['lid'] = lid
        params['discount'] = discount
        params['dfc'] = dfc
        params['dfc_percentage'] = dfc_percentage
        params['category_id'] = category_id
        params['status'] = 'pending_approval'
        params['category_name'] = category_name
        params['max_dfc_value'] = max_dfc_value
        params['max_dfc_percentage'] = max_dfc_percentage
        params['end_date'] = coupon_end_date
        params['created_at'] = timestamp
        params['updated_at'] = timestamp
        params['selling_price'] = sellingprice
        params['total_payout'] = totalpayout

        if dfc_percentage>max_dfc_percentage and max_dfc_percentage>0:
            deleted_listing_ids = lid
            diff = dfc_percentage - max_dfc_percentage
            params['exceeded'] = 'percentage'
            params['difference'] = diff

            if count1==0:
                m = CouponApprOnListing(**params)
                m.save()
            else:
                CouponApprOnListing.objects.filter(coupon_code=coupon_code).filter(listing_id=listing_id).update(status='pending_approval',discount=discount,dfc=dfc,selling_price=sellingprice,total_payout=totalpayout)

        else:
            if dfc>max_dfc_value and max_dfc_value>0:
                deleted_listing_ids = lid
                diff = dfc - max_dfc_value
                params['exceeded'] = 'amount'
                params['difference'] = diff

                if count1==0:
                    m = CouponApprOnListing(**params)
                    m.save()
                else:
                    CouponApprOnListing.objects.filter(coupon_code=coupon_code).filter(listing_id=listing_id).update(
                        status='pending_approval', discount=discount, dfc=dfc, selling_price=sellingprice,
                        total_payout=totalpayout)

            else:
                if count1!=0:
                    CouponApprOnListing.objects.filter(coupon_code=coupon_code).filter(listing_id=listing_id).delete()
                    to_remove_lids = lid

                else:
                    return []

    result = {}
    result['code'] = 'success'
    result['deleted_listing_ids'] = deleted_listing_ids
    result['to_remove_lids'] = to_remove_lids

    return result


@shared_task
def coupon_controller(valid,coupon_code,coupon_discount_type,discount):
    client = MongoClient(host='13.232.224.2', port=27017, username='root', password='profsnapeisdon')
    db = client['droom']
    where = {}
    wrong_city = 0
    cities = []
    where['status'] = 'active'
    where['quantity_available'] = {'$gte': 1}
    i = 0
    Coupon = db.coupons.find_one({"code": coupon_code})
    excludeLids = Coupon['exclude_lids']
    strLids = []
    if not excludeLids:
        for i in excludeLids:
            strLids.append(str[i])
        where['lid'] = {'$nin': strLids}

    if valid['vehicle_type']:
        i = i + 1
        where['category_name'] = {'$eq': valid['vehicle_type']}

    if valid['make']:
        i = i + 1
        where['make'] = [valid['make']]

    if (valid['vehicle_type']):
        i = i + 1

    if (valid['model']):
        i = i + 1
        where['model'] = {'$eq': valid['model']}

    if (valid['year']):
        i = i + 1
        where['year'] = {'$eq': valid['year']}

    if (valid['trim']):
        i = i + 1
        where['trim'] = {'$eq': valid['trim']}

    if (valid['max_price']):
        i = i + 1
        where['selling_price'] = {'$lte': int(valid['max_price'])}

    if (valid['listing_id']):
        ids = []

        for id in valid['listing_id']:
            if (bson.objectid.ObjectId.is_valid(id)):
                ids.append(bson.objectid.ObjectId(id))

        if ids:
            where['_id'] = {'$in': ids}

    if(valid['seller_id']):
        i = i + 1
        where['user_id'] = {'$in':valid['seller_id']}

    where['deleted_at'] = {'$exists':'false'}
    deleted_listing_ids = []
    to_remove_lids = []
    if i>0 and not wrong_city:
        listings = db.cmp_listings.aggregate([
            {"$match": where},
            {"$project": {"_id": 1,'category_id':1,'total_payout_value':1,'selling_price':1}}
        ])
        for listing in listings:
            if(coupon_discount_type=="price_override"):
                discount_new = listing['selling_price'] - discount
                if(discount_new>=0):
                    discount = discount_new
            couponCheckList = maxDFCLimitCheck(coupon_code,coupon_id,coupon_end_date,listing,discount,coupon_discount_type,funded_by,droom_portion)
            if not couponCheckList:
                continue
            else:
                if couponCheckList['deleted_listing_ids'][0]:
                    deleted_listing_ids = couponCheckList['deleted_listing_ids']

                if couponCheckList['to_remove_lids'][0]:
                    to_remove_lids = couponCheckList['to_remove_lids'][0]

    else:
        b = 1

    result = {}
    result['deleted_listing_ids'] = deleted_listing_ids
    result['to_remove_lids'] = to_remove_lids
    return result

