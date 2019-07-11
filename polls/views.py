from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.template import loader
from django.shortcuts import get_object_or_404
from django.shortcuts import render
from django.http import Http404
from django.urls import reverse
from pymongo import MongoClient
from .tasks import coupon_controller
import json
import bson
from bson import ObjectId
from polls.models import CouponApprOnListing
from django.core.serializers.json import DjangoJSONEncoder
import logging
from .savelogs import savelogs
from bson import ObjectId
import threading
logger = logging.getLogger(__name__)

def unique(l):
    list_set = set(l)
    unique_list = list(list_set)
    return unique_list

client = MongoClient(host='13.232.224.2', port=27017, username='root', password='profsnapeisdon')
db = client['droom']
collection = db['coupons']

def threads(request):
    cashback_coupons = collection.find({"code":"DMP1"})
    jobs = []
    for i in cashback_coupons:
        data = {
            "coupon_code": i['code'],
            "coupon_id": i['_id'],
            "coupon_discount": i['discount'],
            "coupon_discount_type": i['discount_type'],
            "coupon_end_date": i['end_date'],
            "coupon_exclude_ids": []
        }
        thread = threading.Thread(target=validateCouponsApproval,args=(data,))
        jobs.append(thread)

    for j in jobs:
        j.start()

    for j in jobs:
        j.join()

    return HttpResponse("List Processing Complete")

def printcode(data):
    print(data['coupon_code']+" ")

def validateCouponsApproval(data):
    try:
        response = HttpResponse("Hello!")
        null = None
        client = MongoClient(host='13.232.224.2',port=27017,username='root',password='profsnapeisdon')
        db = client['droom']
        collection = db['coupons']
        coupon_code = data['coupon_code']
        if coupon_code:
            deleted_listing_ids = []
            to_remove_lids = []
            discount = data['coupon_discount']
            logger.info(discount)
            discount_percentage = 0
            updateCoupon = collection.find_one({"code":coupon_code})
            collection.update_one({"code": coupon_code}, {"$set": {"pending_approval": 2}})
            coupon_end_date = data['coupon_end_date']
            coupon_discount_type = data['coupon_discount_type']
            myquery = {"funded_by": null, "code": coupon_code}
            newvalues = {"$set": {"funded_by": ""}}
            collection.update_one(myquery, newvalues)
            myquery = {"droom_portion": null, "code": coupon_code}
            newvalues = {"$set": {"droom_portion": 0}}
            collection.update_one(myquery, newvalues)
            if(coupon_discount_type=="percentage"):
                discount_percentage = float(Coupon['coupon_discount'])
            coupon_valid_on = updateCoupon['valid_on']
            if(type(coupon_valid_on)==list):
                b = 0
                for valid in coupon_valid_on:
                    count = coupon_controller.delay(valid,coupon_code,coupon_discount_type,discount)
            else:
                b = 1

        changedCoupon = collection.find_one({"code":coupon_code})
        if b==0:
            if changedCoupon.__contains__('exclude_lids')=='false':
                changedCoupon['exclude_lids'] = []

            list_ans = changedCoupon['exclude_lids'] + deleted_listing_ids
            unique_list = unique(list_ans)
            myquery = {"code": coupon_code}
            newvalues = {"$set":{"exclude_lids":unique_list}}
            collection.update_one(myquery, newvalues)
            newvalues = {"$set":{"status": "active"}}
            collection.update_one(myquery, newvalues)

        elif b==1:
            CouponApprOnListing.objects.filter(coupon_code=coupon_code).delete()

        collection.update_one({"code": coupon_code}, {"$set": {"pending_approval": 0}})
        if changedCoupon.__contains__('wait_pending_approval'):
            collection.update_one({"code": coupon_code}, {"$set": {"wait_pending_approval": 0}})

        return response

    except:
        savelogs(data['coupon_id'],'approval_exception')

    finally:
        coupon_id = data['coupon_id']
        if coupon_id:
            db.coupons.update_one({"_id": coupon_id}, {"$set": {"pending_approval": 0}})



