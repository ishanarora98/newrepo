from __future__ import absolute_import
import os
from celery import Celery
from django.conf import settings

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'droomfinal.settings')
#celery = Celery('tasks',backend='amqp', broker='amqp://localhost//')
app = Celery('droomfinal',
             broker='amqp://localhost',
             backend='amqp')
app.config_from_object('django.conf:settings',namespace='CELERY')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)