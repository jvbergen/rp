from urllib import unquote_plus
from boto3 import client,resource
from os import environ
from botocore.vendored import requests
from json import dumps, loads
from xmltodict import parse
from time import time
from datetime import datetime
from dateutil import tz
from urllib2 import urlopen

s3 = client('s3')

nrHeaders = {"Content-Type": "application/json", "X-Insert-Key": environ['NR_ACCOUNT_KEY'] }
nrURL =  "https://insights-collector.newrelic.com/v1/accounts/1641266/events"

esHeaders =  {"Content-Type": "application/json", "Authorization": environ['KONG_AUTH'],
    "Cache-Control": "no-cache", "Postman-Token": environ['KONG_POSTMAN'] }
esURL = environ['ES_URL']

from_zone = tz.gettz('UTC')
to_zone = tz.gettz('Europe/London')

class MyMustRetry(Exception):
    pass