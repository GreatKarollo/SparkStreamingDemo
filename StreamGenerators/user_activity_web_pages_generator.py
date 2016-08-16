__author__ = 'Karol_Sudol'

import string
import random
import time
from datetime import datetime
from boto import kinesis


def random_generator(size=6, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))


region = 'us-east-1'
kinesisStreamName = 'stream1'

kinesis = kinesis.connect_to_region(region)

# generating data and feeding kinesis.

while True:

    urls = ['web1.com','web2.com','web3.com','web4.com','web5.com',web6.com','web7.com']
    x = random.randint(0,7)
    device_id = random.randint(55,60)+150000 
    
    now = datetime.now()
    timestamp = str(now.month) + "/" + str(now.day) + "/" + str(now.year) + " " + str(now.hour) + ":" +str(now.minute) + ":" + str(now.second)

    putString = str(device_id)+','+'www.'+urls[x]+','+timestamp
    patitionKey = random.choice('abcdefghij')

    # schema of the imput string now device_id,domain,timestamp

    print putString

    result = kinesis.put_record(kinesisStreamName,putString,patitionKey)

    print result







