__author__ = 'Karol_Sudol'

import string
import random
import time
from datetime import datetime
from boto import kinesis


def random_generator(size=6, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))


region = 'us-east-1'
kinesisStreamName = 'spark-demo'

kinesis = kinesis.connect_to_region(region)


while True:



    action = ['action1','action2','action3','action3','action4']
    x = random.randint(0,4)
    user_id = random.randint(25,35)+1200
    device_id = random.randint(55,60)+150000

    now = datetime.now()
    timestamp = str(now.month) + "/" + str(now.day) + "/" + str(now.year) + " " + str(now.hour) + ":" +str(now.minute) + ":" + str(now.second)


    #building the pay load for kinesis puts.

    putString = str(user_id)+',' + str(device_id)+','+action[x]+','+timestamp
    patitionKey = random.choice('abcdefghij')

    # schema of the imput string now user_id,device_id,action,timestamp

    print putString

    result = kinesis.put_record(kinesisStreamName,putString,patitionKey)

    print result







