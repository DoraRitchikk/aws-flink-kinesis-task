import random
import datetime
import json
import boto3
import time

aws_access_key_id = 'ASIAXZ3V74IND5SZINRG'
aws_secret_access_key = 'v6b2y7z/9kZ54VJaRV1Ejlis/uDyLFRe74whcYZ8'


def generate_random_json():
    event_id = hash(random.random)

    tickers = ['APL', 'GOOGL', 'TSL']
    ticket = random.choice(tickers)

    price = round(random.uniform(100, 500), 2)

    exchange = ['NASDAQ', 'LSE']
    stock = random.choice(exchange)

    event_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    event = {
        "event_id": event_id,
        "ticket": ticket,
        "price": price,
        "stock": stock,
        "event_time": event_time
    }

    return json.dumps(event)


def generate_package():
    json_str = [generate_random_json() for i in range(3)]
    return json_str


def choose_stream(record):
    data = json.loads(record)
    stock = data.get('stock')
    if stock == 'NASDAQ':
        return 'nasdaq'
    elif stock == 'LSE':
        return 'lse'


kinesis = boto3.client('kinesis', aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key)

while True:
    for line in generate_package():
        stream_key = choose_stream(line)

        if stream_key == 'nasdaq':
            response = kinesis.put_record(StreamName='dritchik-education-nasdaq', Data=line.encode('utf-8'),
                                          PartitionKey=stream_key)
        elif stream_key == 'lse':
            response = kinesis.put_record(StreamName='dritchik-education-lse', Data=line.encode('utf-8'),
                                          PartitionKey=stream_key)
    time.sleep(3)
