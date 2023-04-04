import random
import datetime
import json
import boto3
import time


def generate_random_json():
    event_id = hash(random.random())

    tickers = ['APL', 'GOOGL', 'TSL']
    ticker = random.choice(tickers)

    price = round(random.uniform(100, 500), 2)

    exchange = ['NASDAQ', 'LSE']
    stock = random.choice(exchange)

    event_time = datetime.datetime.now().isoformat()

    event = {
        "event_id": event_id,
        "ticker": ticker,
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

kinesis = boto3.client('kinesis',
aws_access_key_id='ASIAXZ3V74INPDOXFICT',
aws_secret_access_key='xwN/V4N942/z/AOsiDmyO7utKi+mi4C/DwNbtip8',
aws_session_token='IQoJb3JpZ2luX2VjEA8aDGV1LWNlbnRyYWwtMSJGMEQCIEf598qkOKpI3W86rhym7SZNVK8WrkmafnnccQonQkbZAiA9AWLjQLqpAYDchIyD/FbS/HZMP/S9WkrKisZJYeJWWSqYAwjo//////////8BEAQaDDUzNjU4MTQ5NzM3MCIMbOMk8JfBq0OLpji8KuwC3BIMbYs4P+kHOOXpR0g4njYZNQwbbUjKvnHEgYk/tYsAcCHmlxGLU39/CKdJSdugC8QTCdGrrkN+LaM8k9kpg2uQZTpj0TT/DnvK12zDOnU3uPRw5MQFxYxtzw/PN28EFAuwwlwAvv1AyEWeTBXuVkm2j6tOWHN+89IFVrnU5GTlClPsc3zvBWqOwuwuB83P4mLGEJxa6fDmztDyjiAMHIE/rfkS73yrtEXLbGzpWYmahs8FWzRJC4jsLMqXSs3qsBfxJBYfZ0wVbxwiI34Yn5gt+6sIVaCqDhviraOQVw1zWQZuV892Ss1wG8l0KYy8t76WhGCXLT/ZiCd/EzBwEM9P9D6ftOC9xND2HO/5NePzUmdlKYQu+Fv7wve9DDEiXoPy0EsTCPCGaJPTaA5l3TaRoiBkQIWEqO3nBrSEvIVgNsG/icWWNb0cv3TLMcoPRaaC1R75T6ozOou7a+PgI11hbXNw+QqhzxgNMzCmn6+hBjqnAZdzSDfhOVhwPCJy4IOjyJ/HfHfHwFvF2STzAl4qtn2ve2yhAPWaMYxy9IctD4bDGiYRJjQuLYdqWaj7IvCe9J41mVjcgFRnp7wD60lc+33xg/Pu0m1TTjbNAAxqLAv9bfTuk5Mt4R5I7VaSsjl/fCNNfSc+2G9gop5iH0Qs4Rb7sK3V9SanwPdxGNrRUgndZiY7bCNycf5nAtey8uNrDDV2/QlTMf8x')


while True:
    for line in generate_package():
        stream_key = choose_stream(line)

        if stream_key == 'nasdaq':
            response = kinesis.put_record(StreamName='dritchik-education-nasdaq', Data=line,
                                          PartitionKey=stream_key)
            print("nasdaq")
            print(line)
        elif stream_key == 'lse':
            response = kinesis.put_record(StreamName='dritchik-education-lse', Data=line,
                                          PartitionKey=stream_key)
            print("lse")
    time.sleep(3)
