from datetime import datetime
from decimal import Decimal
import json
import time
import boto3
from faker import Faker
import os
from dotenv import load_dotenv

# Number of events that will be generated
numEvents = 100

# load ambience variables
load_dotenv()

# Define custom codification class
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(CustomEncoder, self).default(obj)

# Create Faker library instance
fake = Faker(locale = 'en')

# Get ASW credentials from ambience variables
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

# Config credentials access to AWS acount
s3 = boto3.resource('s3', aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key)

# Define S3 bucket
bucket_name = os.environ.get('BUCKET_RAW')

# Define list of pages from app
pages = [
    'home',
    'products',
    'cart',
    'checkout',
    'profile'
]

# Define list of user actions
actions = [
    'view_page',
    'click_link',
    'add_to_cart',
    'remove_from_cart',
    'checkout',
    'purchase'
]

# Generating random events from user
for i in range(numEvents):
    # Define user data
    user_data = {
        'id': fake.random_int(min=1, max=100),
        'name': fake.name(),
        'sex': fake.random_element(elements=('Male', 'Female')),
        'address': fake.address(),
        'ip': fake.ipv4(),
        'state': fake.state(),
        'latitude': fake.latitude(),
        'longitude': fake.longitude()
    }

    # Define event data
    event_data = {
        'timestamp': int(time.time()),
        'page': fake.random_element(elements=pages),
        'action': fake.random_element(elements=actions),
        'product_id': fake.random_int(min=1, max=100),
        'quantity': fake.random_int(min=1, max=5),
        'estoque_id': fake.random_int(min=1, max=100),
        'price': Decimal(str(round(fake.pyfloat(left_digits=2, right_digits=2, positive=True), 2))),
        'estoque_id_number': fake.random_int(min=10, max=100),
        'price': Decimal(str(round(fake.pyfloat(left_digits=2, right_digits=2, positive=True), 2)))
    }

    # Combine user data and event data in one single obj
    data = {
        'user': user_data,
        'event': event_data
    }

    # Write data in a local JSON
    now = datetime.now()
    frt_date = now.strftime("%d_%m_%Y_%H_%M_%S")

    with open(f"event_customers_mobile{i}_{frt_date}.json", "w") as f:
        time.sleep(1)
        json.dump(data, f, cls=CustomEncoder)

    # Save json files on S3 bucket
    s3.Object(bucket_name, f"event_customers_mobile{i}_{frt_date}.json").put(Body=json.dumps(data, cls=CustomEncoder))