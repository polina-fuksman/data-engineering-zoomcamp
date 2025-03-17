import csv
import json
from kafka import KafkaProducer
from time import time

def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    csv_file = 'green_tripdata_2019-10.csv' 



    columns_to_keep = [
        'lpep_pickup_datetime',
        'lpep_dropoff_datetime',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'tip_amount'
    ]

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        t0 = time()

        for row in reader:

            filtered_row = {key: row[key] for key in columns_to_keep if key in row}

            producer.send('green-trips', value=filtered_row)


    producer.flush()
    producer.close()

    t1 = time()
    print(f'Took {t1 - t0} seconds to send data and flush')


if __name__ == "__main__":
    main()
