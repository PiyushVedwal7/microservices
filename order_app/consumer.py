from kafka import KafkaConsumer
import json
import pymongo

# Kafka Consumer setup
consumer = KafkaConsumer(
    'order_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='order_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# MongoDB setup
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['order_db']
orders_collection = db['orders']

print("Listening for new orders...")

# Process messages
for message in consumer:
    order_data = message.value
    order_id = order_data['order_id']
    product = order_data['product']
    quantity = order_data['quantity']

    # Insert order into MongoDB
    orders_collection.insert_one(order_data)
    print(f"Order saved: {order_id} - {product} (Quantity: {quantity})")



    """
    bin/zookeeper-server-start.sh config/zookeeper.properties

    bin/kafka-server-start.sh config/server.properties

    bin/kafka-topics.sh --create --topic order_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

    """
