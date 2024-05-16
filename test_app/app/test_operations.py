from confluent_kafka import admin
import requests
import threading
import time

def start_producer(test_params):
    return requests.post('http://producer:5001/start', json=test_params)

def start_consumer(test_params):
    return requests.post('http://consumer:5002/start', json=test_params)

def warm_up_producer(warm_up_params):
    return requests.post('http://producer:5001/warm_up', json=warm_up_params)

def warm_up_consumer(warm_up_params):
    return requests.post('http://consumer:5002/warm_up', json=warm_up_params)

def produce(test_params):
    return requests.post('http://producer:5001/produce', json=test_params)

def consume(consumer_results = []):
    response = requests.get('http://consumer:5002/consume')
    if response.status_code == 200:
        consumer_results.append(response.json())

def create_topic(broker, topic_name, num_partitions):
    admin_client = admin.AdminClient({'bootstrap.servers': broker})

    try:
        new_topic = admin.NewTopic(topic = topic_name, num_partitions=num_partitions, replication_factor=1,)
        res = admin_client.create_topics(new_topics=[new_topic], validate_only=False, operation_timeout=10,)
        for topic, f in res.items():
            f.result()
            print(f"Topic {topic} created")
    except Exception as e:
        print(f"An error occurred while creating the topic: {e}")

    time.sleep(5)

    return topic_name

def delete_topic(broker, topic_name):
    admin_client = admin.AdminClient({'bootstrap.servers': broker})

    try:
        res = admin_client.delete_topics([topic_name])
        for topic, f in res.items():
            f.result() 
            print(f"Topic {topic} deleted")
    except Exception as e:
        print(f"An error occurred while deleting the topic: {e}")

    time.sleep(5)

def start_test(broker, message_count, message_size, num_partitions):  
    topic_name = "test"
    delete_topic(broker, topic_name)
    create_topic(broker=broker, topic_name=topic_name, num_partitions=num_partitions)
    
    test_params = {
        "broker": broker,
        "message_count": message_count,
        "message_size": message_size,
        "topic": topic_name
    }

    consumer_results = []

    start_consumer(test_params)
    start_producer(test_params)

    # warm_up(broker, message_count, message_size, topic_name, num_partitions)

    consumer_thread = threading.Thread(target=consume, args=(consumer_results,))
    producer_thread = threading.Thread(target=produce, args=(test_params,))

    consumer_thread.start()
    time.sleep(5)
    producer_thread.start()
    producer_thread.join()
    consumer_thread.join()
    
    print(f"Test of {message_count} messages, each {message_size} bytes, results: {consumer_results[0]['throughput']}")
    
    return {
        "message_count": consumer_results[0]['message_count'],
        "throughput": consumer_results[0]['throughput'],
        "latencies": consumer_results[0]['latencies']
    }

def warm_up(broker, message_count, message_size, topic_name, num_partitions):

    warm_up_params = {
        "broker": broker,
        "message_count": int(message_count * 0.1),
        "message_size": message_size,
        "topic": topic_name
    }

    consumer_thread = threading.Thread(target=warm_up_consumer, args=(warm_up_params,))
    producer_thread = threading.Thread(target=warm_up_producer, args=(warm_up_params,))

    consumer_thread.start()
    time.sleep(5)
    producer_thread.start()
    producer_thread.join()
    consumer_thread.join()
        
    print("Warm-up phase completed.")
