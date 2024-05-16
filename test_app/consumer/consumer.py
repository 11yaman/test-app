from flask import Flask, request, jsonify
from confluent_kafka import Consumer, KafkaException, KafkaError
import time

app = Flask(__name__)

global consumer, message_count, topic

@app.route('/start', methods=['POST'])
def start():
    global consumer, message_count, message_size, topic

    data = request.json
    broker = data['broker']
    message_count = data['message_count']
    message_size = data['message_size']
    topic = data['topic']

    consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': 'consumer_group'
    })
    
    return jsonify({"status": "Consumer started"}), 200

@app.route('/warm_up', methods=['POST'])
def warm_up():
    warm_up_data = request.json
    warm_up_topic = warm_up_data['topic']
    warm_up_message_count = warm_up_data['message_count']

    consumer.subscribe([warm_up_topic])

    warm_up_messages_received = 0

    try:
        while warm_up_messages_received < warm_up_message_count:
            msg = consumer.poll(0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            else:
                warm_up_messages_received += 1
                if warm_up_messages_received >= message_count:
                    break

        print("Warm-up completed.")
    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({"error": "An error occurred"}), 500

    return jsonify({"status": "Warm-up completed"})

@app.route('/consume', methods=['GET'])
def consume():
    global consumer, message_count, message_size, topic
    timestamps = []
    receive_times = []
    
    consumer.subscribe([topic])

    try:
        while len(timestamps) < message_count:
            msg = consumer.poll(0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            else:
                if not timestamps:
                    message_value = msg.value()
                    if message_value is not None:
                        message_size = len(message_value)
                        print(f"Size of the first message received: {message_size} bytes")
                timestamps.append(msg.timestamp()[1] )
                receive_times.append(time.time())
                if len(timestamps) >= message_count:
                    print(f"Consuming finished {time.time()}")
                    break

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({"error": "An error occurred"}), 500

    finally:
        consumer.close()
        print("Consumer closed.")

    if timestamps:
        latencies = [(rt * 1000 - timestamp) for rt, timestamp in zip(receive_times, timestamps)]
        min_timestamp_in_s = min(timestamps) / 1000
        print(f"Max receive time: {max(receive_times)}, Min timestamp: {min_timestamp_in_s}")
        msg_per_s = len(timestamps) / (max(receive_times) - min_timestamp_in_s)
        print(msg_per_s)
        MB_per_s = (msg_per_s * message_size) / 1048576

        return jsonify({"message_count": len(timestamps), "throughput": MB_per_s, "latencies": latencies})
    else:
        print("No messages received")
        return jsonify({"error": "No messages received"}), 500
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)
