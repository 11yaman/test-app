from flask import Flask, request, jsonify
from confluent_kafka import Producer
import time, uuid

app = Flask(__name__)

global producer

@app.route('/start', methods=['POST'])
def start():
    global producer

    data = request.json
    broker = data['broker']
    message_count = data['message_count']
    message_size = data['message_size']
    topic = data['topic']

    producer = Producer({
        'bootstrap.servers': broker
    })
    
    print(f"Warm-up producing started... {time.time()}")

    produce_messages(int(message_count * 0.1), message_size, topic)

    print(f"Warm-up producing finished {time.time()}")

    return jsonify({"message_count": message_count})

@app.route('/produce', methods=['POST'])
def start_producing():
    global producer

    data = request.json
    message_count = data['message_count']
    message_size = data['message_size']
    topic = data['topic']

    print(f"Producing started... {time.time()}")

    produce_messages(message_count, message_size, topic)

    print(f"Producing finished {time.time()}")
    
    return jsonify({"message_count": message_count})

def produce_messages(message_count, message_size, topic):
    if message_size <= len(str(uuid.uuid4())):
        pregenerated_keys = [str(i) for i in range(message_count)]
    else:
        pregenerated_keys = [str(uuid.uuid4()) for _ in range(message_count)]

    payload_size = max(message_size - len(pregenerated_keys[0].encode()), 0) 
    payload = ('ABCDEFGHIJKLMNOPQRSTUVWXYZ' * ((payload_size // len('ABCDEFGHIJKLMNOPQRSTUVWXYZ')) + 1)).encode()[:payload_size]

    for i in range(message_count):
        producer.produce(topic=topic, value=payload, key=pregenerated_keys[i])
    producer.flush()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)