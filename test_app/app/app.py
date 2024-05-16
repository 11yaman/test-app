from flask import Flask, request, jsonify, send_file, make_response
from test_operations import start_test
from plotting import create_throughput_plot, create_latency_plot
import pandas as pd
import numpy as np

app = Flask(__name__)

@app.route('/test', methods=['POST'])
def test():
    message_size = int(request.args.get('message_size'))
    brokers = ['kafka-1:9093', 'redpanda-0:9092']
    message_counts = [10, 100, 1000, 10000]
    num_partitions = [1, 3, 10]
    num_repeats = 3
    
    final_results = []

    test_id = 0

    for num_partition in num_partitions:
        for message_count in message_counts:
            for broker in brokers:
                test_id += 1
                print(f"Test {test_id} started...")

                test_runs = [start_test(broker, message_count, message_size, num_partition) for _ in range(num_repeats)]
            
                throughputs = [run['throughput'] for run in test_runs]
                mean_throughput = np.mean(throughputs)
                std_dev_throughput = np.std(throughputs, ddof=0)

                latencies = [run['latencies'] for run in test_runs]
                latencies_matrix = np.array(latencies).T
                mean_latencies = np.mean(latencies_matrix, axis=1)
                std_dev_latencies = np.std(latencies_matrix, axis=1, ddof=0)
                
                final_results.append({
                    "test_id": test_id,
                    "broker": broker.split('-')[0].capitalize(),
                    "num_partitions": num_partition,
                    "message_count": message_count,
                    "message_size": message_size,
                    "throughput": mean_throughput,
                    "std_dev_throughput": std_dev_throughput,
                    "latencies": mean_latencies.tolist(),
                    "std_dev_latencies": std_dev_latencies.tolist()
                })

    df = pd.DataFrame(final_results)
    
    throughput_buffer = create_throughput_plot(df)
    with open("throughput_plot.png", "wb") as f:
        f.write(throughput_buffer.getbuffer())

    latency_buffer = create_latency_plot(df)
    with open("latency_plot.png", "wb") as f:
        f.write(latency_buffer.getbuffer())

    return jsonify(final_results)

@app.route('/throughput_plot', methods=['GET'])
def get_throughput_plot():
    try:
        return send_file("throughput_plot.png", mimetype='image/png')
    except FileNotFoundError:
        return make_response("File not found.", 404)

@app.route('/latency_plot', methods=['GET'])
def get_latency_plot():
    try:
        return send_file("latency_plot.png", mimetype='image/png')
    except FileNotFoundError:
        return make_response("File not found.", 404)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)