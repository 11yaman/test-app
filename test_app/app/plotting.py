import matplotlib.pyplot as plt
import numpy as np
import io

def create_throughput_plot(df):
    message_size = df['message_size'].iloc[0]
    fig, ax = plt.subplots(figsize=(12, 8))
    fig.suptitle(f'Throughput at {message_size} bytes/message')

    pivot_df = df.pivot_table(index='message_count', columns=['broker', 'num_partitions'], values='throughput', aggfunc='sum')
    pivot_df.columns = [' '.join(map(str, col)).strip() for col in pivot_df.columns.values]
    std_df = df.pivot_table(index='message_count', columns=['broker', 'num_partitions'], values='std_dev_throughput', aggfunc='sum')
    std_df.columns = [' '.join(map(str, col)).strip() for col in std_df.columns.values]


    colors = {
        'Kafka': ['#add8e6', '#87cefa', '#4682b4'],
        'Redpanda': ['#ffc0cb', '#f08080', '#cd5c5c']
    }

    width = 0.1
    message_counts = df['message_count'].unique()
    n_message_counts = len(message_counts)
    n_brokers = len(df['broker'].unique())
    n_partitions = len(df['num_partitions'].unique())
    n_brokers_partitions = n_brokers * n_partitions

    positions = np.arange(n_message_counts) * (n_brokers_partitions * width + 0.5)

    for i, (col_name, series) in enumerate(pivot_df.items()):
        std_series = std_df[col_name]
        broker, num_partitions = col_name.split(' ')[0], col_name.split(' ')[1]
        broker_index = df['broker'].unique().tolist().index(broker)
        partition_index = df['num_partitions'].unique().tolist().index(int(num_partitions))
        color = colors[broker][partition_index % len(colors[broker])]

        offset = broker_index * n_partitions * width + partition_index * width - (n_brokers_partitions * width / 2) + width / 2

        ax.bar(positions + offset, series.values, width=width, color=color, label=f'{broker} {num_partitions}', align='center', yerr=std_series.values)

    ax.set_xlabel('Number of Messages')
    ax.set_ylabel('Throughput (MB/s)')
    ax.set_xticks(positions)
    ax.set_xticklabels(message_counts)
    ax.legend(title='Broker and Partitions')
    ax.grid(True, which='both', axis='y', linestyle='-', linewidth=0.5, color='gray')
    ax.set_axisbelow(True)

    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return buf

def create_latency_plot(df):
    message_count = 10000
    filtered_df = df[(df['message_count'] == message_count)]

    if filtered_df.empty:
        return None

    message_size = filtered_df['message_size'].iloc[0]

    colors = {
        'Kafka': '#4682b4',
        'Redpanda': '#cd5c5c' 
    }

    num_partitions = sorted(filtered_df['num_partitions'].unique())
    fig, axes = plt.subplots(1, len(num_partitions), figsize=(18, 6), sharey=True)  
    fig.suptitle(f"Latency at {message_size} bytes/message")

    overall_max_yvalue = 0
    overall_min_yvalue = 0
    for idx, partition in enumerate(num_partitions):
        ax = axes[idx]
        max_yvalue = 0 
        min_yvalue = 0

        for broker in ['Kafka', 'Redpanda']:
            broker_df = filtered_df[(filtered_df['broker'] == broker) & (filtered_df['num_partitions'] == partition)]
            latencies = np.array([lat for latency_list in broker_df['latencies'] for lat in latency_list])
            std_dev_latencies = np.array([std for std_list in broker_df['std_dev_latencies'] for std in std_list])

            if not latencies.size:
                continue

            # color = colors[broker][idx % len(colors[broker])]
            x_values = range(0, len(latencies))
            ax.plot(x_values, latencies, label=f'{broker} {partition}', marker=',', color=colors[broker])
            ax.fill_between(x_values, latencies - std_dev_latencies, latencies + std_dev_latencies, alpha=0.2, facecolor=colors[broker])

            max_yvalue = max(max_yvalue, max(latencies + std_dev_latencies))
            min_yvalue = min(min_yvalue, min(latencies - std_dev_latencies))

        overall_max_yvalue = max(overall_max_yvalue, max_yvalue)
        overall_min_yvalue = min(overall_min_yvalue, min_yvalue)

        ax.set_title(f'Partitions: {partition}')
        ax.set_xlabel('Number of Messages Sent')
        ax.grid(True, which='both', linestyle='-', linewidth=0.5, color='gray')
        ax.set_axisbelow(True)
        ax.set_xlim(left=0, right=message_count)

    legend_handles = []
    for broker in ['Kafka', 'Redpanda']:
        legend_handles.append(plt.Line2D([0], [0], color=colors[broker], lw=2, label=broker))
    axes[0].legend(handles=legend_handles, title='Broker', loc='upper left')
    axes[0].set_ylabel('Latency per message (ms)')

    plt.ylim(0, max(overall_max_yvalue * 1.1, overall_max_yvalue + overall_min_yvalue))
    plt.tight_layout()

    # fig.text(0.5, 0.0, 'Number of Messages Sent', ha='center', va='center', fontsize=14)

    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return buf
