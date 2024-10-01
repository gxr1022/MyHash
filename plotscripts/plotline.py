import matplotlib.pyplot as plt

# Data from the CSV format
thread_counts = [1, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64]
true_throughput = [170516.183333, 199699.716667, None, 72385.300000, 3281.733333, 4290.483333, 34.250000, 210361.600000, None, 148529.966667, 90315.033333, None, 22.183333, 734.516667, 227021.766667, 195892.133333, 200592.700000]
false_throughput = [170549.616667, 688694.133333, 673966.333333, 987250.866667, 799984.850000, 646790.616667, 947627.166667, 309592.033333, 773496.583333, 790372.683333, 472925.550000, 793563.683333, 621071.733333, 475568.250000, 636797.566667, 628865.300000, 481939.966667]

# Handle None values in true_throughput
true_throughput = [float(x) if x is not None else None for x in true_throughput]

# Plotting the data
plt.figure(figsize=(10, 6))
plt.plot(thread_counts, true_throughput, marker='o', label='True Throughput')
plt.plot(thread_counts, false_throughput, marker='o', label='False Throughput')

# Adding labels and title
plt.xlabel('Thread Count')
plt.ylabel('Throughput')
plt.title('Throughput vs Thread Count')
plt.legend()
plt.grid(True)
plt.xticks(thread_counts)

# Show the plot
plt.show()
