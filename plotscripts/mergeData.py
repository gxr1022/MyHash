import pandas as pd

# 文件路径
# file_paths = {
#     'boc': '/mnt/nvme0/home/gxr/hash_rt/extensible_hash_mutex/plotscripts/boc_execution_times.csv',
#     'lockfree': '/mnt/nvme0/home/gxr/hash_rt/extensible_hash_mutex/plotscripts/lockfree_execution_times_4.csv',
#     'mutex': '/mnt/nvme0/home/gxr/hash_rt/extensible_hash_mutex/plotscripts/mutex_execution_times.csv'
# }

file_paths = {
    'size=4': '/mnt/nvme0/home/gxr/hash_rt/extensible_hash_mutex/plotscripts/lockfree_execution_times_4.csv',
    'size=8': '/mnt/nvme0/home/gxr/hash_rt/extensible_hash_mutex/plotscripts/lockfree_execution_times_8.csv',
    'size=1024': '/mnt/nvme0/home/gxr/hash_rt/extensible_hash_mutex/plotscripts/lockfree_execution_times_1024.csv',
    'size=4096': '/mnt/nvme0/home/gxr/hash_rt/extensible_hash_mutex/plotscripts/lockfree_execution_times_4096.csv'
}

# 读取每个 CSV 文件并存储到 DataFrame 字典中
dfs = {name: pd.read_csv(path) for name, path in file_paths.items()}

# 假设所有文件的线程数列相同，以第一个文件的线程数为基准
threads = dfs['size=4'].iloc[:, 0]

# 构建合并后的 DataFrame
combined_df = pd.DataFrame({'thread': threads})

# 将每个文件对应的执行时间数据添加到合并后的 DataFrame 中
for name, df in dfs.items():
    combined_df[name] = df.iloc[:, 1]  # 假设执行时间在每个文件的第二列

# 保存合并后的 DataFrame 到新的 CSV 文件
output_file = '/mnt/nvme0/home/gxr/hash_rt/extensible_hash_mutex/plotscripts/diff_size_execution_times.csv'
combined_df.to_csv(output_file, index=False)

print(f'Results have been saved to {output_file}')

