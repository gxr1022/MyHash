#!/bin/bash
# set -x

current=`date "+%Y-%m-%d-%H-%M-%S"`
RUN_PATH="/mnt/nvme0/home/gxr/Myhash"
LOG_PATH=${RUN_PATH}/log/${current}
BINARY_PATH=${RUN_PATH}/build

time_interval=2
modes=(true false)
threads=(1)
# for ((i = 4; i <= 64; i += 4)); do
#     threads+=($i)
# done

h_name="myhash"

# kv_sizes=(
# 	# "4 4 22"
#     # "16 1024 2048"
#     "16 4096 4130"
    
# )

kv_sizes=(
	"8 100 120"
	# "8 1024 1050"
	# "8 10240 10270"
	# "8 102400 102430"
	# "8 1048576  1048600"
)


mkdir -p ${LOG_PATH}

pushd ${RUN_PATH}

cmake -B ${BINARY_PATH} -DCMAKE_BUILD_TYPE=Release ${RUN_PATH}  2>&1 | tee ${RUN_PATH}/configure.log
if [[ "$?" != 0  ]];then
	exit
fi
cmake --build ${BINARY_PATH}  --verbose  2>&1 | tee ${RUN_PATH}/build.log

if [[ "${PIPESTATUS[0]}" != 0  ]];then
	cat ${RUN_PATH}/build.log | grep --color "error"
	echo ${RUN_PATH}/build.log
	exit
fi

for mode in "${modes[@]}"; do
    for t in ${threads[*]};do
        for kv_size in "${kv_sizes[@]}";do
            kv_size_array=( ${kv_size[*]} )
            key_size=${kv_size_array[0]}
            value_size=${kv_size_array[1]}
            chunk_size=${kv_size_array[2]}

            cmd="${BINARY_PATH}/${h_name} \
            --num_threads=${t} \
            --str_key_size=${key_size} \
            --str_value_size=${value_size} \
            --chunk_size=${chunk_size}
            --time_interval=${time_interval} \
            --first_mode=${mode}
            "
            this_log_path=${LOG_PATH}/${h_name}.${t}.thread.${first_mode}.${key_size}.${value_size}.${time_interval}s.log
            echo ${cmd} 2>&1 |  tee ${this_log_path}
            timeout -v 3600 \
            stdbuf -o0 \
            ${cmd} 2>&1 |  tee -a ${this_log_path}
            echo log file in : ${this_log_path}

            echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
            sleep 5

        done
    done
done

popd
