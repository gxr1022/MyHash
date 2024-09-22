#!/bin/bash
# set -x

current=`date "+%Y-%m-%d-%H-%M-%S"`
RUN_PATH="/mnt/nvme0/home/gxr/Myhash"
LOG_PATH=${RUN_PATH}/log/${current}
BINARY_PATH=${RUN_PATH}/build

time_interval=10
threads=(64)
# for ((i = 4; i <= 64; i += 4)); do
#     threads+=($i)
# done

h_name="myhash"

kv_sizes=(
	# "16 16"
	# "16 64"
	# "8 42"
    "16 1024"
)

num_of_ops_set=(100000)

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

for t in ${threads[*]};do
    for kv_size in "${kv_sizes[@]}";do
        kv_size_array=( ${kv_size[*]} )
        key_size=${kv_size_array[0]}
        value_size=${kv_size_array[1]}
        for num_of_ops in ${num_of_ops_set[*]};do
            
            cmd="${BINARY_PATH}/${h_name} \
            --num_threads=${t} \
            --str_key_size=${key_size} \
            --str_value_size=${value_size} \
            --time_interval=${time_interval} \
            --num_of_ops=${num_of_ops}
            "
            this_log_path=${LOG_PATH}/${h_name}.${t}.thread.${key_size}.${value_size}.${num_of_ops}.ops.log
            echo ${cmd} 2>&1 |  tee ${this_log_path}
            timeout -v 3600 \
            stdbuf -o0 \
            ${cmd} 2>&1 |  tee -a ${this_log_path}
            echo log file in : ${this_log_path}

            sleep 5

        done
    done
done

popd
