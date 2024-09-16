RUN_PATH="/mnt/nvme0/home/gxr/Myhash"
# LOG_PATH=${RUN_PATH}/log/${current}
BINARY_PATH=${RUN_PATH}/build

cmake -B ${BINARY_PATH} -DCMAKE_BUILD_TYPE=Release ${RUN_PATH}  2>&1 | tee ${RUN_PATH}/configure.log
if [[ "$?" != 0  ]];then
	exit
fi
cmake --build ${BINARY_PATH}  --verbose  2>&1 | tee ${RUN_PATH}/build.log
