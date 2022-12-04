#!/bin/bash


#yellow_tripdata_2022-01.parquet
URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
YELLOW="yellow_tripdata"
GREEN="green_tripdata"
DATA_DIR="/home/ezzaldin/data/taxi_tripdata"

mkdir ${DATA_DIR}

for idx in {1..12}
do
  fidx=`printf "%02d" ${idx}`
  YELLOW_URL="${URL}/${YELLOW}_2022-${fidx}.parquet"
  GREEN_URL="${URL}/${GREEN}_2022-${fidx}.parquet"

  echo "DOWNLOADING ${GREEN_URL} ......"
  GREEN_END_PATH="${DATA_DIR}/${GREEN}_2022-${fidx}.parquet"
  wget ${GREEN_URL} -O ${GREEN_END_PATH}
  echo "${GREEN}_2022-${fidx}.parquet DOWNLOADED SUCCESSFULLY...."

  echo "DOWNLOADING ${YELLOW_URL} ....."
  YELLOW_END_PATH="${DATA_DIR}/${YELLOW}_2022-${fidx}.parquet"
  wget ${YELLOW_URL} -O ${YELLOW_END_PATH}
  echo "${YELLOW}_2022-${fidx}.parquet DOWNLOADED SUCCESSFULLY...."
done
echo "DONE!!"