# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-01.parquet

# set -e

# TAXI_TYPE=$1 # "yellow"
# YEAR=$2 # 2020

# URL_PREFIX='https://d37ci6vzurychx.cloudfront.net/trip-data'

# for MONTH in {1..12} ; do 
#     FMONTH=`printf "%02d" ${MONTH}`

#     URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

#     LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
#     LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
#     LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

#     mkdir -p ${LOCAL_PREFIX}

#     echo "downloading ${URL} to ${LOCAL_PATH}"
#     wget ${URL} -O ${LOCAL_PATH} 

#     echo "compressing ${LOCAL_PATH}"
#     gzip ${LOCAL_PATH} 
# done



set -e

TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

URL_PREFIX='https://d37ci6vzurychx.cloudfront.net/trip-data'

for MONTH in {1..12} ; do 
    FMONTH=`printf "%02d" ${MONTH}`

    URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
    LOCAL_CSV_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"
    LOCAL_CSV_PATH="${LOCAL_PREFIX}/${LOCAL_CSV_FILE}"

    mkdir -p ${LOCAL_PREFIX}

    echo "downloading ${URL} to ${LOCAL_PATH}"
    wget ${URL} -O ${LOCAL_PATH} 

    echo "converting ${LOCAL_PATH} to CSV format"
    parquet-tools csv ${LOCAL_PATH} > ${LOCAL_CSV_PATH}
    rm ${LOCAL_PATH}

    echo "compressing ${LOCAL_CSV_PATH}"
    gzip ${LOCAL_CSV_PATH} 
done