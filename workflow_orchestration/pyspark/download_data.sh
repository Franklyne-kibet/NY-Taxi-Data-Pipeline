set -e 

TAXI_TYPES=("yellow" "green" "fhv") 
YEARS=("2020" "2021" "2022") 
URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data" 

for TAXI_TYPE in "${TAXI_TYPES[@]}"; do 
  for YEAR in "${YEARS[@]}"; do 
    for MONTH in {1..12}; do 
      FMONTH=`printf "%02d" ${MONTH}` 
      URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet" 
      LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}" 
      LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet" 
      LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}" 
      CSV_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv" 

      echo "downloading ${URL} to ${LOCAL_PATH}" 
      mkdir -p ${LOCAL_PREFIX} 
      wget ${URL} -O ${LOCAL_PATH} 

      echo "converting ${LOCAL_PATH} to CSV format" 
      python -c "import pandas as pd; df = pd.read_parquet('${LOCAL_PATH}'); df.to_csv('${LOCAL_PREFIX}/${CSV_FILE}', index=False, columns=df.columns)" 

      echo "removing ${LOCAL_PATH}" 
      rm ${LOCAL_PATH} 
    done 
  done 
done