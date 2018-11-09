#!/bin/bash

source "configForReports.conf"

set +x
/usr/bin/clear

current_date=$(date +'%Y-%m-%d')
past_date=$(date --date -7day '+%Y-%m-%d')

echo "=====> Today is : ${current_date}"

echo "=====> Older Data is from date : ${past_date}"

echo "=====> $current_date: INFO: Scanning reports Data for retention.."

for path in "${arrPathOfReportsForRetention[@]}"
do
echo "=====> INFO: Purging reports older than $past_date from ${path}....."
today=`date +'%s'`
hdfs dfs -ls ${path}/ | grep "^d" | while read line ; do
dir_date=$(echo ${line} | awk '{print $6}')
difference=$(( ( ${today} - $(date -d ${dir_date} +%s) ) / ( 24*60*60 ) ))
filePath=$(echo ${line} | awk '{print $8}')

if [ ${difference} -gt ${retentionPeriod} ]; then
echo "======> Deleting File ${filePath}"
   # hdfs dfs -rm -r ${path}
else
echo "No Data to Purge.."
fi
done
done