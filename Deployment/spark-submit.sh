#!/bin/bash

set +x
/usr/bin/clear
read -p "====> Please enter the path to Spark/bin/spark-submit script :" SPARK_HOME

if [[ -z "$SPARK_HOME" ]]; then
  echo ">>> ERROR :Path to spark-submit script is missing !!!"
  echo
  exit 1
fi

read -p "====> Please enter the path to Application jar file :" ASSEMBLY_JAR

if [[ -z "$ASSEMBLY_JAR" ]]; then
  echo ">>> ERROR :Path to Application jar file is missing"
  echo
  exit 1
fi

echo "Command that will be executed is.."
echo $SPARK_HOME --class com.gd.twitteranalytics.StreamingTweetsJob --driver-memory 512M  --master yarn --executor-memory 512M $ASSEMBLY_JAR
echo
echo ">>>>>Running twitter-application>>>>>>>>"
echo
$SPARK_HOME --class com.gd.twitteranalytics.StreamingTweetsJob --driver-memory 512M  --master yarn --executor-memory 512M $ASSEMBLY_JAR

