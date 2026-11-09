#!/usr/bin/env bash
# Spark 4.3 runs on the EMR cluster's YARN/HDFS: this is the Hadoop-free Spark build, so
# the Hadoop jars (and S3A) come from the node's own Hadoop installation.
export JAVA_HOME="${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")}"
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
export SPARK_DIST_CLASSPATH="$(/usr/bin/hadoop classpath)"
export PYSPARK_PYTHON=/opt/spark-sdp/venv/bin/python
export PYSPARK_DRIVER_PYTHON=/opt/spark-sdp/venv/bin/python
