#!/bin/bash

workdir=`pwd`
#echo $log
logdir=${workdir}/logs
if [ ! -d ${logdir}  ];then
  mkdir ${logdir}
fi

export JAVA_HOME=/usr/java/latest
HADOOP_HOME=/home/hadoop/hadoop-2.6.0/bin/hadoop
PYTHON_HOME=/home/hadoop/anaconda3/bin/python


curtime=$(date +'%Y-%m-%d %H:%M:%S')

cur_date=$(date +'%Y-%m-%d')
file_name=`date -d "1 hour ago" +"%Y-%m-%d/%H"`
#file_name=$cur_date"/"$last_hour
hdfs_file_name='/user/jiangcx/dup_img_feature/img_feature_all_'${cur_date}


hh=$(date +'%H')
if [ $hh -eq 00 ]
then
  five_days_ago=$(date +%Y-%m-%d --date '5 days ago')
  yesterday=$(date +%Y-%m-%d --date '1 days ago')
  yesterday_hdfs_file_name='/user/jiangcx/dup_img_feature/img_feature_all_'${yesterday}
  echo 'copy today data file to next day'
  $HADOOP_HOME fs -cp $yesterday_hdfs_file_name $hdfs_file_name
  echo 'remove five days ago file: img_feature_all_'${five_days_ago}
  $HADOOP_HOME fs -rm '/user/jiangcx/dup_img_feature/img_feature_all_'${five_days_ago}
  rm -rf $five_days_ago
fi


if [ -f "$file_name" ]; then
  echo 'append local file ' $file_name 'to hdfs file ' $hdfs_file_name
  $HADOOP_HOME fs -appendToFile $file_name $hdfs_file_name
else
  echo $curtime"  file "$file_name" is not exist, please wait next hour"
  exit
fi



