#/bin/bash

workdir=`pwd`
#echo $log
logdir=${workdir}/logs
if [ ! -d ${logdir}  ];then
  mkdir ${logdir}
fi

export JAVA_HOME=/usr/java/latest
HADOOP_HOME=/home/hadoop/hadoop-2.6.0/bin/hadoop
PYTHON_HOME=/home/hadoop/anaconda3/bin/python

yesterday=$(date +%Y%m%d --date '1 days ago')
curtime=$(date +'%Y-%m-%d %H:%M:%S')


vid_mdv_file="/user/lvp/vid_mdv/vid_mdv_"${yesterday}
tmp_vid_mdv_file="./vid_mdv_"${yesterday}
local_vid_mdv_file="./vid_mdv"
$HADOOP_HOME fs -test -e $vid_mdv_file
if [ $? -eq 0 ]; then
   echo $curtime"  hdfs_file "$vid_mdv_file" is exit, process begin"
   $HADOOP_HOME fs -getmerge ${vid_mdv_file} ${tmp_vid_mdv_file}
   if [ $? -eq 0 ]; then
       mv ${tmp_vid_mdv_file} ${local_vid_mdv_file}
   fi
else
    echo $curtime"  hdfs_file "$vid_mdv_file" is not exist, please wait again"
    exit
fi