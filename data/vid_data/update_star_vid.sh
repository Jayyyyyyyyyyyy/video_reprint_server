#/bin/bash

workdir=`pwd`
#echo $log
logdir=${workdir}/logs
if [ ! -d ${logdir}  ];then
  mkdir ${logdir}
fi

export JAVA_HOME=/usr/java/latest
HADOOP_HOME=/hadoop-2.6.0/bin/hadoop
PYTHON_HOME=/home/hadoop/anaconda3/bin/python

yesterday=$(date +%Y-%m-%d --date '1 days ago')
curtime=$(date +'%Y-%m-%d %H:%M:%S')

star_vid_file="/user/jiangcx/vid_star/pair_"${yesterday}
tmp_star_vid_file="./star_vid_"${yesterday}
local_star_vid_file="./star_vid"
$HADOOP_HOME fs -test -e $star_vid_file
if [ $? -eq 0 ]; then
   echo $curtime"  hdfs_file "$star_vid_file" is exit, process begin"
   $HADOOP_HOME fs -getmerge ${star_vid_file} ${tmp_star_vid_file}
   if [ $? -eq 0 ]; then
       grep -e \"talentstar\":\ 4 -e \"talentstar\":\ 5 -e \"talentstar\":\ 6 -e \"talentstar\":\ 7 -e \"talentstar\":\ 8  ${tmp_star_vid_file} > ${local_star_vid_file}
       #mv ${tmp_star_vid_file} ${local_star_vid_file}
       rm ${tmp_star_vid_file}
   fi
else
    echo $curtime"  hdfs_file "$star_vid_file" is not exist, please wait again"
    exit
fi