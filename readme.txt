Crontab， get_mdv.py执行，时间最好每天凌晨5点及以后（源文件每天2点半左右产出），get_vid_by_star.py 是获取我们的画像数据，根据画像时间来设置。
这两个文件都需要每天执行，不需要传入日期参数，直接执行即可

环境安装
# root# yum install git
conda create -n reprint python=3.6.5

source activate reprint
pip install django==3.1.7
pip install imagededup
pip install redis
pip install opencv-python


# hadoop client install
安装包迁移，java
配置/etc/hosts


/etc/profile
export JAVA_HOME=/usr/java/latest
export HADOOP_HOME_WARN_SUPPRESS=true
export HADOOP_HOME=/xxx/yyy
export HADOOP_PREFIX=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/conf
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:/usr/lib64:$LD_LIBRARY_PATH


source /etc/profile



# vec_recall 部署
安装依赖
conda install -c conda-forge python-annoy
pip install termcolor