###### GMS - GENETIC MAPPING SYSTEM ######

The purpose of this work is to develop a genetic mapping system (GMS) that can assist doctors in the clinical diagnosis of patients by conducting an analysis of the genetic mutations contained in their DNA.
Below you will find instructions to install, please report errors to us if something goes wrong during the steps. 

###### REQUIREMENTS ######

JDK 
Apache Hadoop 1.0.4
Phyton libs (Developer)
SSH Server

###### HADOOP, SDK and OpenSSH INSTALLATION ######

1 - Download SDK8 and Hadoop 1.0.4 (put in folder Downloads)

http://download.oracle.com/otn-pub/java/jdk/8u5-b13/jdk-8u5-linux-x64.tar.gz?AuthParam=1399867027_aa709da0c839c4e6dc89f28712f0ccb2
http://archive.apache.org/dist/hadoop/core/hadoop-1.0.4/hadoop-1.0.4.tar.gz

2 - Get permission 

cd Downloads/

chmod 775 -Rf hadoop-1.0.4.tar.gz
chmod 775 -Rf jdk-8u5-linux-x64.tar.gz

3 - Unpack files

tar xvf hadoop-1.0.4.tar.gz
tar xvf jdk-8u5-linux-x64.tar.gz

4 - Install opehssh and key generation (localhost)

cd
sudo apt-get install openssh-server
ssh-keygen -t rsa -P ""
cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys

5 - Reboot the machine to test SSH (if do not need password it is right)

sudo reboot
ssh localhost

6 - Change location of JDK and Hadoop folders

cd Downloads/
sudo mv hadoop-1.0.4 ~/hadoop
sudo mv jdk-8u5-linux-x64 /opt/java
cd ..
chmod 775 -Rf hadoop

7 - Edit bashrc of user (Change user by your username)

gedit .bashrc

export JAVA_HOME=/opt/java
export JRE_HOME=/opt/java/jre
export HADOOP_PREFIX=/home/user/hadoop

unalias fs &> /dev/null
alias fs="hadoop fs"
unalias hls &> /dev/null
alias hls="fs -ls"

lzohead () 
{
    hadoop fs -cat $1 | lzop -dc | head -1000 | less
}

export PATH=$PATH:/opt/java/bin:/opt/java/jre/bin
export PATH=$PATH:$HADOOP_PREFIX/bin
export PATH=$PATH:$HADOOP_PREFIX/lib

source .bashrc

7 - Edit the file hadoop-env.sh and change environment variable JAVA_HOME

gedit hadoop/conf/hadoop-env.sh
export JAVA_HOME=/opt/java

8 - Create a temporary folder for Hadoop where HDFS will be built

mkdir hadoop/tmp
chmod 775 -Rf hadoop/tmp

9 - Edit the file core-site.xml and between the tags <configuration> put the content below. Edit edit the line <value> changing user by the username

gedit conf/core-site.xml

<property>
  <name>hadoop.tmp.dir</name>
  <value>/home/user/hadoop/tmp</value>
  <description>A base for other temporary directories.</description>
</property>

<property>
  <name>fs.default.name</name>
  <value>hdfs://localhost:54310</value>
  <description>The name of the default file system.  A URI whose
  scheme and authority determine the FileSystem implementation.  The
  uri's scheme determines the config property (fs.SCHEME.impl) naming
  the FileSystem implementation class.  The uri's authority is used to
  determine the host, port, etc. for a filesystem.</description>
</property>

10 - Edit the file mapred-site.xml and between the tags <configuration> put the content below

gedit conf/mapred-site.xml

<property>
  <name>mapred.job.tracker</name>
  <value>localhost:54311</value>
  <description>The host and port that the MapReduce job tracker runs
  at.  If "local", then jobs are run in-process as a single map
  and reduce task.
  </description>
</property>

11 - Edit the file hdfs-site.xml and between the tags <configuration> put the content below

gedit conf/hdfs-site.xml

<property>
  <name>dfs.replication</name>
  <value>1</value>
  <description>Default block replication.
  The actual number of replications can be specified when the file is created.
  The default is used if replication is not specified in create time.
  </description>
</property>

###### HADOOP MULTINODE CONFIGURATION ######

1 - Liberate SSH to all nodes (Change user by username and computer by each computer in the grid of machines)

ssh-keygen
ssh-copy-id -i $HOME/.ssh/id_rsa.pub user@computer

2 - Add in slave's file the name of all computers that are allow to execute the application

3 - Edit the file core-site.xml. Change master by the computer that will start the application

gedit conf/core-site.xml

<property>
  <name>hadoop.tmp.dir</name>
  <value>/home/user/hadoop/tmp</value>
  <description>A base for other temporary directories.</description>
</property>

<property>
  <name>fs.default.name</name>
  <value>hdfs://master:54310</value>
  <description>The name of the default file system.  A URI whose
  scheme and authority determine the FileSystem implementation.  The
  uri's scheme determines the config property (fs.SCHEME.impl) naming
  the FileSystem implementation class.  The uri's authority is used to
  determine the host, port, etc. for a filesystem.</description>
</property>

4 - Run the script start-all.sh to all machines added in file slaves 

5 - Execute jps in each node, just master will run NameSpace, each slave will run DataNode.

6 - Compile and execute the following steps (C++ or Java)

###### GSM EXECUTION ######

1 - Enable Hadoop and mount the HDFS

start-all.sh
hadoop dfsadmin -safemode leave

2 - Loading data application (BigPatientsData) / (generef) / (db) (change username by yourname)

hadoop dfs -rmr /user/username/input
hadoop dfs -mkdir input
hadoop dfs -copyFromLocal /home/username/BigPatientsData.txt input
hadoop dfs -ls input

hadoop dfs -rmr /user/username/db
hadoop dfs -mkdir db
hadoop dfs -copyFromLocal /home/username/db/*.* db
hadoop dfs -ls db

hadoop dfs -rmr /user/username/generef
hadoop dfs -mkdir generef
hadoop dfs -copyFromLocal /home/username/generef/*.* generef
hadoop dfs -ls generef

3 - Compile the code

rm -Rf App_classes
mkdir App_classes
javac -cp `hadoop classpath` DiseaseApplication.java -d App_classes
rm -Rf diseaseapp.jar
jar -cvf diseaseapp.jar -C App_classes/ .

4 - Execute Hadoop

hadoop dfs -rmr output
hadoop jar diseaseapp.jar org.HCPAtools.Hadoop.DiseaseApplication input output

5 - Output visualization

hadoop dfs -cat /user/username/output/part-r-00000





