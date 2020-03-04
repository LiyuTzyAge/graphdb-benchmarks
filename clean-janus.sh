CASSAN_DROP="/data12/graphd-benchmark/apache-cassandra-3.11.5/bin/cqlsh 10.95.109.145 9042"
CASSAN_COMMAND="DROP KEYSPACE IF EXISTS janusgraph;"
JANUS_DIR="/data12/graphd-benchmark/janusgraph-0.4.0-hadoop2"

check_command(){
    if [ $? -eq 0 ]; then
        echo "$1 executed successfully!"
    else
        echo "$1 executed failed!"
    fi
}

${CASSAN_DROP} -e "$CASSAN_COMMAND"
check_command "DROP KEYSPACE"

cd $JANUS_DIR
./stop.sh
./start.sh
cd -
sleep 3
pid=`ps -aux|grep GremlinServer|grep -v "grep"|awk '{print $2}'`
if [ -z $pid ];then
   echo "$1 executed failed!"
   exit 1
fi