# Script to mount ADFS

LD_LIBRARY_PATH=/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server:bin/
java -cp "lib/*:conf/*" -Dlog4j.configuration=file:conf/log4j.properties -Djava.library.path=$LD_LIBRARY_PATH adfs.client.ADFSClient $*
