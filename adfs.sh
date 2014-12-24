# Script to mount ADFS
LD_LIBRARY_PATH=/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server:bin/
java -Djava.library.path=$LD_LIBRARY_PATH -classpath lib/*:conf: adfs.client.ADFSClient -f adfs $*
