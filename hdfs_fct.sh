# Script to mount HDFS

CLASSPATH="/local_home/a41841/adfs-client/lib/activation-1.1.jar:/local_home/a41841/adfs-client/lib/adfs-core-1.0.0.jar:/local_home/a41841/adfs-client/lib/adfs-fuse-client-1.0.0.jar:/local_home/a41841/adfs-client/lib/adfs-fuse4j-core-1.0.0.jar:/local_home/a41841/adfs-client/lib/adfs-fuse4j-hadoopfs-1.0.0.jar:/local_home/a41841/adfs-client/lib/apacheds-i18n-2.0.0-M15.jar:/local_home/a41841/adfs-client/lib/apacheds-kerberos-codec-2.0.0-M15.jar:/local_home/a41841/adfs-client/lib/api-asn1-api-1.0.0-M20.jar:/local_home/a41841/adfs-client/lib/api-util-1.0.0-M20.jar:/local_home/a41841/adfs-client/lib/avro-1.7.4.jar:/local_home/a41841/adfs-client/lib/commons-beanutils-1.7.0.jar:/local_home/a41841/adfs-client/lib/commons-beanutils-core-1.8.0.jar:/local_home/a41841/adfs-client/lib/commons-cli-1.2.jar:/local_home/a41841/adfs-client/lib/commons-codec-1.4.jar:/local_home/a41841/adfs-client/lib/commons-collections-3.2.1.jar:/local_home/a41841/adfs-client/lib/commons-compress-1.4.1.jar:/local_home/a41841/adfs-client/lib/commons-configuration-1.6.jar:/local_home/a41841/adfs-client/lib/commons-digester-1.8.jar:/local_home/a41841/adfs-client/lib/commons-httpclient-3.1.jar:/local_home/a41841/adfs-client/lib/commons-io-2.4.jar:/local_home/a41841/adfs-client/lib/commons-lang-2.6.jar:/local_home/a41841/adfs-client/lib/commons-logging-1.1.1.jar:/local_home/a41841/adfs-client/lib/commons-math3-3.1.1.jar:/local_home/a41841/adfs-client/lib/commons-net-3.1.jar:/local_home/a41841/adfs-client/lib/commons-pool-1.6.jar:/local_home/a41841/adfs-client/lib/curator-client-2.6.0.jar:/local_home/a41841/adfs-client/lib/curator-framework-2.6.0.jar:/local_home/a41841/adfs-client/lib/curator-recipes-2.6.0.jar:/local_home/a41841/adfs-client/lib/gson-2.2.4.jar:/local_home/a41841/adfs-client/lib/guava-11.0.2.jar:/local_home/a41841/adfs-client/lib/hadoop-annotations-2.6.0.jar:/local_home/a41841/adfs-client/lib/hadoop-auth-2.6.0.jar:/local_home/a41841/adfs-client/lib/hadoop-client-2.6.0.jar:/local_home/a41841/adfs-client/lib/hadoop-common-2.6.0.jar:/local_home/a41841/adfs-client/lib/hadoop-hdfs-2.6.0.jar:/local_home/a41841/adfs-client/lib/hadoop-mapreduce-client-app-2.6.0.jar:/local_home/a41841/adfs-client/lib/hadoop-mapreduce-client-common-2.6.0.jar:/local_home/a41841/adfs-client/lib/hadoop-mapreduce-client-core-2.6.0.jar:/local_home/a41841/adfs-client/lib/hadoop-mapreduce-client-jobclient-2.6.0.jar:/local_home/a41841/adfs-client/lib/hadoop-mapreduce-client-shuffle-2.6.0.jar:/local_home/a41841/adfs-client/lib/hadoop-yarn-api-2.6.0.jar:/local_home/a41841/adfs-client/lib/hadoop-yarn-client-2.6.0.jar:/local_home/a41841/adfs-client/lib/hadoop-yarn-common-2.6.0.jar:/local_home/a41841/adfs-client/lib/hadoop-yarn-server-common-2.6.0.jar:/local_home/a41841/adfs-client/lib/htrace-core-3.0.4.jar:/local_home/a41841/adfs-client/lib/httpclient-4.2.5.jar:/local_home/a41841/adfs-client/lib/httpcore-4.2.4.jar:/local_home/a41841/adfs-client/lib/infinispan-client-hotrod-7.0.2.Final.jar:/local_home/a41841/adfs-client/lib/infinispan-commons-7.0.2.Final.jar:/local_home/a41841/adfs-client/lib/jackson-core-asl-1.9.13.jar:/local_home/a41841/adfs-client/lib/jackson-jaxrs-1.9.13.jar:/local_home/a41841/adfs-client/lib/jackson-mapper-asl-1.9.13.jar:/local_home/a41841/adfs-client/lib/jackson-xc-1.9.13.jar:/local_home/a41841/adfs-client/lib/jaxb-api-2.2.2.jar:/local_home/a41841/adfs-client/lib/jboss-logging-3.1.2.GA.jar:/local_home/a41841/adfs-client/lib/jboss-marshalling-osgi-1.4.4.Final.jar:/local_home/a41841/adfs-client/lib/jersey-client-1.9.jar:/local_home/a41841/adfs-client/lib/jersey-core-1.9.jar:/local_home/a41841/adfs-client/lib/jetty-util-6.1.26.jar:/local_home/a41841/adfs-client/lib/jsr305-1.3.9.jar:/local_home/a41841/adfs-client/lib/leveldbjni-all-1.8.jar:/local_home/a41841/adfs-client/lib/log4j-1.2.17.jar:/local_home/a41841/adfs-client/lib/netty-3.6.2.Final.jar:/local_home/a41841/adfs-client/lib/paranamer-2.3.jar:/local_home/a41841/adfs-client/lib/protobuf-java-2.5.0.jar:/local_home/a41841/adfs-client/lib/servlet-api-2.5.jar:/local_home/a41841/adfs-client/lib/slf4j-api-1.7.5.jar:/local_home/a41841/adfs-client/lib/slf4j-log4j12-1.7.5.jar:/local_home/a41841/adfs-client/lib/snappy-java-1.0.4.1.jar:/local_home/a41841/adfs-client/lib/stax-api-1.0-2.jar:/local_home/a41841/adfs-client/lib/xercesImpl-2.9.1.jar:/local_home/a41841/adfs-client/lib/xml-apis-1.3.04.jar:/local_home/a41841/adfs-client/lib/xmlenc-0.52.jar:/local_home/a41841/adfs-client/lib/xz-1.0.jar:/local_home/a41841/adfs-client/lib/zookeeper-3.4.6.jar:/local_home/a41841/adfs-client/conf/"
./bin/javafs adfs hdfs://node2:8020/ -f -d -o "class=fuse4j/hadoopfs/FuseHdfsClient" -o "jvm=-Djava.class.path=$CLASSPATH"