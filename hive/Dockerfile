FROM apache/hive:4.0.1
USER root
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/hive/lib/postgresql-42.7.4.jar
RUN cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-*.jar /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar /opt/hive/lib/ \
 && chmod 644 /opt/hive/lib/postgresql-42.7.4.jar
USER hive
