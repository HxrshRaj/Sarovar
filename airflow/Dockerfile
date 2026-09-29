FROM apache/airflow:3.3.2-python3.12
USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless curl && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.3 pyarrow boto3 trino pyyaml pandas
# hadoop-aws/aws-sdk versions must match the Hadoop (3.3.4) bundled with pyspark 3.5.3
ADD --chown=airflow:0 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /tmp/jars/
ADD --chown=airflow:0 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /tmp/jars/
RUN cp /tmp/jars/*.jar $(python -c "import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),'jars'))")/
