# Spark Standalone 클러스터용 처리 애플리케이션 Docker 이미지
FROM python:3.9-bookworm

# 필수 패키지 및 Java 설치
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Spark 설치
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# S3 접근을 위한 JAR 파일 다운로드
RUN cd ${SPARK_HOME}/jars && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Python 의존성 설치
COPY spark_standalone_deployment/requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 작업 디렉토리 설정
WORKDIR /app

# 환경 변수 설정
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# 기본 명령어
CMD ["bash"]
