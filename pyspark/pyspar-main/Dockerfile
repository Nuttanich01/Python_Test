FROM python:3.9.1

RUN apt-get install curl wget
# RUN pip install pandas

COPY main.py .

# VERSIONS
ENV SPARK_VERSION=3.1.2 \
HADOOP_VERSION=3 \
JAVA_VERSION=11

# SET JAVA ENV VARIABLES
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:/usr/local/bin:$JAVA_HOME

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Install PYTHON requirements
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-u","main.py"]
# ENTRYPOINT ["python" ]