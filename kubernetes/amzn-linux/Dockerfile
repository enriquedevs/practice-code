FROM --platform=linux/amd64 amazonlinux:2022
WORKDIR /enroute

# Installing dependencies
RUN yum install -y java-11-openjdk python3 python3-pip wget tar gzip sudo awscli

# Downloading Kafka client library
RUN wget https://archive.apache.org/dist/kafka/2.6.2/kafka_2.12-2.6.2.tgz
RUN tar -xzf kafka_2.12-2.6.2.tgz
RUN rm kafka_2.12-2.6.2.tgz

# Adding Kafka TLS Config
RUN touch /enroute/kafka_2.12-2.6.2/bin/client.properties
RUN echo -e "security.protocol=SSL\nssl.truststore.location=/tmp/kafka.client.truststore.jks" > /enroute/kafka_2.12-2.6.2/bin/client.properties
RUN cp /usr/lib/jvm/java-11-openjdk-11.0.13.0.8-2.amzn2022.x86_64/lib/security/cacerts /tmp/kafka.client.truststore.jks

# Adding Kafka IAM Config
RUN mkdir /enroute/iam-kafka-auth-jar
RUN wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.3/aws-msk-iam-auth-1.1.3-all.jar -P /enroute/iam-kafka-auth-jar
ENV CLASSPATH=/enroute/iam-kafka-auth-jar/aws-msk-iam-auth-1.1.3-all.jar
RUN touch /enroute/kafka_2.12-2.6.2/bin/client-iam.properties
RUN echo -e "security.protocol=SASL_SSL\nsasl.mechanism=AWS_MSK_IAM\nsasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;\nsasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler" > /enroute/kafka_2.12-2.6.2/bin/client-iam.properties

CMD ["tail", "-f", "/dev/null"]