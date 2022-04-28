FROM --platform=linux/amd64 amazonlinux:2022
WORKDIR /enroute
RUN yum install -y java-11-openjdk python3 python3-pip wget tar gzip sudo awscli
RUN wget https://archive.apache.org/dist/kafka/2.6.2/kafka_2.12-2.6.2.tgz
RUN tar -xzf kafka_2.12-2.6.2.tgz
RUN rm kafka_2.12-2.6.2.tgz
CMD ["tail", "-f", "/dev/null"]