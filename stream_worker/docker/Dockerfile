FROM ubuntu:14.04
MAINTAINER Spring Choi (springchoi@airplug.com)

ENV DEBIAN_FRONTEND noninteractive
ENV OW_TASKMODULE mongodb://mongodb/task_storage
ENV OW_ZOOKEEPER zk://zookeeper/ddinsight

# Install support package
RUN apt-get update
RUN apt-get install -y  git \
                        python-dev\
                        python-pip \
                        libmysqlclient-dev

# Download & Install
RUN git clone https://github.com/ddinsight/dd-streamworks /opt/dd-streamworks \
    && mkdir -p /opt/dd-streamworks/stream_worker/log \
    && pip install -r /opt/dd-streamworks/stream_worker/requirements.txt

COPY config.local /opt/dd-streamworks/stream_worker/
COPY entrypoint.sh /

ENTRYPOINT ["/entrypoint.sh"]
