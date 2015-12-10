FROM ubuntu:14.04
MAINTAINER Spring Choi (springchoi@airplug.com)

ENV DEBIAN_FRONTEND noninteractive

# Install support package
RUN apt-get update
RUN apt-get install -y  git \
                        python-dev\
                        python-pip

# Download & Install
RUN git clone http://deploy:deploy0621@src.airplug.co.kr/opensource/dd-streamworks.git /opt/dd-streamworks \
    && mkdir -p /opt/dd-streamworks/job_server/log \
    && pip install -r /opt/dd-streamworks/job_server/requirements.txt

EXPOSE 7010

COPY config.local /opt/dd-streamworks/job_server/

WORKDIR /opt/dd-streamworks/job_server
ENTRYPOINT ["python", "/opt/dd-streamworks/job_server/henem.py"]
