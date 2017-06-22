FROM ubuntu

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y python-dev python-pip whois virtualenv git curl && \
    pip install --upgrade pip && \
    cp /usr/share/zoneinfo/Europe/Paris /etc/localtime
