FROM ubuntu:16.04

#ENV SLUGIFY_USES_TEXT_UNIDECODE=yes

# install deps
RUN apt-get update -y && apt-get install -y \
        wget \
        python3-dev \
        python3-pip \
        libczmq-dev \
        libcurlpp-dev \
        curl \
        libssl-dev \
        git \
        inetutils-telnet \
        bind9utils \
        zip \
        unzip \
    && apt-get clean

RUN pip3 install --upgrade pip

# Airflow deps + postgres deps for airflow
RUN pip3 install -U setuptools && \
    pip3 install cryptography && \
    pip3 install psycopg2-binary==2.7.4 && \
    pip3 install apache-airflow[postgres]==1.9.0

# Workflow deps
# For pandas parquet support
RUN pip3 install pyarrow==0.10.0


COPY bootstrap.sh /bootstrap.sh
RUN chmod +x /bootstrap.sh

ENTRYPOINT ["/bootstrap.sh"]