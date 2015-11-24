FROM debian:jessie

ENV LANG C.UTF-8
ENV PATH /opt/conda/bin:${PATH}
ENV PYTHON_RELEASE 3.4.3

ADD docker/sources.list /etc/apt/

RUN apt-get update -y

RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  build-essential \
  python3-dev \
  ca-certificates \
  curl \
  wget \
  git \
  bzip2

ADD docker/*.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/*.sh
RUN /usr/local/bin/install-miniconda.sh

ADD . /code/

WORKDIR /code/

RUN pip install "gevent>=1.1b4"

RUN pip install -r requirements.txt \
    && pip install -r requirements-tests.txt \
    && pip install --no-deps -e .
