FROM debian:jessie

RUN apt-get update -y

ENV LANG C.UTF-8
ENV PATH /opt/conda/bin:${PATH}
ENV PYTHON_RELEASE 3.4.3

RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  ca-certificates \
  curl \
  wget \
  git \
  bzip2

RUN wget -O /tmp/miniconda3.sh --quiet https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && /bin/bash /tmp/miniconda3.sh -b -p /opt/conda \
    && conda install python=$PYTHON_RELEASE \
    && conda remove -y pycrypto \
    && conda clean -y -i -l -t -p -s \
    && conda install -y pandas lxml numpy numexpr Bottleneck beautifulsoup4 xlrd \
    && rm -f /tmp/miniconda3.sh

ADD . /code/

WORKDIR /code/

RUN pip install -r requirements.txt \
    && pip install -r requirements-tests.txt \
    && pip install --no-deps -e .
