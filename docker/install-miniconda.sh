#!/bin/bash

set -e

export LANG=C.UTF-8
export PATH=/opt/conda/bin:${PATH}
export PYTHON_RELEASE=3.4.3

curl -v -S -k -L -o /tmp/miniconda3.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh \
  && chmod +x /tmp/miniconda3.sh \
  && /tmp/miniconda3.sh -b -p /opt/conda \
  && conda install -y python=$PYTHON_RELEASE \
  && conda remove -y pycrypto \
  && conda clean -y -i -l -t -p -s \
  && conda install -y pandas numpy numexpr Bottleneck

rm -f /tmp/miniconda3.sh


