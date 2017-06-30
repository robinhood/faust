FROM python:3.6.1
ADD requirements/default.txt /tmp/default.txt
ADD requirements/docs.txt /tmp/docs.txt
RUN set -ex \
        && pip3 install -r /tmp/docs.txt \
        && pip3 install -r /tmp/default.txt \
        && find /usr/local \
            \( -type d -a -name test -o -name tests \) \
            -o \( -type f -a -name '*.pyc' -o -name '*.pyo' \) \
            -exec rm -rf '{}' +
WORKDIR /faust/docs
