FROM spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root

COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /workspace
