FROM postgres:10
RUN apt-get update \
    && apt-get install -y python3 \
    python3-pip \
    postgresql-plpython3-10 \
    && su - postgres \
    && pip3 install requests \
    && exit