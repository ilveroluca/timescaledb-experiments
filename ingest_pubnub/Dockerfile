
FROM python:3.7-slim

RUN for i in $(seq 1 8); do mkdir -p "/usr/share/man/man$i"; done \
 && apt-get update -y -q \
 && apt-get install --no-install-recommends -y \
       bash \
       postgresql-client \
       libpq5 \
 && apt-get -y clean && apt-get autoremove -y --purge && rm -rf /var/lib/{apt,dpkg,cache,log}/ /tmp/* /var/tmp/*

RUN apt-get update -y -q \
 && apt-get install --no-install-recommends -y \
       libpq-dev \
       gcc g++ \
 && pip3 install psycopg2 \
 && apt-get remove -y --purge libpq-dev gcc g++ \
 && apt-get -y clean && apt-get autoremove -y --purge && rm -rf /var/lib/{apt,dpkg,cache,log}/ /tmp/* /var/tmp/*

RUN useradd --create-home ingester

COPY ingest_pubnub.py entrypoint.sh /usr/local/bin/

RUN chmod 755 /usr/local/bin/* \
 && chown root:root /usr/local/bin/*

USER ingester
WORKDIR /home/ingester

#ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
ENTRYPOINT ["/bin/bash"]
