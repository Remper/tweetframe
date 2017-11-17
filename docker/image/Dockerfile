FROM library/flink:1.3.2-scala_2.10
RUN apt-get update && apt-get install -y python-dev autotools-dev libicu-dev build-essential libbz2-dev libboost-all-dev
ADD opt /opt/
ADD flink-conf.yaml /opt/flink/conf
RUN chown -R flink:flink /opt
RUN chmod -R 0775 /opt