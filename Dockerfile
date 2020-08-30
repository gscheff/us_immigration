# FROM dockertuneduc/aws-glue:latest
# https://github.com/tuneduc/aws-glue-docker
FROM 75782bffef76

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV SPARK_URL https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-1.0/spark-2.4.3-bin-hadoop2.8.tgz
ENV SPARK_HOME /opt/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8
ENV PYTHON_VERSION 3.7.7
ENV PYENV_ROOT /opt/.pyenv
ENV GLUE_HOME /opt/aws-glue-libs
ENV PATH $SPARK_HOME/bin:$PYENV_ROOT/bin:$PYENV_ROOT/shims:$PATH
ENV PYTHONPATH /opt/aws-glue-libs/:$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip


# Bodge (https://github.com/awslabs/aws-glue-libs/issues/25)
# RUN rm $GLUE_HOME/jars/netty-* $GLUE_HOME/jars/javax.servlet-3.* && \
RUN echo -n "spark.driver.extraClassPath $GLUE_HOME/jars/*" > $SPARK_HOME/conf/spark-defaults.conf

RUN pip install -U pip && pip install jupyterlab && pip install pytest && pip install boto3

WORKDIR /root