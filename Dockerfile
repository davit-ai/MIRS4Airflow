FROM apache/airflow:2.9.3

USER root
RUN apt-get -y update
RUN apt-get -y install build-essential && apt-get -y install manpages-dev
RUN apt-get -y install telnet
RUN apt-get -y install unixodbc-dev

USER airflow

RUN pip install apache-airflow-providers-odbc
RUN pip install apache-airflow-providers-microsoft-mssql apache-airflow-providers-mongo==3.6.0
RUN pip uninstall redis kombu celery -y
RUN pip install redis kombu celery
RUN pip install pymysql

