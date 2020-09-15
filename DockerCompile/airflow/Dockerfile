FROM puckel/docker-airflow

WORKDIR /etlJob

COPY . /etlJob

RUN pip install --trusted-host pypi.python.org -r requirements.txt
