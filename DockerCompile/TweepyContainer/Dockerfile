FROM python:3.6-slim

WORKDIR /tweets

COPY . /tweets

RUN pip install --trusted-host pypi.python.org -r requirements.txt

CMD ["python", "get_tweets.py"]
