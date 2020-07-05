FROM python:3

RUN pip install twython pymongo


ADD twitter-app /opt/twitter-app
WORKDIR /opt/twitter-app

EXPOSE 5000

CMD [ "python", "./Twitter_mongodb.py" ]
