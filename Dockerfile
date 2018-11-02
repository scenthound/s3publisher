FROM centos/python-36-centos7

WORKDIR /app
COPY . /app

RUN python -m pip install -r requirements.txt

ENTRYPOINT ["python", "sns-publisher.py"]
