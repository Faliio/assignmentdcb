FROM python:3.9

COPY ./requirements.txt .

RUN mkdir /home/python-app \
	&& pip install -r requirements.txt -v

COPY ./src/ ./python-app

ENTRYPOINT [ "python", "./python-app/dataset_to_sql.py" ]