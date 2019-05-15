FROM python:latest
ADD . /code
WORKDIR /code
ADD ./requirements.txt ./
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
CMD ["python", "main.py"]