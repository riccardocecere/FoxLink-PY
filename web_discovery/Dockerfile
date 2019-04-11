FROM python:latest
ADD . /code
WORKDIR /code
RUN pip install -r requirements.txt
#COPY wait-for-it.sh wait-for-it.sh
#RUN chmod +x wait-for-it.sh
CMD ["python", "main.py"]