FROM python:3.6
WORKDIR /opt
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY ./excel.py /opt/
COPY ./events.yaml /opt/
COPY ./event_log.xlsx /opt/
CMD ["python", "/opt/excel.py"]
