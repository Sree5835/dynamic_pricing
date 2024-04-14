FROM python:3.10

WORKDIR /home

COPY ./src /home/src

COPY ./requirements.txt /home/requirements.txt

RUN pip install --no-cache-dir -r /home/requirements.txt

CMD ["python", "src/dynamic_pricing/webhook/app.py","--host","0.0.0.0", "--port","80"]

