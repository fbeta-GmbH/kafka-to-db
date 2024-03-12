FROM python:3.10
RUN apt-get update && apt-get install -y python3-pip python3 build-essential libpq-dev

WORKDIR /app
COPY ./python/requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT [ "python", "app.py" ]