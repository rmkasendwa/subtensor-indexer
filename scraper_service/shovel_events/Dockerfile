FROM python:3.12-slim

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./shared /app/shared
COPY ./shovel_events /app/shovel_events

ENV PYTHONPATH="/app:/app/shared"

CMD ["python", "-u", "shovel_events/main.py"]
