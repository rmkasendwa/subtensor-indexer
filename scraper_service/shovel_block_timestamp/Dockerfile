FROM python:3.12-slim

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./shared /app/shared
COPY ./shovel_block_timestamp /app/shovel_block_timestamp

ENV PYTHONPATH="/app:/app/shared"

CMD ["python", "-u", "shovel_block_timestamp/main.py"]
