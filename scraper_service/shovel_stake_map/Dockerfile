FROM python:3.12-slim

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./shared /app/shared
COPY ./shovel_stake_map /app/shovel_stake_map

ENV PYTHONPATH="/app:/app/shared"

CMD ["python", "-u", "shovel_stake_map/main.py"]
