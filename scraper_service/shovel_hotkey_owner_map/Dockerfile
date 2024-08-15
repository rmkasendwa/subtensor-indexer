FROM python:3.12-slim

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./shared /app/shared
COPY ./shovel_hotkey_owner_map /app/shovel_hotkey_owner_map

ENV PYTHONPATH="/app:/app/shared"

CMD ["python", "-u", "shovel_hotkey_owner_map/main.py"]
