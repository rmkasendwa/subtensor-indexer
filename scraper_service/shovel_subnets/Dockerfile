FROM python:3.12-slim

WORKDIR /app

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"


COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Set up Rust bindings
RUN apt-get -qq update
RUN apt-get install -y -q \
  build-essential \
  curl
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
COPY ./shovel_subnets/rust_bindings /app/rust_bindings
WORKDIR /app/rust_bindings
RUN pip install patchelf
RUN maturin build --release
RUN pip install /app/rust_bindings/target/wheels/*.whl

WORKDIR /app
COPY ./shared /app/shared
COPY ./shovel_subnets /app/shovel_subnets

ENV PYTHONPATH="/app:/app/shared"

CMD ["python", "-u", "shovel_subnets/main.py"]
