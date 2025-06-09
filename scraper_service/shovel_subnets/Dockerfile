FROM python:3.12-slim

WORKDIR /app

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"


COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN pip install patchelf

# Install Rust
RUN apt-get -qq update
RUN apt-get install -y -q \
  build-essential \
  curl
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Build and cache Rust deps
COPY ./shovel_subnets/rust_bindings/Cargo.toml /app/rust_bindings/Cargo.toml
COPY ./shovel_subnets/rust_bindings/Cargo.lock /app/rust_bindings/Cargo.lock
COPY ./shovel_subnets/rust_bindings/pyproject.toml /app/rust_bindings/pyproject.toml
# WORKDIR /app/rust_bindings
# RUN mkdir src && echo "fn main() {}" > src/lib.rs
# RUN cargo fetch
# RUN maturin build --release
# RUN rm src/lib.rs
# RUN rm /app/rust_bindings/target/wheels/*

# Copy the rest of the Rust bindings and build
WORKDIR /app
COPY ./shovel_subnets/rust_bindings /app/rust_bindings
WORKDIR /app/rust_bindings
RUN maturin build --release
RUN pip install /app/rust_bindings/target/wheels/*.whl

WORKDIR /app
COPY ./shared /app/shared
COPY ./shovel_subnets /app/shovel_subnets

ENV PYTHONPATH="/app:/app/shared"

CMD ["python", "-u", "shovel_subnets/main.py"]
