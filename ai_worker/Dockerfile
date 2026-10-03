FROM python:3.12-slim
# UID/GID 10001 deliberately matches cpp/Dockerfile's `taskforge` user, not a
# typo: this container shares the `taskforge_data` volume with the scheduler.
# Docker populates a fresh named volume's ownership from whichever container's
# image mounts it first, so if the two images used different UIDs here, whichever
# service started second would get a permission-denied error the first time it
# tried to create its own subdirectory (this one needs /data/model) under a
# volume root chown'd by the other's UID.
RUN apt-get update && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 10001 taskforge && useradd -m -u 10001 -g 10001 taskforge
WORKDIR /app
COPY ai_worker/requirements.txt .
# The default PyPI `torch` wheel for Linux bundles ~3GB of NVIDIA/CUDA shared
# libraries this demo model never uses (it's CPU-only, a few hundred KB of weights),
# which makes both the image and cold-start import time (measured ~16s in testing)
# far larger than necessary. The CPU-only wheel is a small fraction of the size and
# imports much faster; fall back to the default index if that one is unreachable.
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu -r requirements.txt \
    || pip install --no-cache-dir -r requirements.txt
COPY ai_worker/*.py ./
RUN mkdir -p /data/model && chown -R taskforge:taskforge /data
USER taskforge
CMD ["python", "main.py"]
