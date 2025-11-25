FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libxkbcommon0 \
    libxcomposite1 libxdamage1 libxrandr2 libgbm1 libasound2 libpangocairo-1.0-0 \
    libpango-1.0-0 libcairo2 libatspi2.0-0 wget xvfb \
    libpq-dev gcc g++ \
    curl autoconf automake libtool pkg-config git make \
 && git clone https://github.com/openvenues/libpostal /tmp/libpostal \
 && cd /tmp/libpostal \
 && ./bootstrap.sh \
 && ./configure --datadir=/usr/share/libpostal \
 && make -j"$(nproc)" \
 && make install \
 && ldconfig \
 && rm -rf /tmp/libpostal \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install --with-deps chromium

COPY . .

CMD ["python", "rew_detail_scraper.py"]
