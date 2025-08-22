FROM python:3.10-slim
RUN pip install -r requirements.txt --no-cache-dir
WORKDIR /app
COPY gdelt /app/gdelt
# every 15 min
RUN printf '*/15 * * * * /usr/local/bin/python /app/gdelt >>/proc/1/fd/1 2>>/proc/1/fd/2\n' > /etc/crontab.txt
CMD ["supercronic", "/etc/crontab.txt"]