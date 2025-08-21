FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline.py disinfo_labeler.py ./
ENTRYPOINT ["python","pipeline.py"]
