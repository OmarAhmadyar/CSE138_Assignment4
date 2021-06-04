FROM python:3
EXPOSE 8085

COPY requirements.txt .
COPY src .
RUN pip3 install --no-cache-dir -r requirements.txt
CMD ["python3", "./main.py"]
