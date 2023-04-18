FROM python:3

WORKDIR /src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .  

CMD [ "python", "./archive.py" ]