FROM python:3.7

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DATAROBOT_ENDPOINT=""
ENV DATAROBOT_API_TOKEN=""
ENV DATAROBOT_PRED_ENDPOINT=""
ENV DATAROBOT_USERNAME=""
ENV DEPLOYMENT_ID=""
ENV DATAROBOT_KEY=""

CMD [ "python", "./main.py" ]
