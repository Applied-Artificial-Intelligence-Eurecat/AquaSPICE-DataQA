FROM python:slim
WORKDIR /usr/app

COPY ./requirements.txt ./requirements.txt
RUN python -m pip install -r requirements.txt

COPY ./src/ /usr/app/src/
COPY ./config/ /usr/app/config/

CMD [ "python", "-u", "./src/streaming_analysis.py", "./config/base_config.json;./config/data_qa_params.json;./config/data_qa_config.json"]
