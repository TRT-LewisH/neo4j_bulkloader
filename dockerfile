FROM python:3.11.3
LABEL Maintainer="LewisHepburn"

WORKDIR /usr/neo4j_loader/src
COPY . .
RUN pip install -r requirements.txt
RUN apt-get update
RUN apt-get install -y libsasl2-dev
RUN curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip"
RUN unzip awscli-bundle.zip
RUN ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws
RUN aws configure set region "us-east-1" --profile localstack
RUN aws configure set aws_access_key_id test  --profile localstack
RUN aws configure set aws_secret_access_key test --profile localstack

#CMD ["sleep", "1d"]
CMD [ \
 "python", "./neo4j_pipeline_entrypoint.py"]