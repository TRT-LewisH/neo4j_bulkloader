from modules.loaders import Neo4jRDFLoader
from modules.transform import Neo4JRDFPipeline
from modules.storage import AWSStorageBucketWrapper, LocalFileStorage
from botocore.exceptions import ClientError
from neo4j import GraphDatabase
import boto3
import os
import logging
import copy
import multiprocessing

def create_bucket(client, bucket_name):
    """
    Creates a S3 bucket.
    """
    try:
        response = client.create_bucket(
            Bucket=bucket_name)
    except ClientError:
        logger.exception('Could not create S3 bucket locally.')
        raise
    else:
        return response

class Neo4jPipeline():
    """ 
    Neo4j Pipeline class, acts as a main for processing files from s3 bucket
    into desired Neo4j instance.
    """
    TEMP_STORAGE = '/datasink'

    def __init__(self, input_args, *args, **kwargs):
        self.input_args = input_args
        self.limit = self.input_args.pop('file_limit', None)
        self.source_aws_bucket =  self.input_args.pop('source_bucket', None)
        self.batch_size = self.input_args.pop('batch_size', None)
        self.container_name =self.input_args.pop('neo4j-container-name', None)
        print(input_args)
        
    def _extract(self):
        """Extracts files from S3 buckets for ingestion."""
        aws_bucket_wrapper= AWSStorageBucketWrapper(self.source_aws_bucket)
        aws_bucket_wrapper.pull(save_to=self.TEMP_STORAGE, limit=self.limit)

    def _process(self, loader, batch):
        original_batch = copy.deepcopy(batch)
        # Pre-transform data for Neo4j ingest
        pipeline = Neo4JRDFPipeline(batch)
        batch = pipeline.run()
        loader.load(batch)

        # Remove cleaned and original from container
        for f in [batch, original_batch]:
            f.remove_from_container(
                self.TEMP_STORAGE, self.container_name
            )
            
    def run(self):
        """ 
        Runs the full e2e pipeline. Pulls from S3 bucket, splits into 
        batches, transforms the batch files and sending to Neo4j.
        """
        self._extract()
        local_storage = LocalFileStorage(self.TEMP_STORAGE, limit=self.limit)
        driver = GraphDatabase.driver("neo4j://neo4j:7687",
                                    auth=("neo4j", "neo4j123"))
        pool = multiprocessing.Pool() 
        with driver.session(database="neo4j") as session:
            loader = Neo4jRDFLoader(session, self.container_name)
            for batch in local_storage.to_batches(batch_size=self.batch_size):
                pool.apply_sync(self.process, args=(self, loader, batch))
            loader.transform()
        loader.write_failed_files_to_csv('results.csv')
        driver.close()

class MockNeo4jPipeline(Neo4jPipeline):
    #TEMP_STORAGE = '.' # Use for local dev
    TEMP_STORAGE = '/datasink'

    def __init__(self, input_args, *args, **kwargs): # Use for local dev
        super().__init__(input_args, *args, **kwargs)
        self.source_aws_bucket =  'tests3'
        self.container_name = 'neo4j_neo4j_1'

    def _extract(self):
        AWS_REGION = 'us-east-1'
        AWS_PROFILE = 'localstack'
        ENDPOINT_URL = 'http://localstack:4566'
        #ENDPOINT_URL = 'http://localhost:4566'
        #ENDPOINT_URL = 'http://172.25.0.1:4566' # Use for local dev

        SESSION = boto3.session.Session(profile_name=AWS_PROFILE,
                     aws_access_key_id="foobar",
                    aws_secret_access_key="foobar"
        )
        s3_client = SESSION.client("s3", region_name=AWS_REGION, 
                    endpoint_url=ENDPOINT_URL, aws_access_key_id="foobar",
                    aws_secret_access_key="foobar"
                    )

        aws_bucket_wrapper= AWSStorageBucketWrapper(
            self.source_aws_bucket, endpoint_url=ENDPOINT_URL, session=SESSION
        )
        try:
            create_bucket(s3_client, 'tests3')
        except:pass
        try:
            i = 0
            for file in os.listdir('/toLoad'):
            #for file in os.listdir('/mnt/c/Users/LewisHepburn/Testfiles/Good Files'): # Use for local dev
                if "cleaned" not in file and file[-5:] == ".trig":
                    response = s3_client.upload_file(os.path.join('/toLoad', file), 'tests3', file)
                    #response = s3_client.upload_file(os.path.join('/mnt/c/Users/LewisHepburn/Testfiles/Good Files', file), 'tests3', file) # Use for local dev
                    i += 1
                    if i == self.limit:
                        break
        except ClientError as e:
            logging.error(e)
        aws_bucket_wrapper.pull(save_to=self.TEMP_STORAGE, limit=self.limit)
        aws_bucket_wrapper.clear_bucket()