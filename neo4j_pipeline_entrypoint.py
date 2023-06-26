from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from modules.pipelines import Neo4jPipeline, MockNeo4jPipeline
import os

parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-c", "--neo4j-container-name",
                    default=os.environ.get('NEO4J_CONTAINER_NAME', None),
                    help="Neo4j Container name")

parser.add_argument("-b", "--source-bucket",
                    default=os.environ.get('AWS_SOURCE_BUCKET', None),
                    help="AWS S3 bucket path")

parser.add_argument("-l", "--file-limit",
                    default=os.environ.get('FILE_LIMIT', 20) , type=int,
                    help="Maximum amount of files processed")

parser.add_argument("-bs", "--batch-size",
                    default=os.environ.get('BATCH_SIZE', 10), type=int,
                    help="Size of batches when processing")

parser.add_argument("-m", "--mock",
                    default=os.environ.get('MOCK', 'n'), type=str,
                    help="Mock S3 (y/n)")

args = vars(parser.parse_args())
args['mock'] = 'y'
pipeline_class = MockNeo4jPipeline if args['mock'] == 'y' else Neo4jPipeline
pipeline = pipeline_class(args)
print(f"Running {pipeline.__class__.__name__} Pipeline")
pipeline.run()