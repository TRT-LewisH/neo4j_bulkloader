import subprocess
from .storage import *
import abc
from .transform import Neo4JRDFPipeline
import csv

    
class RDFLoader(abc.ABC):
    """Abstract class used originally for both Neo4j and Virtuoso sub
    classes.

    Args:
        abc (_type_): Abstract base class
    """
    def __init__(self, container_name: str, *args, **kwargs):
        self.container_name = container_name
        self.failed_files = []

    def write_failed_files_to_csv(self, filepath: str):
        """Writes failed files to csv when the database is unable to load file.

        Args:
            filepath (str): full filepath of the file.
        """
        for file in self.failed_files:
            with open(filepath, "a", newline="") as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:  # Check if file is empty
                    writer.writerow(["file"])
                writer.writerow([file])



class Neo4jRDFLoader(RDFLoader):
    CONTAINER_TEMP_STORAGE = "datasink"

    def __init__(self, session, *args, **kwargs):
        self.session = session
        super().__init__(*args, **kwargs)
        try:
            self.session.execute_write(self._setup_constraint)
        except:
            pass
        try:
            self.session.execute_write(self._setup_config)
        except:
            pass

    @staticmethod
    def _load_file(tx, self, file):
        """The UOW is forcing a static method. As seen here:

        https://neo4j.com/docs/api/python-driver/current/

        Therefore, I've passed  self as argument so I can modify the
        attribute in the method.
        """
        try:
            result = tx.run(
                "CALL n10s.rdf.import.fetch( 'file:///{}', 'TriG' )".format(file)
            )
            resulted_cypher = result.single()
            if resulted_cypher[0] == "KO":
                raise Exception("CypherError: {}".format(resulted_cypher[4]))
        except Exception as error:
            print(error)
            self.failed_files.append(file)

    @staticmethod
    def _setup_constraint(tx):
        """Creates contraint on graph to have uri as unique

        Args:
            tx (_type_): transaction for Neo4j
        """
        try:
            tx.run(
                "CREATE CONSTRAINT n10s_unique_uri FOR (r:Resource) \
                REQUIRE r.uri IS UNIQUE"
            )
        except Exception as Error:
            print("Database constraint already exists.")

    @staticmethod
    def _setup_config(tx):
        """Sets the graph config for the data import 

        Args:
            tx (_type_): transaction for Neo4j
        """
        try:
            tx.run(
                'CALL n10s.graphconfig.init({})'
            )
        except Exception as error:
            print("Database config already initialised.")


    def load(self, batch: Batch):
        """Loads file into Neo4j using session transaction.

        Args:
            batch (Batch): Group of files with specified length.
        """
        for file in batch:
            print(f"Loading: {file.filename}")
            self.session.execute_write(
                 self._load_file, self,
                 self.CONTAINER_TEMP_STORAGE + "//" + file.filename,
            )
       
    def transform(self):
        """Runs a transform script to clean and format data already in
        the graph

        Args:
            tx (_type_): transaction for Neo4j
        """
        cypher_file = open('modules/cypher_transform.cql', 'r')
        for query in cypher_file.read().split(";"):
            if query.strip():
                try:
                    self.session.run(query)
                except Exception as error:
                    print("Transform failed.")
        cypher_file.close()
