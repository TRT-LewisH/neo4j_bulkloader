import os
import subprocess
import abc
import uuid
import pathlib
import boto3
from tqdm import tqdm

class File:
    """This is a class representation of a file.

    Attributes:
        filepath: directory and the filename together
        filepath: name of the file inc extension
    """

    __slots__ = "filename", "filepath", "directory"

    def __init__(self, directory, filename, *args, **kwargs):
        self.directory = directory
        self.filepath = f"{directory}//{filename}"
        self.filename = filename

    def __str__(self):
        return self.filename

    def remove_file_to_container(self, target):
        """Removes file from docker container."""
        try:
            os.remove(target + "/" + self.filename)
        except:
            print("Could not remove: {}".format(
                    target + "/" + self.filename
                )
            )


class Batch:
    """ Aggregate for files use for spliting up FileStorage into chunks"""

    def __init__(self, files: list):
        self.files = files

    def __iter__(self):
        for file in self.files:
            yield file

    def copy_to_container(self, target: str, container_name: str):
        for file in self.files:
            file.copy_file_to_container(container_name, target)

    def remove_from_container(self, target, container_name):
        for file in self.files:
            file.remove_file_to_container(target)

    def add(self, file):
        self.files.append(file)


class LocalFileStorage:
    def __init__(self, directory=None, limit=None, *args, **kwargs):
        self.files = []
        self.directory = directory
        self.limit = limit
        if self.directory:
            self._build_files()

    def _build_files(self):
        i = 0
        for file in os.listdir(self.directory):
            if "cleaned" not in file and file[-5:] == ".trig":
                self.files.append(File(self.directory, file))
                i += 1
                if i == self.limit:
                    break

    def __iter__(self):
        for file in self.files:
            yield file

    def __str__(self):
        return f"Folder: {self.directory}"

    def to_batches(self, batch_size=1):
        batched_files = []
        batch = []
        for file in self:
            if len(batch) == batch_size:
                batched_files.append(Batch(batch))
                batch = []
            batch.append(file)
        if len(batch) > 0:
            batched_files.append(Batch(batch))
        return batched_files

    @staticmethod
    def create_file(directory, filename):
        filepath = directory + "//" + filename
        if not pathlib.Path(filepath).is_file():
            f = open(filepath, "a")
            f.write(str(uuid.uuid4()))
            f.close()
        file = File(directory, filename)
        return file


class AWSStorageBucketWrapper:
    """Abstration layer for the S3 storage used. Whether mock or aws."""
    
    def __init__(self, bucket_name, endpoint_url=None, session=None):
        s3 = session.resource("s3", endpoint_url=endpoint_url)
        self.bucket = s3.Bucket(bucket_name)
        self.processed_files = []

    def pull(self, save_to=".", limit=100):
        """Pulls from s3 bucket and applys limit to number pulled.

        Args:
            save_to (str, optional): Temporary save location
            limit (int, optional): file download limit.
        """
        files = tqdm(self.bucket.objects.limit(limit))
        for file in files:
            files.set_description("Pulling from AWS %s" % file)
            self.processed_files.append(file)
            self.bucket.download_file(file.key, os.path.join(save_to, file.key))

    def clear_bucket(self):
        self.bucket.objects.all().delete()

