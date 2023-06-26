import apache_beam as beam
from apache_beam import DoFn
import pathlib

def add_suffix(filepath, suffix) -> str:
    """Adds suffix to filename

    Args:
        filepath (_type_):
        suffix (_type_): suffix to be added

    Returns:
        _type_: New filepath with suffix
    """
    path = pathlib.Path(filepath)
    return str(path.with_name(path.stem + suffix + path.suffix))


class ReplaceHash(DoFn):
    """Replaces 2+ instances of has in a given line of text.

    Args:
        DoFn (_type_): Base parent for transformations
    """
    def process(self, element):
        """Entry point for transforming the line of text

        Args:
            line (_type_): Line of text from the text file.

        Yields:
            _type_: Transformed line
        """
        output_value = self.transform(element)
        yield output_value

    def transform(self, line):
        """Finds the hashs and replaces all instances above 1 with %23

        Args:
            line (_type_): Line to be transformed

        Returns:
            _type_: Transformed line with replaced hash
        """
        new_line = ''
        for substring in line.split(' '):
            new_ss = substring
            hash_positions = [
                i for i, ltr in enumerate(substring) if ltr == "#"
            ]
            if len(hash_positions) > 0:
                to_replace = hash_positions[1:]
                for hash_index in to_replace:
                    text_before = new_ss[:hash_index]
                    text_after = new_ss[hash_index + 1:]
                    new_ss = f'{text_before}%23{text_after}'
            new_line += new_ss+' '
        return new_line

class RDFPipeline:
    transform_suffix = "_cleaned"

    def __init__(self, files):
        self.files = files

    def transform(self, file, new_filepath):
        raise NotImplementedError
    
    def run(self, force_reprocess=False):
        """Runs transformation of file and produces new file with added suffix

        Args:
            force_reprocess (bool, optional): Whether to reprocess file 
            or use existing

        Returns:
            _type_: Set of completed File's 
        """
        for file in self.files:
            new_filepath = add_suffix(file.filepath, self.transform_suffix)
            if not pathlib.Path(new_filepath).is_file() or force_reprocess:
                self.transform(file, new_filepath)
            file.filepath = new_filepath
            file.filename = pathlib.Path(new_filepath).name
        return self.files
    
class Neo4JRDFPipeline(RDFPipeline):
    """File processer for Neo4j, Pipeline will parse RDF file to ensure
    it's compatible with the neosematic's importer

    Args:
        RDFPipeline (_type_): Parent Pipeline

    Returns:
        _type_: New filepath of transformed data.
    """
    transform_suffix = "_neo4jcleaned"

    def transform(self, file, new_filepath):
        """Runs through files and makes necessary trasformations required
        by Neo4j.

        Args:
            file (_type_): File to be transformed
            new_filepath (_type_): New Filepath of file.

        Returns:
            new_filepath _type_: New filepath name.
        """
        with beam.Pipeline() as p:
            # Read input from a source (e.g., a file or database)
            input_data = p | beam.io.ReadFromText(file.filepath)

            # Apply your custom DoFn
            transformed_data = input_data | beam.ParDo(ReplaceHash())

            # Write the transformed data to an output sink (e.g., a file or database)
            transformed_data | beam.io.WriteToText(
                new_filepath, shard_name_template=''
            )
        return new_filepath
