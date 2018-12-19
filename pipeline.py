import apache_beam
from apache_beam.io import filebasedsource
from beam_utils.sources import CsvFileSource
import argparse
import re

BIGQUERY_TABLE_FORMAT = re.compile(r'[\w.:-]+:\w+\.\w+$')

def discard_dubious(record):
    """Discards dubious records."""
    if record['clean_test'] == 'dubious':
        raise StopIteration()
    yield record

def filter_na(record):
    """Filters records without budget info."""
    if record['domgross'] == '#N/A':
        raise StopIteration()
    yield record

def massage_rec(record):
    """Massage columns."""
    # Redundant columns
    del record['test']
    del record['code']
    del record['budget']
    del record['domgross']
    del record['intgross']
    del record['binary']
    del record['period code']
    del record['decade code']

    # Remove the 'clean' prefix
    record['test'] = record['clean_test']
    del record['clean_test'] 

    record['budget'] = record['budget_2013$']
    if record['budget'] == '#N/A':
        del record['budget']
    del record['budget_2013$'] 
    record['domgross'] = record['domgross_2013$']
    if record['domgross'] == '#N/A':
        del record['domgross']
    del record['domgross_2013$'] 
    record['intgross'] = record['intgross_2013$']
    if record['intgross'] == '#N/A':
        del record['intgross']
    del record['intgross_2013$'] 

    return record

def convert_types(record):
    """Converts string values to their appropriate type."""

    record['year'] = int(record['year']) if 'year' in record else None
    record['budget'] = int(record['budget']) if 'budget' in record else None
    record['domgross'] = int(record['domgross']) if 'domgross' in record else None
    record['intgross'] = int(record['intgross']) if 'intgross' in record else None

    return record

def main(src_path, dest_table, pipeline_args):
    p = apache_beam.Pipeline(argv=pipeline_args)

    value = p | 'Read CSV' >> apache_beam.io.Read(CsvFileSource(src_path))

    value |= (
        'Remove dubious records' >>
        apache_beam.FlatMap(discard_dubious))

    value |= (
        'Filter N/A data' >>
        apache_beam.FlatMap(filter_na))

    value |= (
        'Massage columns' >>
        apache_beam.Map(massage_rec))

    value |= (
        'Convert string values to their types' >>
        apache_beam.Map(convert_types))

    value |= (
        'Dump data to BigQuery' >>
        apache_beam.io.Write(apache_beam.io.BigQuerySink(
            dest_table,
            schema=', '.join([
                'year:INTEGER',
                'imdb:STRING',
                'title:STRING',
                'test:STRING',
                'budget:INTEGER',
                'domgross:INTEGER',
                'intgross:INTEGER',
            ]),
            create_disposition=(
                apache_beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
            write_disposition=(
                apache_beam.io.BigQueryDisposition.WRITE_TRUNCATE))))

    p.run().wait_until_finish()

def bq_table(bigquery_table):
    if not BIGQUERY_TABLE_FORMAT.match(bigquery_table):
        raise argparse.ArgumentTypeError(
            'bq_table must be of format PROJECT:DATASET.TABLE')
    return bigquery_table


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('src_path', help=(
        'The csv file with the data. This can be either a local file, or a '
        'Google Cloud Storage uri of the form gs://bucket/object.'))
    parser.add_argument(
        'dest_table', type=bq_table, help=(
            'A BigQuery table, of the form project-id:dataset.table'))

    args, pipeline_args = parser.parse_known_args()

    main(args.src_path, args.dest_table, pipeline_args=pipeline_args)