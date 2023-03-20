import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import json


#inherit from beam.Fn
class FilterTransactions(beam.DoFn):
    #Filter data based on the criteria 
    def process(self, element):
        try:
            timestamp, origin, destination, transaction_amount = element.split(',')
            year = int(timestamp[:4])
            transaction_amount = float(transaction_amount)
            
            if transaction_amount > 20 and year >= 2010:
                yield timestamp.split(' ')[0], transaction_amount
        except ValueError:
            # Skip lines that do not match the expected format
            pass


#Composite transforms -- inherit from beam.PTransform
class TransactionsTransform(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | "Filter Transactions" >> beam.ParDo(FilterTransactions())
            | "Group By Date" >> beam.GroupByKey()
            | "Sum Transaction Amounts" >> beam.Map(lambda x: (x[0], sum(x[1])))
        )


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        default="gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv",
        help="Input file to process.",
    )
    parser.add_argument(
        "--output",
        dest="output",
        default="output/results.jsonl.gz",
        help="Output file to write results to.",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read Transactions" >> ReadFromText(known_args.input, skip_header_lines=1)
            | "Apply Transformations" >> TransactionsTransform()
            | "Format Output" >> beam.Map(lambda x: json.dumps({"date": x[0], "total_amount": x[1]}))
            | "Write Results" >> WriteToText(known_args.output, num_shards=1)
        )


if __name__ == "__main__":
    run()
