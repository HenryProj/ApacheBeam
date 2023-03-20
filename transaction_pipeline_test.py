import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from transaction_pipeline import TransactionsTransform


class TransactionAnalysisTest(unittest.TestCase):
    def test_transactions_transform(self):
        with TestPipeline() as p:
            test_data = [
                "2011-01-01 00:00:00 UTC\twallet1\twallet2\t25.00",
                "2009-12-31 23:59:59 UTC\twallet1\twallet2\t30.00",
                "2011-01-01 00:00:00 UTC\twallet1\twallet2\t40.00",
                "2011-01-01 00:00:00 UTC\twallet1\twallet2\t10.00",
            ]
            
            expected_output = [
                ("2011-01-01", 65.00),
            ]

            input_data = p | "Create Test Data" >> beam.Create(test_data)
            transformed_data = input_data | "Apply Test Transform" >> TransactionsTransform()
            assert_that(transformed_data, equal_to(expected_output))


if __name__ == "__main__":
    unittest.main()
