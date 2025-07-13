import sys
from pyspark.sql.types import *

from soda.scan import Scan
from soda.sampler.sampler import Sampler
from soda.sampler.sample_context import SampleContext

class SodaToUnityCatalogSchemaConverter:

    @classmethod
    def convert(cls, soda_schema):
        return StructType([
            StructField(
                column.name, 
                cls.instantiate_class(column.type), 
                True # Since SODA schema doesn't have metadata about nullability (as of 2025-07), assume column nullable to True
            ) for column in soda_schema.columns
        ])

    @classmethod
    def instantiate_class(cls, class_name):
        # Some python magic that turns a class name (as string object) into an instance of the class
        return getattr(sys.modules[__name__], class_name)()


class UnityCatalogSampler(Sampler):

    def __init__(self, spark, failed_table_path: str):
        super().__init__()
        self.spark = spark
        self.target_table_path = failed_table_path
    
    def store_sample(self, sample_context: SampleContext):
        """
        This method is called by the scan to store the sample.
        """
        # TODO: Add metadata about scan time: when did the scan happen?
        # TODO: Add metadata about failure reason: what checks failed?
        sampleDF = self.spark.createDataFrame(
            sample_context.sample.get_rows(), 
            schema=SodaToUnityCatalogSchemaConverter.convert(sample_context.sample.get_schema())
        )
        # TODO: How can we partition?
        sampleDF.write.format("delta").mode("append").saveAsTable(self.target_table_path)