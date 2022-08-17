import unittest
import pathlib
import os
import pyspark

from win_spark import WinSpark
from pyspark.sql import SparkSession


class TestWinSpark(unittest.TestCase):

    def test_winspark(self):
        WinSpark.load_spark_winutils()
        WinSpark.set_pyspark_env_vars()
        #WinSpark.set_JDK4py()

        py_path = str(pathlib.Path(__file__).parent.resolve())

        
        input_csv = os.path.join(py_path, 'tests\\input\\avengers.csv')
        output_csv = os.path.join(py_path, 'tests\\output\\avengers.csv')
        
        print(f'Pypath: {py_path}')
        print(f'Input File: {input_csv}')
        print(f'Output File: {output_csv}')

        spark = SparkSession \
            .builder \
            .getOrCreate()

        df = spark.read \
            .format('csv') \
            .option("inferSchema" , "true") \
            .option('header', 'true') \
            .option('sep',',') \
            .load(input_csv)

        df.write \
            .format('parquet') \
            .mode("overwrite") \
            .save(output_csv)

        df_p = spark.read \
            .format('parquet') \
            .load(output_csv)

        self.assertEqual(df_p.count(), 175)

if __name__ == '__main__':
    unittest.main()