#
# Copyright (2020) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import os

from pyspark.sql import Row
from pyspark.sql.functions import col, lit, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from delta.tables import DeltaTable
from delta.testing.utils import DeltaTestCase


class DeltaTableTests(DeltaTestCase):

    def test_convertToDelta(self):
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        df.write.format("parquet").save(self.tempFile)
        dt = DeltaTable.convertToDelta(self.spark, "parquet.`%s`" % self.tempFile)
        self.__checkAnswer(
            self.spark.read.format("delta").load(self.tempFile),
            [('a', 1), ('b', 2), ('c', 3)])

        # test if convert to delta with partition columns work
        tempFile2 = self.tempFile + "_2"
        df.write.partitionBy("value").format("parquet").save(tempFile2)
        schema = StructType()
        schema.add("value", IntegerType(), True)
        dt = DeltaTable.convertToDelta(
            self.spark,
            "parquet.`%s`" % tempFile2,
            schema)
        self.__checkAnswer(
            self.spark.read.format("delta").load(tempFile2),
            [('a', 1), ('b', 2), ('c', 3)])

        # convert to delta with partition column provided as a string
        tempFile3 = self.tempFile + "_3"
        df.write.partitionBy("value").format("parquet").save(tempFile3)
        dt = DeltaTable.convertToDelta(
            self.spark,
            "parquet.`%s`" % tempFile3,
            "value int")
        self.__checkAnswer(
            self.spark.read.format("delta").load(tempFile3),
            [('a', 1), ('b', 2), ('c', 3)])

    ### Helper Functions - use these with tests ###
    def __checkAnswer(self, df, expectedAnswer, schema=["key", "value"]):
        if not expectedAnswer:
            self.assertEqual(df.count(), 0)
            return
        expectedDF = self.spark.createDataFrame(expectedAnswer, schema)
        try:
            self.assertEqual(df.count(), expectedDF.count())
            self.assertEqual(len(df.columns), len(expectedDF.columns))
            self.assertEqual([], df.subtract(expectedDF).take(1))
            self.assertEqual([], expectedDF.subtract(df).take(1))
        except AssertionError:
            print("Expected:")
            expectedDF.show()
            print("Found:")
            df.show()
            raise

    def __writeDeltaTable(self, datalist):
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("delta").save(self.tempFile)

    def __writeAsTable(self, datalist, tblName):
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("delta").saveAsTable(tblName)

    def __overwriteDeltaTable(self, datalist):
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("delta").mode("overwrite").save(self.tempFile)

    def __createFile(self, fileName, content):
        with open(os.path.join(self.tempFile, fileName), 'w') as f:
            f.write(content)

    def __checkFileExists(self, fileName):
        return os.path.exists(os.path.join(self.tempFile, fileName))


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
