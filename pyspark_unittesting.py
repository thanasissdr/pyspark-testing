#!/usr/bin/env python
# coding: utf-8

# In[14]:


from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import FloatType, IntegerType, BooleanType

import pyspark.sql.functions as f

import pandas as pd
import numpy as np

import string

import unittest
import logging
import pytest

import sys


# In[41]:


spark = SparkSession.builder.getOrCreate()
spark.sparkContext._conf.getAll()


# In[3]:


conf = [('spark.app.name', "Spark AppName Updated")]
conf = spark.sparkContext._conf.setAll(conf)
spark.sparkContext.stop()
spark = spark.builder.config(conf=conf).getOrCreate()
spark


# In[8]:


class PySparkTest(unittest.TestCase):
    
    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
     .master('local[2]')
     .appName('my-local-testing-pyspark-context')
     .enableHiveSupport()
     .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()
        
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


# In[35]:


class SimpleTest(PySparkTest):
    
    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')
        
        
    def test_lower(self):
        self.assertEqual("FOO".lower(), 'foo')


# In[39]:


class AdvancedTest(PySparkTest):
    
    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')
        
        
    def test_lower(self):
        self.assertEqual("FOO".lower(), 'foo')


# In[40]:


if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)


# In[ ]:




