import pandas as pd
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
import warnings
import re
import logging
import math
import functools
import numpy as np
import os
import delta
import subprocess
import datetime
import json


def get_percentile(column, percentile, alias):
    """
    Args:
        column (str) : column to calc percentiles on 
        percentile (float) : fraction for the percentile, e.g. .01 for 1%
        alias (str) : alias to rename the column in spark dataframe
        
    Returns:
        `spark.sql.functions.expr`
    """
    return  F.expr(f'percentile_approx({column}, {percentile})').alias(alias)


def compute_summary(df:pyspark.sql.dataframe.DataFrame, valuecol:str='amtallowed', groupbycol=None, percentiles=[]):
    """Compute summary statistics.
    
    Computes following columns by default for each category in groupbycol:
      
        - default percentiles: [.01,  .25, .50, .75, .99]   
        - mean
        - num_zeros
        - percent_zeros
        - stddev
        - sum
    
    Args:
        df (`pyspark.sql.dataframe.DataFrame`) 
        valuecol (str) : A numeric column to compute statistics on.
        groupbycol (str) : A categorical column to group the statistics on.
        percentiles (list) : list of percentiles in addition to default percentiles
        
    Returns:
        `pyspark.sql.dataframe.DataFrame`
    """
    assert groupbycol is not None, 'groupby should be a column name (str) in dataframe, not NoneType'
    assert isinstance(groupbycol, str), 'groupbycol should be a single column name of type str'
    
    default_percentiles = [.01,  .25, .50, .75, .99]     
    percentiles = sorted(list(set(default_percentiles + percentiles)))
    
    # percentile_cols = [f'p{str(p).split(".")[-1]}' for p in percentiles]
    # formatted version - keeps exactly 2 decimal places, above version drops trailing zeros
    def make_alias(p):
        """Returns formatted column name
        
        Example:
            >>>make_alias(.2)
            'p20'
            >>>make_alias(.20)
            'p20'
            >>>make_alias(.201)
            'p20'
        """
        return 'p' + f'{p:.2f}'[-2:]
    
    percentile_cols = [make_alias(p)  for p in percentiles]

    dfagg = (
      df
      .filter(F.col(valuecol) >= 0)
      .withColumn('is_zero', F.when(F.col(valuecol) <= 1e-2, 1).otherwise(0))
      .groupby(groupbycol)
      .agg(F.sum(valuecol).alias('sum'), 
          F.count(groupbycol).alias('count'), 
          *[get_percentile(valuecol, p, make_alias(p)) for p in percentiles], # expand list of exprs
          F.mean(valuecol).alias('mean'),
          F.stddev(valuecol).alias('stddev'),
          F.sum('is_zero').alias('num_zeros')
         )
      .withColumn('percent_zeros', F.col('num_zeros') / F.col('count') * 100)
     .select( groupbycol, 
             'sum', 'count', 'mean', 
             *percentile_cols,   # expand list of columns
             'stddev', 'num_zeros', 'percent_zeros') #'providerzip', 
    ) 
    return dfagg
