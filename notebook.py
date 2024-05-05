<html><head></head><body>#!/usr/bin/env python
# coding: utf-8

# # Analyzing Wikipedia Clickstream Data
# * [View Solution Notebook](./solution.html)
# 
# * [Project Page Link](https://www.codecademy.com/courses/big-data-pyspark/projects/analyzing-wikipedia-pyspark)

# ### Import Libraries

# In[4]:


from pyspark.sql import SparkSession


# ## Task Group 1 - Introduction to Clickstream Data

# ### Task 1
# Create a new `SparkSession` and assign it to a variable named `spark`.

# In[5]:


# Create a new SparkSession
spark = SparkSession.builder.getOrCreate()


# ### Task 2
# 
# Create an RDD from a list of sample clickstream counts and save it as `clickstream_counts_rdd`.

# In[6]:


# Sample clickstream counts
sample_clickstream_counts = [
    [&#34;other-search&#34;, &#34;Hanging_Gardens_of_Babylon&#34;, &#34;external&#34;, 47000],
    [&#34;other-empty&#34;, &#34;Hanging_Gardens_of_Babylon&#34;, &#34;external&#34;, 34600],
    [&#34;Wonders_of_the_World&#34;, &#34;Hanging_Gardens_of_Babylon&#34;, &#34;link&#34;, 14000],
    [&#34;Babylon&#34;, &#34;Hanging_Gardens_of_Babylon&#34;, &#34;link&#34;, 2500]
]

# Create RDD from sample data
clickstream_counts_rdd = spark.sparkContext.parallelize(sample_clickstream_counts)


# ### Task 3
# 
# Using the RDD from the previous step, create a DataFrame named `clickstream_sample_df`

# In[7]:


# Create a DataFrame from the RDD of sample clickstream counts
clickstream_sample_df = clickstream_counts_rdd.toDF([&#39;source_page&#39;,&#39;target_page&#39;,&#39;link_category&#39;,&#39;link_count&#39;])

# Display the DataFrame to the notebook
clickstream_sample_df.show(truncate=False)


# ## Task Group 2 - Inspecting Clickstream Data

# ### Task 4
# 
# Read the files in `./cleaned/clickstream/` into a new Spark DataFrame named `clickstream` and display the first few rows of the DataFrame in the notebook

# In[8]:


# Read the target directory (`./cleaned/clickstream/`) into a DataFrame (`clickstream`)
clickstream = spark.read.option(&#39;delimiter&#39;,&#39;\t&#39;).option(&#39;header&#39;,True).option(&#39;inferSchema&#39;, True).csv(&#34;cleaned/clickstream/&#34;)

# Display the DataFrame to the notebook
clickstream.show(5)


# ### Task 5
# 
# Print the schema of the DataFrame in the notebook.

# In[9]:


# Display the schema of the `clickstream` DataFrame to the notebook
clickstream.printSchema()


# ### Task 6
# 
# Drop the `language_code` column from the DataFrame and display the new schema in the notebook.

# In[10]:


# Drop target columns
clickstream = clickstream.drop(&#34;language_code&#34;)

# Display the first few rows of the DataFrame
clickstream.show(5)
# Display the new schema in the notebook
clickstream.printSchema()


# ### Task 7
# 
# Rename `referrer` and `resource` to `source_page` and `target_page`, respectively,

# In[11]:


# Rename `referrer` and `resource` to `source_page` and `target_page`
clickstream = clickstream.withColumnRenamed(&#34;referrer&#34;,&#34;source_page&#34;).withColumnRenamed(&#34;resource&#34;,&#34;target_page&#34;)
  
# Display the first few rows of the DataFrame
clickstream.show(5)

# Display the new schema in the notebook
clickstream.printSchema()


# ## Task Group 3 - Querying Clickstream Data

# ### Task 8
# 
# Add the `clickstream` DataFrame as a temporary view named `clickstream` to make the data queryable with `sparkSession.sql()`

# In[12]:


# Create a temporary view in the metadata for this `SparkSession` 
clickstream.createOrReplaceTempView(&#34;clickstream&#34;)


# ### Task 9
# 
# Filter the dataset to entries with `Hanging_Gardens_of_Babylon` as the `target_page` and order the result by `click_count` using PySpark DataFrame methods.

# In[13]:


# Filter and sort the DataFrame using PySpark DataFrame methods
clickstream.filter(clickstream.target_page==&#39;Hanging_Gardens_of_Babylon&#39;)\
.orderBy(&#39;click_count&#39;,ascending=False).show(10,truncate=False)


# ### Task 10
# 
# Perform the same analysis as the previous exercise using a SQL query. 

# In[15]:


# Filter and sort the DataFrame using SQL
query=&#34;&#34;&#34;select * from clickstream where target_page=&#39;Hanging_Gardens_of_Babylon&#39; order by click_count DESC&#34;&#34;&#34;
spark.sql(query).show(10,truncate=False)


# ### Task 11
# 
# Calculate the sum of `click_count` grouped by `link_category` using PySpark DataFrame methods.

# In[23]:


# Aggregate the DataFrame using PySpark DataFrame Methods 
clickstream.groupBy(&#39;link_category&#39;).sum(&#39;click_count&#39;).show(5)


# ### Task 12
# 
# Perform the same analysis as the previous exercise using a SQL query.

# In[25]:


# Aggregate the DataFrame using SQL
query = &#34;&#34;&#34;select link_category,sum(click_count) from clickstream group by link_category&#34;&#34;&#34;
spark.sql(query).show(5)


# ## Task Group 4 - Saving Results to Disk

# ### Task 13
# 
# Let&#39;s create a new DataFrame named `internal_clickstream` that only contains article pairs where `link_category` is `link`. Use `filter()` to select rows to a specific condition and `select()` to choose which columns to return from the query.

# In[29]:


# Create a new DataFrame named `internal_clickstream`
internal_clickstream = clickstream.select(&#39;source_page&#39;,&#39;target_page&#39;,&#39;click_count&#39;).filter(clickstream.link_category==&#39;link&#39;)

# Display the first few rows of the DataFrame in the notebook
internal_clickstream.show(5)


# ### Task 14
# 
# Using `DataFrame.write.csv()`, save the `internal_clickstream` DataFrame as CSV files in a directory called `./results/article_to_article_csv/`.

# In[30]:


# Save the `internal_clickstream` DataFrame to a series of CSV files
internal_clickstream.write.csv(&#39;results/article_to_article_csv/&#39;)


# ### Task 15
# 
# Using `DataFrame.write.parquet()`, save the `internal_clickstream` DataFrame as parquet files in a directory called `./results/article_to_article_pq/`.

# In[ ]:


# Save the `internal_clickstream` DataFrame to a series of parquet files


# ### Task 16
# 
# Close the `SparkSession` and underlying `SparkContext`. What happens if you we call `clickstream.show()` after closing the `SparkSession`?

# In[ ]:


# Stop the notebook&#39;s `SparkSession` and `SparkContext`


# In[ ]:


# The SparkSession and sparkContext are stopped; the following line will throw an error:
clickstream.show()

<script type="text/javascript" src="https://www.codecademy.com/assets/relay.js"></script></body></html>