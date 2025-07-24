# cloud_big_data_management
This repository consists of projects about Cloud and Big Data Management

This repository contains a series of big data assignments that demonstrate my ability to work across a wide range of big data technologies including Hadoop, Apache Hive, Apache Pig, Apache Spark, MongoDB, and HBase. The datasets used span Amazon product reviews (Camera, Electronics) and other large-scale text corpora. The tasks involved data ingestion, transformation, querying, and analysis using distributed processing frameworks.

1. Hadoop MapReduce (HW1)
Cleaned and processed a raw text file using Java-based MapReduce. Removed punctuations and stopwords, handled case insensitivity. Computed word frequencies, top 200 frequent words, and average word length grouped by starting letter. Implemented custom cleanup logic and sorting operations.

2. Apache Pig Assignment
Worked with the Amazon Camera review dataset. Loaded data into Pig and executed multiple aggregation queries: Grouped by marketplace and product category for count and average ratings. Aggregated review statistics by date and product. Filtered verified purchases to analyze vote distributions and ranking by helpfulness ratio.

3. Apache Hive Assignment
Created external Hive tables with partitioning (by star rating) and bucketing (by review date). Wrote complex HiveQL queries to analyze product reviews and vote patterns. Calculated grouped aggregates and top product-level stats using partitioned tables.

4. Apache Spark Assignment
Processed the alice.txt file using PySpark. Removed punctuations and stopwords.
Calculated:
Word frequency (top 200).
Average word length per starting letter.
Top 10 starting letters with highest average word lengths and their total word counts.

5. MongoDB Assignment
Loaded Amazon Electronics review data into MongoDB. Performed complex queries using aggregation, filtering, regex, and projection: Filtered based on rating, summary, and review text. Aggregated review stats by helpfulness and reviewer. Applied disk optimization strategies for large datasets.

6. HBase Assignment
Loaded movie review data into an HBase table with two column families: User and Product. Implemented version control on text column and used regex to extract pattern-based data. Performed analysis on scores, helpfulness, and summary content using scan, filters, and count logic.
