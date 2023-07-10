# workflowusingcrawler_job
The workflow trigger starts the crawler, which crawls the CSV file IMDB-Movie-Data.csv which is present in the S3 bucket 04072023 and creates the schema in the data catalog.
once this crawler is completed successfully a trigger added in the workflow will start the glue job. This glue job will do the transformations for the cvs file like droping the fields and data types, once this is done we do filter of record where the movies revenue is more than 300 million and put the output in the output folder in the parquet file format in S3 bucket 04072023.
