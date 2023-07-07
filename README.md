# workflowusingcrawler_job
The workflow trigger starts the crawler, which crawls the CSV file IMDB-Movie-Data.csv which is present in the S3 bucket 04072023 and creates the schema in the data catalog.
once this crawler is completed successfully a trigger added in the workflow will start the glue job. This glue job will do the transformations for the cvs file and put the out put in the output folder in the parquet file format in S3 bucket 04072023.
