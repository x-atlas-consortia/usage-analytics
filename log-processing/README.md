# HuBMAP Log Processing scripts

The scripts in Log Processing support extracting data from various sources for long-term storage in AWS S3, and using that data in S3 to create ElasticSearch documents for analysis. Data formats in S3 and ES are distinctive to the source, as is the refresh schedule of data.

See each of the repository directories  for specific information such as a README, flow diagram, and code.
- ```file-downloads-to-S3```
- ```api-usage-to-S3```
- ```S3-log-data-to-ES```

## File Download data stored in S3

File download is enabled through Globus.  The ```file-downloads-to-S3``` scripts read log files provided by Globus to extract information about sessions which had file transfers. These scripts run on the PSC Hive servers as of Winter 2025, and create AWS S3 Objects in an S3 Bucket folder dedicated to file transfers.

## API Usage data stored in S3

HuBMAP's ```entity-api```, ```search-api```, and ```uuid-api``` log using AWS Cloudwatch.  The ```api-usage-to-S3``` scripts read Cloudwatch Log Groups to extract information about endpoint usage.  These scripts run on HuBMAP AWS servers, and create AWS S3 Objects in an S3 Bucket folder dedicated to API usage.

## ElasticSearch Indexing of S3 Data

To make data stored in AWS S3 for API usage and file transfers available for analysis, the contents of S3 Objects previously described is used to populate indices in the AWS Open Search Service.  There is one index dedicated to each data source.  The ```S3-log-data-to-ES``` scripts run on HuBMAP AWS servers, reading JSON from S3 Objects and creating one ElasticSearch document for each logged event.

