# Lambda Architecture Example

This project demonstrates a simple lambda architecture with a batch layer using Parquet files in S3 and a real-time layer using ClickHouse.

## Components

- **Batch Layer**: Data is stored in Parquet format in an S3 bucket. This represents the batch processing component of the lambda architecture.
- **Real-time Layer**: Data is also ingested into a ClickHouse database, which serves as the real-time processing component. The ClickHouse database uses the S3 engine to seamlessly integrate with the Parquet files in S3.
- **Seeder**: A Python script that generates sample data and populates both the S3 Parquet files and the ClickHouse database.

## Usage

The `run.sh` script provides the following commands:

```bash
./run.sh start  # Start the services
./run.sh stop   # Stop the services
./run.sh seed START END COUNT  # Generate and seed data
./run.sh check  # Check the data
```

- **start** : Builds the Docker containers, starts the services, and runs the initial seeding script.
- **stop** : Stops and removes the Docker containers.
- **seed START END COUNT**: Generates sample data for the given date range (in the format YYYY-MM) and count, and populates both the S3 Parquet files and the ClickHouse database.
- **check** : Runs a script to validate the data in the ClickHouse database.

## Prerequisites
 - Docker
 - Docker Compose

## Example Run

```sh
anton.borisov@Anton-Borisov-MacBook-Pro lambda_s3_ch % ./run.sh start
✔ Network lambda_s3_ch_default         Created                                                                                                    0.0s 
✔ Container lambda_s3_ch-seeder-1      Started                                                                                                    0.1s 
✔ Container lambda_s3_ch-clickhouse-1  Started                                                                                                    0.1s 
✔ Container lambda_s3_ch-localstack-1  Started                                                                                                    0.2s 
⏳ Waiting for services...
✅ LocalStack is ready
✅ ClickHouse is ready
🚀 Setting up infrastructure...
✅ Created S3 bucket
✅ Created ClickHouse table/view
✅ Created ClickHouse table/view
✅ Created ClickHouse table/view
```

```sh
anton.borisov@Anton-Borisov-MacBook-Pro lamdba_s3_ch % ./run.sh seed 2024-01 2024-11 1000
📦 Saving to S3: 2024-01 (1000 records)
✅ Saved parquet to s3://transactions/2024/01/transactions.parquet
📦 Saving to S3: 2024-02 (1000 records)
✅ Saved parquet to s3://transactions/2024/02/transactions.parquet
📦 Saving to S3: 2024-03 (1000 records)
✅ Saved parquet to s3://transactions/2024/03/transactions.parquet
📦 Saving to S3: 2024-04 (1000 records)
✅ Saved parquet to s3://transactions/2024/04/transactions.parquet
📦 Saving to S3: 2024-05 (1000 records)
✅ Saved parquet to s3://transactions/2024/05/transactions.parquet
📦 Saving to S3: 2024-06 (1000 records)
✅ Saved parquet to s3://transactions/2024/06/transactions.parquet
📦 Saving to S3: 2024-07 (1000 records)
✅ Saved parquet to s3://transactions/2024/07/transactions.parquet
📦 Saving to S3: 2024-08 (1000 records)
✅ Saved parquet to s3://transactions/2024/08/transactions.parquet
📦 Saving to S3: 2024-09 (1000 records)
✅ Saved parquet to s3://transactions/2024/09/transactions.parquet
📦 Saving to S3: 2024-10 (1000 records)
✅ Saved parquet to s3://transactions/2024/10/transactions.parquet
📊 Saving to ClickHouse: 1000 records
```

```sh
anton.borisov@Anton-Borisov-MacBook-Pro lamdba_s3_ch % ./run.sh check
🔍 Checking data:
S3 contents:
  2024/01/transactions.parquet (48957 bytes)
  2024/02/transactions.parquet (48986 bytes)
  2024/03/transactions.parquet (49072 bytes)
  2024/04/transactions.parquet (48968 bytes)
  2024/05/transactions.parquet (48955 bytes)
  2024/06/transactions.parquet (48966 bytes)
  2024/07/transactions.parquet (48876 bytes)
  2024/08/transactions.parquet (49107 bytes)
  2024/09/transactions.parquet (48845 bytes)
  2024/10/transactions.parquet (49057 bytes)

ClickHouse contents:
Transaction Summary:
historical	202401	1000	501.45
current	202411	1000	507.35
```

## License
This project is licensed under the MIT License.