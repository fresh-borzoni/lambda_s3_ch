import uuid
import random
import calendar
import time
from datetime import datetime, date
from io import BytesIO
import argparse
import sys

import boto3
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from decimal import Decimal

class Seeder:
    def __init__(self):
        self.s3 = boto3.client('s3', 
                              endpoint_url='http://localstack:4566',
                              aws_access_key_id='test',
                              aws_secret_access_key='test',
                              region_name='us-east-1')
        self.clickhouse_url = 'http://clickhouse:8123'

    def wait_for_services(self):
        """Wait for services to be ready"""
        print("⏳ Waiting for services...")
        
        # Wait for LocalStack
        for i in range(30):
            try:
                self.s3.list_buckets()
                print("✅ LocalStack is ready")
                break
            except Exception:
                if i == 29:
                    raise
                time.sleep(1)
        
        # Wait for ClickHouse
        for i in range(30):
            try:
                requests.get(f"{self.clickhouse_url}/ping")
                print("✅ ClickHouse is ready")
                break
            except Exception:
                if i == 29:
                    raise
                time.sleep(1)

    def setup(self):
        """Create S3 bucket and ClickHouse tables"""
        print("🚀 Setting up infrastructure...")
        
        # Create S3 bucket
        try:
            self.s3.create_bucket(Bucket='transactions')
            print("✅ Created S3 bucket")
        except Exception as e:
            print(f"ℹ️  S3 bucket setup: {str(e)}")

        # Create ClickHouse tables
        create_table_queries = [
            # Regular transactions table
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id UUID,
                date Date,
                type String,
                amount Decimal(18,2),
                created_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            ORDER BY (date, id)
            """,
            
            # S3 table for historical data
            """
            CREATE TABLE IF NOT EXISTS historical_transactions_s3 (
                id UUID,
                date Date,
                type String,
                amount String
            )
            ENGINE = S3('http://localstack:4566/transactions/2024/01/transactions.parquet', 'Parquet')
            SETTINGS 
                s3_endpoint_url = 'http://localstack:4566',
                s3_access_key_id = 'test',
                s3_secret_access_key = 'test'
            """,
            
            # View that combines both sources
            """
            CREATE VIEW IF NOT EXISTS all_transactions AS
            SELECT
                id,
                date,
                type,
                CAST(amount AS Decimal(18,2)) as amount,
                now() as created_at,
                'historical' as source
            FROM historical_transactions_s3
            UNION ALL
            SELECT
                id,
                date,
                type,
                amount,
                created_at,
                'current' as source
            FROM transactions
            """
        ]
        
        for query in create_table_queries:
            try:
                result = requests.post(self.clickhouse_url, data=query)
                if result.status_code != 200:
                    print(f"❌ Error executing query: {result.text}")
                    raise Exception(f"Query failed: {result.text}")
                print("✅ Created ClickHouse table/view")
            except Exception as e:
                print(f"Error creating ClickHouse structure: {str(e)}")
                print(f"Failed query: {query}")
                raise

    def generate_month_data(self, year: int, month: int, count: int) -> pd.DataFrame:
        """Generate random transactions for a month"""
        _, last_day = calendar.monthrange(year, month)
        
        data = [{
            'id': str(uuid.uuid4()),
            'date': pd.Timestamp(f"{year}-{month:02d}-{random.randint(1, last_day):02d}").date(),
            'type': random.choice(['DEPOSIT', 'WITHDRAWAL', 'TRANSFER', 'PAYMENT']),
            'amount': Decimal(str(round(random.uniform(10, 1000), 2)))
        } for _ in range(count)]
        
        df = pd.DataFrame(data)
        return df

    def save_to_s3(self, year: int, month: int, df: pd.DataFrame):
        """Save DataFrame to S3 in Parquet format"""
        print(f"📦 Saving to S3: {year}-{month:02d} ({len(df)} records)")
        
        df_to_save = df.copy()
        df_to_save['amount'] = df_to_save['amount'].astype(str)  # Convert Decimal to string for Parquet
        
        table = pa.Table.from_pandas(df_to_save)
        
        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy', version='2.6')
        buffer.seek(0)

        try:
            key = f'{year}/{month:02d}/transactions.parquet'
            self.s3.put_object(
                Bucket='transactions',
                Key=key,
                Body=buffer.getvalue(),
                ContentType='application/x-parquet'
            )
            print(f"✅ Saved parquet to s3://transactions/{key}")
            
            # Update the S3 table structure to point to the new file
            query = f"""
            ALTER TABLE historical_transactions_s3 
            MODIFY SETTING 
            s3_url = 'http://localstack:4566/transactions/{key}';
            """
            requests.post(self.clickhouse_url, data=query)
            
        except Exception as e:
            print(f"Error saving to S3: {str(e)}")
            raise

    def save_to_clickhouse(self, df: pd.DataFrame):
        """Save DataFrame to ClickHouse"""
        print(f"📊 Saving to ClickHouse: {len(df)} records")
        
        batch_size = 1000
        for start_idx in range(0, len(df), batch_size):
            batch_df = df.iloc[start_idx:start_idx + batch_size]
            
            values = [
                f"('{row.id}', '{row.date}', '{row.type}', {row.amount:.2f})"
                for row in batch_df.itertuples()
            ]
            
            query = f"INSERT INTO transactions (id, date, type, amount) VALUES {','.join(values)}"
            
            try:
                requests.post(self.clickhouse_url, data=query)
            except Exception as e:
                print(f"Error saving batch to ClickHouse: {str(e)}")
                raise

    def check(self):
        """Check data in S3 and ClickHouse"""
        print("\n🔍 Checking data:")
        
        print("\nS3 contents:")
        try:
            response = self.s3.list_objects_v2(Bucket='transactions')
            if 'Contents' in response:
                for obj in response['Contents']:
                    print(f"  {obj['Key']} ({obj['Size']} bytes)")
            else:
                print("  (empty)")
        except Exception as e:
            print(f"  Error reading S3: {str(e)}")

        print("\nClickHouse contents:")
        try:
            queries = [
                """
                SELECT 
                    source,
                    toYYYYMM(date) as month,
                    count(*) as count,
                    round(avg(amount), 2) as avg_amount
                FROM all_transactions 
                GROUP BY source, month 
                ORDER BY month, source
                """
            ]
            for query in queries:
                result = requests.post(self.clickhouse_url, data=query)
                print(f"\nTransaction Summary:\n{result.text}")
        except Exception as e:
            print(f"  Error querying ClickHouse: {str(e)}")

    def seed(self, start_date: str, end_date: str, records_per_month: int):
        """Generate and save data for date range"""
        start_year, start_month = map(int, start_date.split('-'))
        end_year, end_month = map(int, end_date.split('-'))
        today = date.today()

        year, month = start_year, start_month
        while (year, month) <= (end_year, end_month):
            df = self.generate_month_data(year, month, records_per_month)
            
            if (year, month) == (today.year, today.month):
                self.save_to_clickhouse(df)
            else:
                self.save_to_s3(year, month, df)

            month += 1
            if month > 12:
                month = 1
                year += 1

def main():
    parser = argparse.ArgumentParser(description='Manage test data for S3 and ClickHouse')
    parser.add_argument('action', choices=['setup', 'seed', 'check'])
    parser.add_argument('--start', help='Start date (YYYY-MM)', default=None)
    parser.add_argument('--end', help='End date (YYYY-MM)', default=None)
    parser.add_argument('--count', type=int, help='Records per month', default=None)
    args = parser.parse_args()

    seeder = Seeder()

    if args.action == 'setup':
        seeder.wait_for_services()
        seeder.setup()
    elif args.action == 'seed':
        if not all([args.start, args.end, args.count]):
            print("Error: seed requires --start, --end, and --count")
            sys.exit(1)
        seeder.seed(args.start, args.end, args.count)
    elif args.action == 'check':
        seeder.check()

if __name__ == '__main__':
    main()
