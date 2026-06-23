import asyncio
import os
import sys
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# Ensure backend directory is in sys.path for importing app
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from app.config import settings
    DATABASE_URL = settings.database_url_async
except ImportError:
    DATABASE_URL = os.getenv("DATABASE_URL_ASYNC")

if not DATABASE_URL:
    user = os.getenv("POSTGRES_USER", "pulse_admin")
    password = os.getenv("POSTGRES_PASSWORD", "")
    db = os.getenv("POSTGRES_DB", "pulse_analytics")
    DATABASE_URL = f"postgresql+asyncpg://{user}:{password}@localhost:5444/{db}"

async def main():
    print("=" * 60)
    print("Connecting to Pipeline Database...")
    masked_url = f"postgresql+asyncpg://***@{DATABASE_URL.split('@')[-1]}" if "@" in DATABASE_URL else DATABASE_URL
    print(f"URL: {masked_url}")
    print("=" * 60)
    
    engine = create_async_engine(DATABASE_URL, echo=False)
    
    try:
        async with engine.connect() as conn:
            # 1. Count the total rows in the ingested_events table.
            res_events = await conn.execute(text("SELECT COUNT(*) FROM ingested_events;"))
            total_events = res_events.scalar()
            
            # 2. Count the total rows in the dead_letter_queue table.
            res_dlq = await conn.execute(text("SELECT COUNT(*) FROM dead_letter_queue;"))
            total_dlq = res_dlq.scalar()
            
            # 3. Fetch correlation_id, event_type, created_at for last 3 records
            res_last_3 = await conn.execute(
                text("SELECT correlation_id, event_type, created_at FROM ingested_events ORDER BY created_at DESC LIMIT 3;")
            )
            last_3 = res_last_3.all()
            
            print("\nPipeline Database Report")
            print("-" * 60)
            print(f"Total Ingested Events: {total_events}")
            print(f"Total Dead Letter Queue Entries: {total_dlq}")
            print("-" * 60)
            print("Last 3 Ingested Events:")
            if not last_3:
                print("  No records found in ingested_events table.")
            else:
                for idx, row in enumerate(last_3, start=1):
                    print(f"  {idx}. Correlation ID: {row[0]}")
                    print(f"     Event Type:     {row[1]}")
                    print(f"     Created At:     {row[2]}")
                    print()
            print("-" * 60)
            
            # Temporary time_bucket test
            try:
                print("Testing time_bucket query...")
                bucket_minutes = 1
                metric_name = "market_tick"
                import datetime
                interval_val = datetime.timedelta(minutes=bucket_minutes)
                query = text("""
                    SELECT 
                        time_bucket(:bucket_interval, created_at) AS bucket,
                        COUNT(*) as total_ticks,
                        MIN((payload->>'value')::float) as min_value,
                        MAX((payload->>'value')::float) as max_value,
                        AVG((payload->>'value')::float) as avg_value
                    FROM ingested_events
                    WHERE event_type = :metric_name
                    GROUP BY bucket
                    ORDER BY bucket DESC
                    LIMIT 20;
                """)
                res_bucket = await conn.execute(query, {
                    "bucket_interval": interval_val,
                    "metric_name": metric_name,
                })
                print("Success! Row count:", len(res_bucket.all()))
            except Exception as inner_e:
                print("Failed time_bucket query:", inner_e)
            
    except Exception as e:
        print(f"ERROR: Failed to query database: {e}", file=sys.stderr)
    finally:
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
