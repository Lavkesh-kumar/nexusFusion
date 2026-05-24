import os
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
# NUM_BUCKETS: controls how many partitions we split the files into.
# Each bucket must fit in RAM. With 256MB limit and ~500MB files, 10 buckets
# means each bucket is ~50MB.
NUM_BUCKETS = 10
CHUNK_SIZE  = 100_000       # rows read at a time from disk
TEMP_DIR    = "data/temp_buckets"
USERS_CSV   = "data/users.csv"
TRANS_CSV   = "data/transactions.csv"
RESULT_CSV  = "data/result.csv"


def _get_bucket(user_id: int) -> int:
    """Deterministic bucket assignment: same user_id always -> same bucket."""
    return int(user_id) % NUM_BUCKETS


def _partition(filepath: str, prefix: str, id_col: str) -> None:
    """
    Stream through a large CSV in chunks and write each row to its
    corresponding bucket file based on user_id hash.

    Example:
        user_id=101 → 101 % 10 = 1 → users_bucket_1.csv
        user_id=202 → 202 % 10 = 2 → users_bucket_2.csv
    """
    logger.info(f"Partitioning {filepath} into {NUM_BUCKETS} buckets...")

    # Remove stale bucket files from a previous run
    for b in range(NUM_BUCKETS):
        path = os.path.join(TEMP_DIR, f"{prefix}_bucket_{b}.csv")
        if os.path.exists(path):
            os.remove(path)

    for chunk in pd.read_csv(filepath, chunksize=CHUNK_SIZE):
        chunk[id_col] = chunk[id_col].astype(int)

        for bucket_id in range(NUM_BUCKETS):
            subset = chunk[chunk[id_col].apply(_get_bucket) == bucket_id]
            if subset.empty:
                continue

            bucket_path = os.path.join(TEMP_DIR, f"{prefix}_bucket_{bucket_id}.csv")
            write_header = not os.path.exists(bucket_path)
            subset.to_csv(bucket_path, mode='a', index=False, header=write_header)

    logger.info(f"Partitioning of {filepath} complete.")


def run_join() -> None:
    """
    Full out-of-core INNER JOIN pipeline:
      1. Partition users.csv   → N bucket files
      2. Partition transactions.csv → N bucket files
      3. For each bucket i: load users_bucket_i + transactions_bucket_i,
         do an in-memory merge, append to result.csv
      4. Clean up temp files
    """
    logger.info("=== Join job started ===")
    os.makedirs(TEMP_DIR, exist_ok=True)

    # Step 1 & 2 : partition both files
    _partition(USERS_CSV, "users", "user_id")
    _partition(TRANS_CSV, "transactions", "user_id")

    # Step 3 : join bucket by bucket
    logger.info("Joining buckets...")
    first_write = True
    total_rows  = 0

    for bucket_id in range(NUM_BUCKETS):
        u_path = os.path.join(TEMP_DIR, f"users_bucket_{bucket_id}.csv")
        t_path = os.path.join(TEMP_DIR, f"transactions_bucket_{bucket_id}.csv")

        # A missing bucket just means no user_ids hashed to it, skip safely
        if not os.path.exists(u_path) or not os.path.exists(t_path):
            logger.info(f"Bucket {bucket_id}: skipped (no matching rows)")
            continue

        users_bucket = pd.read_csv(u_path)
        trans_bucket = pd.read_csv(t_path)

        merged = pd.merge(users_bucket, trans_bucket, on='user_id', how='inner')
        total_rows += len(merged)

        merged.to_csv(
            RESULT_CSV,
            mode='w' if first_write else 'a',
            index=False,
            header=first_write
        )
        first_write = False
        logger.info(f"Bucket {bucket_id}: {len(merged)} rows joined")

    # Step 4 : cleanup temp files
    for f in os.listdir(TEMP_DIR):
        os.remove(os.path.join(TEMP_DIR, f))

    logger.info(f"=== Join job complete. Total rows: {total_rows}. Output: {RESULT_CSV} ===")


if __name__ == "__main__":
    run_join()
