import pandas as pd
import numpy as np
import os

def generate_data():
    os.makedirs("data", exist_ok=True)
    print("Generating users.csv (5M rows)...")

    num_users = 5_000_000
    users = pd.DataFrame({
        'user_id': np.arange(1, num_users + 1),
        'name': ['User_' + str(i) for i in range(num_users)],
        'signup_date': pd.date_range(start='2020-01-01', periods=num_users, freq='min')
    })
    users.to_csv('data/users.csv', index=False)
    print("users.csv created.")

    print("Generating transactions.csv (10M rows)...")
    num_transactions = 10_000_000
    transactions = pd.DataFrame({
        'transaction_id': np.arange(1, num_transactions + 1),
        'user_id': np.random.randint(1, num_users + 1, size=num_transactions),
        'amount': np.random.uniform(5.0, 500.0, size=num_transactions).round(2)
    })
    transactions.to_csv('data/transactions.csv', index=False)
    print("transactions.csv created.")

if __name__ == "__main__":
    generate_data()
