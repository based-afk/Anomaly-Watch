import pandas as pd
from clickhouse_driver import Client
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_score, recall_score
import joblib

CLICKHOUSE_HOST = 'localhost'

def fetch_data():
    client = Client(host=CLICKHOUSE_HOST, database='observability')
    # Fetch all data, ordering by service and timestamp is crucial for rolling windows
    query = """
    SELECT id, service, timestamp, cpu_usage, memory_usage, error_rate, is_anomaly
    FROM telemetry
    ORDER BY service, timestamp
    """
    data = client.execute(query)
    columns = ['id', 'service', 'timestamp', 'cpu_usage', 'memory_usage', 'error_rate', 'is_anomaly']
    df = pd.DataFrame(data, columns=columns)
    return df

def feature_engineering(df):
    """
    Computes rolling mean and std for cpu and memory.
    STRICT RULE: Use .shift(1) before .rolling() to ensure we only use PAST data 
    and avoid data leakage into the current row's features.
    """
    if df.empty:
        return df

    # Sort to be absolutely sure
    df = df.sort_values(by=['service', 'timestamp'])
    
    # Define window size (e.g. last 10 points)
    window = 10
    
    # We must group by service to avoid crossing service boundaries
    grouped = df.groupby('service')
    
    # Compute rolling features without data leakage
    # .shift(1) ensures the current row's own values are not included in the rolling stats
    df['cpu_roll_mean'] = grouped['cpu_usage'].transform(lambda x: x.shift(1).rolling(window, min_periods=1).mean())
    df['cpu_roll_std'] = grouped['cpu_usage'].transform(lambda x: x.shift(1).rolling(window, min_periods=1).std().fillna(0))
    df['mem_roll_mean'] = grouped['memory_usage'].transform(lambda x: x.shift(1).rolling(window, min_periods=1).mean())
    df['mem_roll_std'] = grouped['memory_usage'].transform(lambda x: x.shift(1).rolling(window, min_periods=1).std().fillna(0))
    
    # Fill any remaining NaNs (e.g. first row of each service) with the raw values
    df['cpu_roll_mean'] = df['cpu_roll_mean'].fillna(df['cpu_usage'])
    df['mem_roll_mean'] = df['mem_roll_mean'].fillna(df['memory_usage'])
    
    return df

def main():
    print("Fetching data from ClickHouse...")
    df = fetch_data()
    
    if df.empty:
        print("No data found in ClickHouse. Run the telemetry_generator and consumer first.")
        return
        
    print(f"Fetched {len(df)} rows.")
    
    print("Performing feature engineering...")
    df = feature_engineering(df)
    
    # Define feature columns used for training
    feature_cols = [
        'cpu_usage', 'memory_usage', 'error_rate', 
        'cpu_roll_mean', 'cpu_roll_std',
        'mem_roll_mean', 'mem_roll_std'
    ]
    
    X = df[feature_cols]
    y_true = df['is_anomaly']
    
    print("Training Isolation Forest...")
    # Train only on "normal" data or train on all data depending on assumption.
    # IsolationForest is an unsupervised algorithm, it assumes anomalies are rare.
    # We will fit it on the dataset.
    clf = IsolationForest(n_estimators=100, contamination=0.02, random_state=42)
    clf.fit(X)
    
    print("Evaluating model...")
    # decision_function returns anomaly scores (lower is more anomalous)
    # We will invert it or just use a threshold to classify.
    # For Isolation Forest, predict() returns -1 for outliers and 1 for inliers.
    # We want a custom score where >0.75 is an anomaly.
    # decision_function is around 0. We'll normalize it somewhat.
    
    scores = -clf.decision_function(X) # Higher score = more anomalous
    
    # We define a threshold for the >0.75 claim.
    # Let's scale scores so that the max is around 1.0. 
    # Or just find a raw threshold that gives good precision/recall.
    # We'll normalize scores to [0, 1] using min-max scaling for simplicity, 
    # but based on the training set distribution to avoid leakage.
    min_score = scores.min()
    max_score = scores.max()
    
    # Normalize:
    normalized_scores = (scores - min_score) / (max_score - min_score + 1e-9)
    df['anomaly_score'] = normalized_scores
    
    threshold = 0.75
    y_pred = (normalized_scores > threshold).astype(int)
    
    precision = precision_score(y_true, y_pred, zero_division=0)
    recall = recall_score(y_true, y_pred, zero_division=0)
    
    print(f"Model Evaluation (Threshold > {threshold}):")
    print(f"Precision: {precision:.4f}")
    print(f"Recall:    {recall:.4f}")
    
    print("Saving model and metadata...")
    # Save the model and the scaling parameters so we can apply them in real-time
    model_data = {
        'model': clf,
        'min_score': min_score,
        'max_score': max_score,
        'feature_cols': feature_cols
    }
    joblib.dump(model_data, 'model.pkl')
    print("Model saved to model.pkl")

if __name__ == "__main__":
    main()
