import joblib
import pandas as pd
import numpy as np
import yaml
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, accuracy_score

# My Libs
import features

def load_config():
    # TODO: This is repeated here and in features.py -> reduce
    with open("models/v1/config.yaml") as f:
        return yaml.safe_load(f)

def train():
    config = load_config()

    # Clean the data
    df = features.prepare_data()

    # Extract the our features and targets
    X = features.prepare_features(df)
    y = features.prepare_targets(df)

    # Train / Test data split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=config['training']['test_size'],
        random_state=config['model']['hyperparameters']['random_state']
    )

    # Scale feature data
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Model
    model = LogisticRegression(
        C=config["model"]["hyperparameters"]["C"],
        max_iter=config["model"]["hyperparameters"]["max_iter"],
        random_state=config["model"]["hyperparameters"]["random_state"]
    )
    model.fit(X_train_scaled, y_train)

    y_pred = model.predict(X_test_scaled)
    y_prob = model.predict_proba(X_test_scaled)[:, 1]

    print('='*5 + ' Model Results ' + '='*5)

    print(classification_report(y_test, y_pred))
    print("ROC-AUC:", roc_auc_score(y_test, y_prob))
    print(confusion_matrix(y_test, y_pred))

    # Create artifacts
    joblib.dump(model, './artifacts/v1/model.pkl')
    joblib.dump(scaler, './artifacts/v1/scaler.pkl')


if __name__ == "__main__":
    train()