import joblib
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, accuracy_score
from helpers import data_prep


def train():
    # Clean the data
    df = data_prep.data_clean()

    # Extract the our features and targets
    X = data_prep.prepare_features(df)
    y = data_prep.prepare_targets(df)

    # Train / Test data split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42
    )

    # Scale feature data
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Model
    model = LogisticRegression(
        C=1,
        max_iter=1000,
        random_state=42
    )
    model.fit(X_train_scaled, y_train)

    y_pred = model.predict(X_test_scaled)
    y_prob = model.predict_proba(X_test_scaled)[:, 1]

    print('='*5 + ' Full Model ' + '='*5)

    print(classification_report(y_test, y_pred))
    print("ROC-AUC:", roc_auc_score(y_test, y_prob))
    print(confusion_matrix(y_test, y_pred))

    joblib.dump(model, 'model.pkl')
    joblib.dump(scaler, 'scaler.pkl')


if __name__ == "__main__":
    train()