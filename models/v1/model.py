import pickle
import yaml
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, accuracy_score
from helpers import data_prep


def train(data_path: str, artifact_path: str):
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

    print(classification_report(y_test, y_pred))
    print("ROC-AUC:", roc_auc_score(y_test, y_prob))
    print(confusion_matrix(y_test, y_pred))

    baseline_preds = (X_test["diff_seed"] > 0).astype(int)
    print(f"Seed-only accuracy: {accuracy_score(y_test, baseline_preds):.3f}")


# def load(artifact_path: str) -> dict:
#     """Loads a saved model artifact."""
#     with open(artifact_path, "rb") as f:
#         return pickle.load(f)


# def predict_proba(team_a_stats: dict, team_b_stats: dict, artifact_path: str) -> float:
#     """
#     Predicts probability that team_a beats team_b.

#     Args:
#         team_a_stats: Dict of raw stats for team A
#         team_b_stats: Dict of raw stats for team B
#         artifact_path: Path to saved .pkl artifact

#     Returns:
#         Float between 0 and 1 — probability team A wins
#     """
#     artifact = load(artifact_path)
#     model = artifact["model"]
#     scaler = artifact["scaler"]
#     config = artifact["config"]

#     # Build a single-row dataframe in the format compute_features expects
#     matchup = pd.DataFrame([{**{f"team_a_{k}": v for k, v in team_a_stats.items()},
#                               **{f"team_b_{k}": v for k, v in team_b_stats.items()}}])

#     X = compute_features(matchup, config)
#     X_scaled = scaler.transform(X)

#     return model.predict_proba(X_scaled)[0][1]


if __name__ == "__main__":
    train(
        data_path="data/processed/",
        artifact_path="artifacts/2026_v1.pkl"
    )