from __future__ import annotations

import json
import os
from pathlib import Path

import joblib
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

from src.data import load_parquets_from_minio
from src.features import build_dataset


ARTIFACTS_DIR = Path("artifacts")
MODEL_PATH = ARTIFACTS_DIR / "model.joblib"
SPEC_PATH = ARTIFACTS_DIR / "feature_spec.json"


def _rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Calculate the Root Mean Squared Error.

    Parameters
    ----------
    y_true : np.ndarray
        Ground truth target values.
    y_pred : np.ndarray
        Estimated target values.

    Returns
    -------
    float
        The calculated RMSE value.
    """
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))


def main() -> None:
    """
    Train a traditional ML regression model to predict taxi trip total amount.

    Steps:
    1) Load cleaned parquet(s) from MinIO
    2) Build ML dataset (X, y) + spec via features.build_dataset
    3) Train preprocessing+model pipeline
    4) Save artifacts: model.joblib + feature_spec.json
    """
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    # --- 1) Load data from MinIO (data.py returns ONLY a DataFrame)
    bucket = os.getenv("MINIO_BUCKET", "nyc-cleaned")
    prefix = os.getenv("MINIO_PREFIX", "year=2024/month=01/")
    df = load_parquets_from_minio(bucket=bucket, prefix=prefix)

    # --- 2) Build dataset + spec (features.py returns X, y, spec)
    X, y, spec = build_dataset(df)

    feature_cols = spec["feature_cols"]
    numeric_cols = spec["numeric_cols"]
    categorical_cols = spec["categorical_cols"]

    # --- 3) Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # --- 4) Preprocess
    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            (
                "ordinal",
                OrdinalEncoder(
                    handle_unknown="use_encoded_value",
                    unknown_value=-1,
                ),
            ),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_cols),
            ("cat", categorical_transformer, categorical_cols),
        ],
        remainder="drop",
    )

    # --- 5) Model (kept, best for tabular)
    model = HistGradientBoostingRegressor(
        max_depth=8,
        learning_rate=0.08,
        max_iter=300,
        random_state=42,
    )

    pipe = Pipeline(steps=[("preprocess", preprocessor), ("model", model)])

    # --- 6) Train
    pipe.fit(X_train, y_train)

    # --- 7) Evaluate
    y_pred = pipe.predict(X_test)
    rmse = _rmse(y_test.to_numpy(), y_pred)

    # --- 8) Save artifacts
    joblib.dump(pipe, MODEL_PATH)

    # IMPORTANT: predict.py expects these keys
    payload = {
        "bucket": bucket,
        "prefix": prefix,
        "rmse": float(rmse),
        "feature_cols": feature_cols,
        "numeric_cols": numeric_cols,
        "categorical_cols": categorical_cols,
        "target": "total_amount",
    }
    SPEC_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"[EX5] RMSE: {rmse:.4f}")
    print(f"[EX5] Saved model -> {MODEL_PATH}")
    print(f"[EX5] Saved spec  -> {SPEC_PATH}")


if __name__ == "__main__":
    main()
