# src/predict.py
import json
from pathlib import Path
from typing import Any, Dict

import joblib
import pandas as pd


ARTIFACTS_DIR = Path("artifacts")
MODEL_PATH = ARTIFACTS_DIR / "model.joblib"
SPEC_PATH = ARTIFACTS_DIR / "feature_spec.json"


def _load_artifacts():
    """Load model and feature spec."""
    if not MODEL_PATH.exists():
        raise FileNotFoundError("Model not found. Run: uv run python -m src.train")
    if not SPEC_PATH.exists():
        raise FileNotFoundError("feature_spec.json not found. Run train first.")

    model = joblib.load(MODEL_PATH)
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    return model, spec


def predict_one(record: Dict[str, Any]) -> float:
    """
    Predict total_amount for one ride.

    Parameters
    ----------
    record : Dict[str, Any]
        A dict containing some or all features used during training.

    Returns
    -------
    float
        Predicted price.
    """
    model, spec = _load_artifacts()
    feature_cols = spec["feature_cols"]
    categorical_cols = spec["categorical_cols"]

    row = {c: record.get(c, None) for c in feature_cols}
    X = pd.DataFrame([row], columns=feature_cols)

    for c in categorical_cols:
        if c in X.columns:
            X[c] = X[c].astype(str).fillna("UNKNOWN")

    pred = model.predict(X)[0]
    return float(pred)


def main() -> None:
    """
    CLI example:
    uv run python -m src.predict \
    '{"trip_distance":2.1,"passenger_count":1,"fare_amount":12.0,"PULocationID":"79","DOLocationID":"142","payment_type":"1","pickup_hour":10,"pickup_dayofweek":2,"pickup_month":1}'
    """
    import sys
    if len(sys.argv) < 2:
        raise SystemExit("Provide a JSON string as the first argument.")
    record = json.loads(sys.argv[1])
    yhat = predict_one(record)
    print(f"[EX5] Predicted total_amount: {yhat:.4f}")


if __name__ == "__main__":
    main()
