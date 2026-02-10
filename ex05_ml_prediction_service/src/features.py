from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

TARGET_COL = "total_amount"


def build_dataset(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series, Dict[str, List[str]]]:
    """
    Transform raw taxi data into a machine learning dataset.

    Parameters
    ----------
    df : pd.DataFrame
        The raw dataframe loaded from MinIO.

    Returns
    -------
    X : pd.DataFrame
        Feature matrix.
    y : pd.Series
        Target vector (total_amount).
    spec : dict
        Dictionary containing feature columns and metadata.
    """
    required = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        TARGET_COL,
        "PULocationID",
        "DOLocationID",
    ]

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    data = df.copy()

    # Conversion robuste des dates
    data["tpep_pickup_datetime"] = pd.to_datetime(data["tpep_pickup_datetime"], errors="coerce")
    data["tpep_dropoff_datetime"] = pd.to_datetime(data["tpep_dropoff_datetime"], errors="coerce")

    # Suppression des lignes avec des données cruciales manquantes
    data = data.dropna(subset=required)

    # Filtres de cohérence
    data = data[
        (data["passenger_count"] > 0)
        & (data["trip_distance"] > 0)
        & (data[TARGET_COL] > 0)
        & (data["tpep_pickup_datetime"] < data["tpep_dropoff_datetime"])
    ]

    # Trimming des valeurs aberrantes pour stabiliser l'apprentissage
    data = data[(data["trip_distance"] < 100)]  # Réduit pour être plus réaliste
    data = data[(data[TARGET_COL] < 300)]

    # Engineering des variables temporelles
    data["pickup_hour"] = data["tpep_pickup_datetime"].dt.hour.astype(np.int8)
    data["pickup_dayofweek"] = data["tpep_pickup_datetime"].dt.dayofweek.astype(np.int8)
    data["pickup_month"] = data["tpep_pickup_datetime"].dt.month.astype(np.int8)

    # SELECTION DES FEATURES
    # On retire fare_amount, tip_amount, extra, etc. car ce sont des fuites du target !
    numeric_candidates = [
        "passenger_count",
        "trip_distance",
        "pickup_hour",
        "pickup_dayofweek",
        "pickup_month",
    ]

    categorical_candidates = [
        "VendorID",
        "RatecodeID",
        "payment_type",
        "PULocationID",
        "DOLocationID",
    ]

    numeric_cols = [c for c in numeric_candidates if c in data.columns]
    categorical_cols = [c for c in categorical_candidates if c in data.columns]

    # On s'assure que les types sont corrects
    for c in categorical_cols:
        data[c] = data[c].astype(str)

    feature_cols = numeric_cols + categorical_cols
    X = data[feature_cols].copy()
    y = data[TARGET_COL].astype(float).copy()

    spec = {
        "numeric_cols": numeric_cols,
        "categorical_cols": categorical_cols,
        "feature_cols": feature_cols
    }

    return X, y, spec
