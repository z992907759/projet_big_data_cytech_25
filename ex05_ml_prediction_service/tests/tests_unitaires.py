import pytest
import pandas as pd
import numpy as np
from src.features import build_dataset

def test_data_quality():
    """Test unitaires sur les données d'entrée (Exercice 5).""" [cite: 83]
    # Simulation d'un petit DataFrame comme celui de MinIO
    data = {
        'passenger_count': [1, 2],
        'trip_distance': [2.5, 3.0],
        'fare_amount': [10.0, 15.0],
        'total_amount': [12.0, 18.0],
        'PULocationID': [100, 101],
        'DOLocationID': [102, 103],
        'tpep_pickup_datetime': pd.to_datetime(['2024-01-01 10:00:00', '2024-01-01 11:00:00']),
        'tpep_dropoff_datetime': pd.to_datetime(['2024-01-01 10:10:00', '2024-01-01 11:15:00'])
    }
    df = pd.DataFrame(data)
    
    X, y, spec = build_dataset(df)

    # TEST 1 : Sur la target (total_amount doit être positif) [cite: 79]
    assert (y > 0).all(), "Certains prix totaux sont négatifs ou nuls"
    
    # TEST 2 : Sur les features (pas de valeurs manquantes après build_dataset)
    assert not X.isnull().values.any(), "Il reste des valeurs manquantes dans les features"
    
    # TEST 3 : Cohérence : distance toujours positive
    assert (df['trip_distance'] > 0).all(), "La distance de trajet doit être supérieure à 0"
