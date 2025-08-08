import pandas as pd
from etl.etl_pipeline import transform

def test_quality_rules():
    df = pd.DataFrame({
        "id":[1,2,3,3],
        "price":[10,20,30,40],
        "qty":[1,2,3,4],
        "timestamp":["2024-01-01","2024-01-02","2024-01-03","2024-01-04"]
    })
    out = transform(df)
    assert out["id"].is_unique
    assert out["price"].isna().mean() < 0.05
    assert (out["qty"] > 0).all()
