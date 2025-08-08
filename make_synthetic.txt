import numpy as np, pandas as pd
n=15000; rng=np.random.default_rng(7)
df=pd.DataFrame({
"id": np.arange(n),
"price": np.round(np.abs(rng.normal(50,20,n)),2),
"qty": rng.integers(1,10,n),
"timestamp": pd.date_range("2024-01-01", periods=n, freq="min").astype(str)
})
# ensuciamos un poco
df.loc[rng.choice(n,300,replace=False),"price"]=np.nan
df=df.sample(frac=1,random_state=7).reset_index(drop=True)
df.to_csv("data/sales_raw.csv", index=False)
print("OK -> data/sales_raw.csv")