import duckdb
from etl import etl_pipeline

def test_pipeline_end_to_end():
    # Ejecuta el pipeline completo
    etl_pipeline.run()

    # Conecta a DuckDB y verifica los datos cargados
    con = duckdb.connect("analytics.duckdb")
    result = con.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
    assert result > 0, "La tabla sales no tiene datos"

    # También podrías verificar alguna regla
    nulos = con.execute("SELECT COUNT(*) FROM sales WHERE price IS NULL").fetchone()[0]
    assert nulos == 0, "Existen valores nulos en price, lo cual no debería pasar"

    con.close()
