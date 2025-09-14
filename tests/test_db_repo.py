import pandas as pd
import sqlite3
import pytest
from fooddelivery.db import DatabaseRepository

def test_push_and_fetch_tmp(monkeypatch, tmp_path):
    # Use sqlite in-memory engine for a light test: monkeypatch engine to sqlite
    import sqlalchemy
    from sqlalchemy import text, create_engine

    engine = create_engine("sqlite:///:memory:", future=True)
    # monkeypatch the engine used by DatabaseRepository
    import fooddelivery.db as dbmod
    monkeypatch.setattr(dbmod, "engine", engine)
    db = DatabaseRepository()

    df = pd.DataFrame({"a":[1,2], "b":["x","y"]})
    # push
    db.push_dataframe(df, table_name="tmp_table", schema=None, if_exists="replace")
    # fetch
    res = db.fetch_dataframe("SELECT * FROM tmp_table")
    assert len(res) == 2
    assert list(res.columns) == ["a", "b"]
