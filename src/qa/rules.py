import pandas as pd


def require_non_null(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for c in cols:
        mask &= df[c].notna() & (df[c].astype(str).str.strip() != "")
    return mask


def require_in_set(df: pd.DataFrame, col: str, allowed: set[str]) -> pd.Series:
    return df[col].astype(str).str.strip().isin(allowed)


def parse_ts_utc(series: pd.Series) -> pd.Series:
    # Coerce invalid to NaT
    return pd.to_datetime(series, errors="coerce", utc=True)


def require_ts_order(start: pd.Series, end: pd.Series) -> pd.Series:
    # both must be valid and start <= end
    return start.notna() & end.notna() & (start <= end)


def require_non_negative(series: pd.Series) -> pd.Series:
    return series.notna() & (series >= 0)


def dedupe_keep_latest(
    df: pd.DataFrame, key: str, ts_col: str | None = None
) -> pd.DataFrame:
    if ts_col and ts_col in df.columns:
        df = df.sort_values(ts_col)
    return df.drop_duplicates(subset=[key], keep="last")
