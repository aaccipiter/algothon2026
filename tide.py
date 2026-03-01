import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX
import requests

# ── 1. Fetch Thames tidal data ──────────────────────────────────────────────
THAMES_MEASURE = "0006-level-tidal_level-i-15_min-mAOD"

resp = requests.get(
    f"https://environment.data.gov.uk/flood-monitoring/id/measures/{THAMES_MEASURE}/readings",
    params={"_sorted": "", "_limit": 500},
)
resp.raise_for_status()
items = resp.json().get("items", [])

df = pd.DataFrame(items)[["dateTime", "value"]].rename(
    columns={"dateTime": "time", "value": "level"}
)
df["time"] = pd.to_datetime(df["time"], utc=True)
df = df.sort_values("time").reset_index(drop=True)

# ── 2. Prepare dataframe ────────────────────────────────────────────────────
df["time"] = pd.to_datetime(df["time"])
df = df[df["time"] >= pd.to_datetime("2026-02-01 00:00:00+00:00")]
df["time"] = df["time"].dt.tz_convert(None)
df = df.set_index("time").sort_index().asfreq("15T")

# ── 3. Fit SARIMA ───────────────────────────────────────────────────────────
model = SARIMAX(
    df["level"],
    order=(1, 0, 1),
    seasonal_order=(1, 0, 1, 50),
    enforce_stationarity=False,
    enforce_invertibility=False,
)
results = model.fit(disp=False)

# ── 4. Forecast to target time ──────────────────────────────────────────────
last_time = df.index[-1]
target_time = pd.Timestamp("2026-03-01 12:00:00")
steps = int((target_time - last_time).total_seconds() / 900)

forecast = results.get_forecast(steps=steps)

# ── 5. Combine in-sample + out-of-sample predictions ────────────────────────
insample_pred = results.get_prediction(start=0).predicted_mean
oos_pred = forecast.predicted_mean
all_pred = pd.concat([insample_pred, oos_pred])

sarima_pred_df = pd.DataFrame({"pred_mean": all_pred})
sarima_pred_df["level"] = df["level"].reindex(sarima_pred_df.index)

# ── 6. Slice target window & compute price ──────────────────────────────────
start = pd.Timestamp("2026-02-28 12:00:00")
end = pd.Timestamp("2026-03-01 12:00:00")
window = sarima_pred_df.loc[start:end].copy()


def straddle(value):
    if value < 0.2:
        return 0.2 - value
    elif value > 0.25:
        return value - 0.25
    return 0.0


window["level_diff"] = window["pred_mean"].diff().abs()
window["price"] = window["level_diff"].apply(straddle)

# ── 7. Output ────────────────────────────────────────────────────────────────
print(f"Rows:       {len(window)}")
print(f"sum(price): {window['price'].sum() * 100}")
