

```markdown
# Anomalies Interpretation

## What This File Represents
This note explains how to read the anomaly values computed for South Asia in 2024.

The anomalies are based on a within-year monthly reference:

\[
\text{anomaly}_{d,m} = x_{d,m} - \overline{x}_{m}
\]

where:
- \(x_{d,m}\) is the value on day \(d\) in month \(m\)
- \(\overline{x}_{m}\) is the mean of that variable for month \(m\) (using 2024 data)

## How To Interpret Sign and Magnitude
- Positive anomaly: higher than that month’s average
- Negative anomaly: lower than that month’s average
- Near zero: close to monthly-normal conditions

Larger absolute values indicate stronger departures from the monthly baseline.

## Temperature Anomalies
- `t2m_c_anom`: daily mean near-surface temperature departure
- `t2m_c_max_anom`: daily maximum temperature departure
- `t2m_c_min_anom`: daily minimum temperature departure

Example:
- \(t2m\_c\_anom = +1.5\) means the day was 1.5°C warmer than the average day of that month.
- \(t2m\_c\_anom = -1.5\) means it was 1.5°C cooler than that month’s average.

## Radiation and Cloud Anomalies
- `ssr_anom`: surface shortwave radiation anomaly
- `str_nlw_anom`: surface longwave radiation anomaly
- `net_rad_anom`: net surface radiation anomaly (`ssr + str_nlw`)
- `tcc_anom`: cloud cover anomaly
- `reflected_solar_anom`: reflected solar anomaly (`tisr - tsr`)

Typical physical interpretation:
- Higher cloud anomaly (`tcc_anom > 0`) often reduces incoming shortwave at surface (`ssr_anom < 0`).
- Sustained positive `net_rad_anom` can support warmer `t2m_c_anom`, though not always on the same day due to lag and circulation effects.

## Practical Reading Pattern
When analyzing a period:
1. Check if `t2m_c_anom` remains positive/negative for multiple days (persistence).
2. Compare with `net_rad_anom` and `tcc_anom` to infer possible drivers.
3. Use `t2m_c_max_anom` and `t2m_c_min_anom` to distinguish daytime vs nighttime signal strength.

## Important Limitation
These are **within-2024 monthly anomalies**, not long-term climate anomalies.

So this answers:
- “Was this day unusual relative to its month in 2024?”

It does **not** answer:
- “Was 2024 unusual relative to a long-term climatology (e.g., 1991–2020)?”

## Note on `number_anom`
`number_anom` is constant (0.0) and does not add interpretation value. It can be excluded from plots and summaries.
```