from enum import Enum

import numpy as np
import pandas as pd
from pytz import FixedOffset, timezone, utc

DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL = "event_timestamp"


class EventTimestampType(Enum):
    TZ_NAIVE = 0
    TZ_AWARE_UTC = 1
    TZ_AWARE_FIXED_OFFSET = 2
    TZ_AWARE_US_PACIFIC = 3


def _convert_event_timestamp(event_timestamp: pd.Timestamp, t: EventTimestampType):
    if t == EventTimestampType.TZ_NAIVE:
        return event_timestamp
    elif t == EventTimestampType.TZ_AWARE_UTC:
        return event_timestamp.replace(tzinfo=utc)
    elif t == EventTimestampType.TZ_AWARE_FIXED_OFFSET:
        return event_timestamp.replace(tzinfo=utc).astimezone(FixedOffset(60))
    elif t == EventTimestampType.TZ_AWARE_US_PACIFIC:
        return event_timestamp.replace(tzinfo=utc).astimezone(timezone("US/Pacific"))


def create_orders_df(
    customers,
    drivers,
    start_date,
    end_date,
    order_count,
    infer_event_timestamp_col=False,
) -> pd.DataFrame:
    """
    Example df generated by this function:

    | order_id | driver_id | customer_id | order_is_success |    event_timestamp  |
    +----------+-----------+-------------+------------------+---------------------+
    |      100 |      5004 |        1007 |                0 | 2021-03-10 19:31:15 |
    |      101 |      5003 |        1006 |                0 | 2021-03-11 22:02:50 |
    |      102 |      5010 |        1005 |                0 | 2021-03-13 00:34:24 |
    |      103 |      5010 |        1001 |                1 | 2021-03-14 03:05:59 |
    """
    df = pd.DataFrame()
    df["order_id"] = [order_id for order_id in range(100, 100 + order_count)]
    df["driver_id"] = np.random.choice(drivers, order_count)
    df["customer_id"] = np.random.choice(customers, order_count)
    df["order_is_success"] = np.random.randint(0, 2, size=order_count).astype(np.int32)

    if infer_event_timestamp_col:
        df["e_ts"] = [
            _convert_event_timestamp(
                pd.Timestamp(dt, unit="ms", tz="UTC").round("ms"),
                EventTimestampType(3),
            )
            for idx, dt in enumerate(
                pd.date_range(start=start_date, end=end_date, periods=order_count)
            )
        ]
        df.sort_values(
            by=["e_ts", "order_id", "driver_id", "customer_id"], inplace=True,
        )
    else:
        df[DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL] = [
            _convert_event_timestamp(
                pd.Timestamp(dt, unit="ms", tz="UTC").round("ms"),
                EventTimestampType(idx % 4),
            )
            for idx, dt in enumerate(
                pd.date_range(start=start_date, end=end_date, periods=order_count)
            )
        ]
        df.sort_values(
            by=[
                DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
                "order_id",
                "driver_id",
                "customer_id",
            ],
            inplace=True,
        )
    return df


def create_driver_hourly_stats_df(drivers, start_date, end_date) -> pd.DataFrame:
    """
    Example df generated by this function:

    | event_timestamp  | driver_id | conv_rate | acc_rate | avg_daily_trips | created          |
    |------------------+-----------+-----------+----------+-----------------+------------------|
    | 2021-03-17 19:31 |     5010  | 0.229297  | 0.685843 | 861             | 2021-03-24 19:34 |
    | 2021-03-17 20:31 |     5010  | 0.781655  | 0.861280 | 769             | 2021-03-24 19:34 |
    | 2021-03-17 21:31 |     5010  | 0.150333  | 0.525581 | 778             | 2021-03-24 19:34 |
    | 2021-03-17 22:31 |     5010  | 0.951701  | 0.228883 | 570             | 2021-03-24 19:34 |
    | 2021-03-17 23:31 |     5010  | 0.819598  | 0.262503 | 473             | 2021-03-24 19:34 |
    |                  |      ...  |      ...  |      ... | ...             |                  |
    | 2021-03-24 16:31 |     5001  | 0.061585  | 0.658140 | 477             | 2021-03-24 19:34 |
    | 2021-03-24 17:31 |     5001  | 0.088949  | 0.303897 | 618             | 2021-03-24 19:34 |
    | 2021-03-24 18:31 |     5001  | 0.096652  | 0.747421 | 480             | 2021-03-24 19:34 |
    | 2021-03-17 19:31 |     5005  | 0.142936  | 0.707596 | 466             | 2021-03-24 19:34 |
    | 2021-03-17 19:31 |     5005  | 0.142936  | 0.707596 | 466             | 2021-03-24 19:34 |
    """
    df_hourly = pd.DataFrame(
        {
            "event_timestamp": [
                pd.Timestamp(dt, unit="ms", tz="UTC").round("ms")
                for dt in pd.date_range(
                    start=start_date, end=end_date, freq="1H", closed="left"
                )
            ]
            # include a fixed timestamp for get_historical_features in the quickstart
            + [
                pd.Timestamp(
                    year=2021, month=4, day=12, hour=7, minute=0, second=0, tz="UTC"
                )
            ]
        }
    )
    df_all_drivers = pd.DataFrame()

    for driver in drivers:
        df_hourly_copy = df_hourly.copy()
        df_hourly_copy["driver_id"] = driver
        df_all_drivers = pd.concat([df_hourly_copy, df_all_drivers])

    df_all_drivers.reset_index(drop=True, inplace=True)
    rows = df_all_drivers["event_timestamp"].count()

    df_all_drivers["conv_rate"] = np.random.random(size=rows).astype(np.float32)
    df_all_drivers["acc_rate"] = np.random.random(size=rows).astype(np.float32)
    df_all_drivers["avg_daily_trips"] = np.random.randint(0, 1000, size=rows).astype(
        np.int32
    )
    df_all_drivers["created"] = pd.to_datetime(pd.Timestamp.now(tz=None).round("ms"))

    # Create duplicate rows that should be filtered by created timestamp
    # TODO: These duplicate rows area indirectly being filtered out by the point in time join already. We need to
    #  inject a bad row at a timestamp where we know it will get joined to the entity dataframe, and then test that
    #  we are actually filtering it with the created timestamp
    late_row = df_all_drivers[rows // 2 : rows // 2 + 1]
    df_all_drivers = pd.concat([df_all_drivers, late_row, late_row], ignore_index=True)

    return df_all_drivers


def create_customer_daily_profile_df(customers, start_date, end_date) -> pd.DataFrame:
    """
    Example df generated by this function:

    | event_timestamp  | customer_id | current_balance | avg_passenger_count | lifetime_trip_count | created          |
    |------------------+-------------+-----------------+---------------------+---------------------+------------------|
    | 2021-03-17 19:31 | 1010        | 0.889188        |     0.049057        |          412        | 2021-03-24 19:38 |
    | 2021-03-18 19:31 | 1010        | 0.979273        |     0.212630        |          639        | 2021-03-24 19:38 |
    | 2021-03-19 19:31 | 1010        | 0.976549        |     0.176881        |           70        | 2021-03-24 19:38 |
    | 2021-03-20 19:31 | 1010        | 0.273697        |     0.325012        |           68        | 2021-03-24 19:38 |
    | 2021-03-21 19:31 | 1010        | 0.438262        |     0.313009        |          192        | 2021-03-24 19:38 |
    |                  |  ...        |      ...        |          ...        |          ...        |                  |
    | 2021-03-19 19:31 | 1001        | 0.738860        |     0.857422        |          344        | 2021-03-24 19:38 |
    | 2021-03-20 19:31 | 1001        | 0.848397        |     0.745989        |          106        | 2021-03-24 19:38 |
    | 2021-03-21 19:31 | 1001        | 0.301552        |     0.185873        |          812        | 2021-03-24 19:38 |
    | 2021-03-22 19:31 | 1001        | 0.943030        |     0.561219        |          322        | 2021-03-24 19:38 |
    | 2021-03-23 19:31 | 1001        | 0.354919        |     0.810093        |          273        | 2021-03-24 19:38 |
    """
    df_daily = pd.DataFrame(
        {
            "event_timestamp": [
                pd.Timestamp(dt, unit="ms", tz="UTC").round("ms")
                for dt in pd.date_range(
                    start=start_date, end=end_date, freq="1D", closed="left"
                )
            ]
        }
    )
    df_all_customers = pd.DataFrame()

    for customer in customers:
        df_daily_copy = df_daily.copy()
        df_daily_copy["customer_id"] = customer
        df_all_customers = pd.concat([df_daily_copy, df_all_customers])

    df_all_customers.reset_index(drop=True, inplace=True)

    rows = df_all_customers["event_timestamp"].count()

    df_all_customers["current_balance"] = np.random.random(size=rows).astype(np.float32)
    df_all_customers["avg_passenger_count"] = np.random.random(size=rows).astype(
        np.float32
    )
    df_all_customers["lifetime_trip_count"] = np.random.randint(
        0, 1000, size=rows
    ).astype(np.int32)

    # TODO: Remove created timestamp in order to test whether its really optional
    df_all_customers["created"] = pd.to_datetime(pd.Timestamp.now(tz=None).round("ms"))
    return df_all_customers
