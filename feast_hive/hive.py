from pathlib import Path
import contextlib
from datetime import datetime
from typing import Any, Callable, ContextManager, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from dateutil import parser
from pydantic import StrictBool, StrictInt, StrictStr
from pydantic.typing import Literal
from pytz import utc
from six import reraise

from feast import FeatureView
from feast.data_source import DataSource
from feast.errors import InvalidEntityType
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast_hive.hive_source import HiveSource
from feast_hive.hive_type_map import hive_to_pa_value_type, pa_to_hive_value_type

try:
    from impala.dbapi import connect as impala_connect
    from impala.hiveserver2 import CBatch as ImpalaCBatch
    from impala.hiveserver2 import Column as ImpalaColumn
    from impala.interface import Connection as ImpalaConnection
    from impala.interface import Cursor as ImpalaCursor
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("hive", str(e))


# default Hive settings that can be overridden in offline_store:hive_conf of feature_store.yaml
DEFAULT_HIVE_CONF = {
    "hive.strict.checks.cartesian.product": "false",
    "hive.mapred.mode": "nonstrict",
    "hive.support.quoted.identifiers": "none",
    "hive.resultset.use.unique.column.names": "false",
    "hive.exec.temporary.table.storage": "memory",
}


class HiveOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for Hive """

    type: StrictStr = "hive"
    """ Offline store type selector """

    engine_type: Literal["hive", "impala"] = "hive"
    """ "hive" or "impala" suported """

    host: StrictStr
    """ Host name of the HiveServer2 """

    port: StrictInt = 10000
    """ Port number of HiveServer2 (default is 10000) """

    database: StrictStr = "default"
    """ Default database when obtaining new cursors """

    timeout: Optional[StrictInt] = None
    """ Connection timeout in seconds when communicating with HiveServer2 """

    hive_conf: Optional[Dict[str, str]] = None
    """ Configuration overlay for the HiveServer2 session """

    entity_uploading_chunk_size: StrictInt = 10000
    """ The chunk size of multiple insert when uploading entity_df to Hive,
        tuning this may affect the performance of get_historical_features """

    use_ssl: StrictBool = False
    """ Use SSL when connecting to HiveServer2 """

    ca_cert: Optional[StrictStr] = None
    """ Local path to the the third-party CA certificate. If SSL is enabled but
        the certificate is not specified, the server certificate will not be
        validated. """

    auth_mechanism: StrictStr = "PLAIN"
    """ {'NOSASL', 'PLAIN' <- default, 'GSSAPI', 'LDAP'}.
        Use PLAIN for non-secured Hive clusters.  Use NOSASL for 
        non-secured Impala connections.  Use LDAP for LDAP authenticated
        connections.  Use GSSAPI for Kerberos-secured clusters. """

    user: Optional[StrictStr] = None
    """ LDAP user to authenticate """

    password: Optional[StrictStr] = None
    """ LDAP password to authenticate """

    kerberos_service_name: Optional[StrictStr] = "hive"
    """ Specify particular impalad service principal. """

    query_template: StrictStr = "default"
    """ What SQL template to use for historical feature retrival (select from feast_hive/templates directory) """

    def __init__(self, **data: Any):
        if "hive_conf" not in data:
            data["hive_conf"] = DEFAULT_HIVE_CONF.copy()
        elif type(data["hive_conf"]) == dict:
            data["hive_conf"] = dict(
                list(DEFAULT_HIVE_CONF.items()) + list(data["hive_conf"].items())
            )
        else:
            raise TypeError("hive_conf should be a dictionary!")

        super().__init__(**data)


class HiveConnection:
    def __init__(self, store_config: HiveOfflineStoreConfig):
        assert isinstance(store_config, HiveOfflineStoreConfig)
        self._store_config = store_config
        self._real_conn = impala_connect(
            **store_config.dict(
                exclude={"type", "entity_uploading_chunk_size", "hive_conf", "engine_type", "query_template"}
            )
        )

    @property
    def real_conn(self) -> ImpalaConnection:
        return self._real_conn

    def close(self) -> None:
        self.real_conn.close()

    def cursor(self) -> ImpalaCursor:
        if self._store_config.hive_conf and self._store_config.engine_type == "hive":
            return self.real_conn.cursor(configuration=self._store_config.hive_conf)
        else:
            return self.real_conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_type is not None:
            reraise(exc_type, exc_val, exc_tb)


class HiveOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, HiveOfflineStoreConfig)
        assert isinstance(data_source, HiveSource)

        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamps = [event_timestamp_column]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        field_string = ", ".join(join_key_columns + feature_name_columns + timestamps)

        start_date = _format_datetime(start_date)
        end_date = _format_datetime(end_date)

        # if config.offline_store.engine_type == "hive":
        #     queries = ["SET hive.resultset.use.unique.column.names=false"]
        # elif config.offline_store.engine_type == "impala":
        #     queries = []
        # else:
        #     raise ValueError("engine_type must be `hive` or `impala`")

        queries = []
        queries.append(
            f"""
            SELECT 
                {field_string}
                {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS feast_row_
                FROM {from_expression} t1
                WHERE {event_timestamp_column} BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
            ) t2
            WHERE feast_row_ = 1
            """)

        conn = HiveConnection(config.offline_store)
        return HiveRetrievalJob(conn, queries)

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, HiveOfflineStoreConfig)
        conn = HiveConnection(config.offline_store)

        @contextlib.contextmanager
        def query_generator() -> ContextManager[List[str]]:
            table_name = offline_utils.get_temp_entity_table_name()

            try:
                entity_schema = _upload_entity_df_and_get_entity_schema(
                    config, conn, table_name, entity_df
                )

                entity_df_event_timestamp_col = offline_utils.infer_event_timestamp_from_entity_df(
                    entity_schema
                )

                expected_join_keys = offline_utils.get_expected_join_keys(
                    project, feature_views, registry
                )

                offline_utils.assert_expected_columns_in_entity_df(
                    entity_schema, expected_join_keys, entity_df_event_timestamp_col
                )

                entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
                    entity_df, entity_df_event_timestamp_col, conn, table_name,
                )

                query_contexts = offline_utils.get_feature_view_query_context(
                    feature_refs,
                    feature_views,
                    registry,
                    project,
                    entity_df_event_timestamp_range,
                )

                query_tmpl = open(Path(__file__).parent / f"templates/{config.offline_store.query_template}.sql").read()

                rendered_query = offline_utils.build_point_in_time_query(
                    query_contexts,
                    left_table_query_string=table_name,
                    entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                    entity_df_columns=entity_schema.keys(),
                    query_template=query_tmpl,
                    full_feature_names=full_feature_names,
                )

                # In order to use `REGEX Column Specification`, need set `hive.support.quoted.identifiers` to None.
                # Can study more here: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select
                queries = rendered_query.split(";")

                yield queries

            finally:
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        return HiveRetrievalJob(
            conn,
            query_generator,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
        )


class HiveRetrievalJob(RetrievalJob):
    def __init__(
        self,
        conn: HiveConnection,
        queries: Union[str, List[str], Callable[[], ContextManager[List[str]]]],
        full_feature_names: bool = False,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
    ):
        assert (
            isinstance(queries, str) or isinstance(queries, list) or callable(queries)
        )

        if callable(queries):
            self._queries_generator = queries
        else:
            if isinstance(queries, str):
                queries = [queries]
            elif not isinstance(queries, list):
                raise TypeError(
                    "queries should be a context manager yielding list[queries]"
                    "or list[str] or str"
                )

            @contextlib.contextmanager
            def query_generator() -> ContextManager[List[str]]:
                yield queries

            self._queries_generator = query_generator

        self._conn = conn
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> Optional[List[OnDemandFeatureView]]:
        return self._on_demand_feature_views

    def _to_df_internal(self) -> pd.DataFrame:
        return self._to_arrow_internal().to_pandas()

    def _to_arrow_internal(self) -> pa.Table:
        with self._queries_generator() as queries:
            with self._conn.cursor() as cursor:
                for query in queries:
                    cursor.execute(query)
                batches = cursor.fetchcolumnar()
                schema = pa.schema(
                    [
                        (field[0], hive_to_pa_value_type(field[1]))
                        for field in cursor.description
                    ]
                )
                pa_batches = [
                    HiveRetrievalJob._convert_hive_batch_to_arrow_batch(b, schema)
                    for b in batches
                ]
                return pa.Table.from_batches(pa_batches, schema)

    @staticmethod
    def _convert_hive_batch_to_arrow_batch(
        hive_batch: ImpalaCBatch, schema: pa.Schema
    ) -> pa.RecordBatch:
        return pa.record_batch(
            [
                HiveRetrievalJob._get_values_from_column(column)
                for column in hive_batch.columns
            ],
            schema,
        )

    @staticmethod
    def _get_values_from_column(column: ImpalaColumn) -> List:
        values = column.values
        for i in range(len(values)):
            if column.nulls[i]:
                values[i] = None
        return values


def _format_datetime(t: datetime):
    # Since Hive does not support timezone, need to transform to utc.
    if t.tzinfo:
        t = t.astimezone(tz=utc)
    t = t.strftime("%Y-%m-%d %H:%M:%S.%f")
    return t


def _upload_entity_df_and_get_entity_schema(
    config: RepoConfig,
    conn: HiveConnection,
    table_name: str,
    entity_df: Union[pd.DataFrame, str],
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        _upload_entity_df_by_insert(
            conn,
            table_name,
            entity_df,
            config.offline_store.entity_uploading_chunk_size,
        )
        return dict(zip(entity_df.columns, entity_df.dtypes))
    elif isinstance(entity_df, str):
        with conn.cursor() as cursor:
            cursor.execute(
                f"CREATE TABLE {table_name} STORED AS PARQUET AS {entity_df}"
            )
        limited_entity_df = HiveRetrievalJob(conn, [f"SELECT * FROM {table_name} LIMIT 1"]).to_df()
        return dict(zip(limited_entity_df.columns, limited_entity_df.dtypes))
    else:
        raise InvalidEntityType(type(entity_df))


def _upload_entity_df_by_insert(
    conn: HiveConnection,
    table_name: str,
    entity_df: Union[pd.DataFrame, str],
    chunk_size: int = None,
) -> None:
    """Uploads a Pandas DataFrame to Hive by data insert"""
    entity_df.reset_index(drop=True, inplace=True)

    pa_table = pa.Table.from_pandas(entity_df)
    hive_schema = []
    for field in pa_table.schema:
        hive_type = pa_to_hive_value_type(str(field.type))
        if not hive_type:
            raise ValueError(f'Not supported type "{field.type}" in entity_df.')
        hive_schema.append((field.name, hive_type))

    with conn.cursor() as cursor:
        # Create Hive temporary table according to entity_df schema
        create_entity_table_sql = f"""
            CREATE TABLE {table_name} (
              {', '.join([f'{col_name} {col_type}' for col_name, col_type in hive_schema])}
            )
            STORED AS PARQUET
            """
        cursor.execute(create_entity_table_sql)

        def preprocess_value(raw_value, col_type):
            if raw_value is None:
                return "null"

            col_type = col_type.lower()

            if col_type == "timestamp":
                if isinstance(raw_value, datetime):
                    raw_value = _format_datetime(raw_value)
                    return f'"{raw_value}"'
                else:
                    return f'"{raw_value}"'

            if col_type in ["string", "date"]:
                return f'"{raw_value}"'
            else:
                return str(raw_value)

        # Upload entity_df to the Hive table by multiple rows insert method
        entity_count = len(pa_table)
        chunk_size = entity_count if chunk_size is None else chunk_size
        pa_batches = pa_table.to_batches(chunk_size)
        if len(pa_batches) > 1:
            # fix this hive bug: https://issues.apache.org/jira/browse/HIVE-19316
            cursor.execute(f"truncate table {table_name}")
        for batch in pa_batches:
            chunk_data = []
            for i in range(len(batch)):
                chunk_data.append(
                    [
                        preprocess_value(batch.columns[j][i].as_py(), hive_schema[j][1])
                        for j in range(len(hive_schema))
                    ]
                )

            entity_chunk_insert_sql = f"""
                INSERT INTO TABLE {table_name}
                VALUES ({'), ('.join([', '.join(chunk_row) for chunk_row in chunk_data])})
            """
            cursor.execute(entity_chunk_insert_sql)


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    conn: HiveConnection,
    table_name: str,
) -> Tuple[datetime, datetime]:
    # TODO haven't got time to test this yet,
    #      just use fake min and max datetime for now, since they will be detected inside the sql
    return datetime.now(), datetime.now()

    # if isinstance(entity_df, pd.DataFrame):
    #     entity_df_event_timestamp = entity_df.loc[
    #         :, entity_df_event_timestamp_col
    #     ].infer_objects()
    #     if pd.api.types.is_string_dtype(entity_df_event_timestamp):
    #         entity_df_event_timestamp = pd.to_datetime(
    #             entity_df_event_timestamp, utc=True
    #         )
    #     entity_df_event_timestamp_range = (
    #         entity_df_event_timestamp.min(),
    #         entity_df_event_timestamp.max(),
    #     )
    # elif isinstance(entity_df, str):
    #     # If the entity_df is a string (SQL query), determine range
    #     # from table
    #     with conn.cursor() as cursor:
    #         cursor.execute(
    #             f"SELECT MIN({entity_df_event_timestamp_col}) AS min, MAX({entity_df_event_timestamp_col}) AS max FROM {table_name}",
    #         )
    #         result = cursor.fetchone()
    #         assert (
    #             result is not None
    #         ), "Fetching the EntityDataframe's timestamp range failed."
    #         # TODO haven't tested this yet
    #         entity_df_event_timestamp_range = (
    #             parser.parse(result[0]),
    #             parser.parse(result[1]),
    #         )
    # else:
    #     raise InvalidEntityType(type(entity_df))
    #
    # return entity_df_event_timestamp_range