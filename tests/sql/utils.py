from substrait.sql.sql_to_substrait import convert
import pyarrow
import substrait.json
import tempfile
import pyarrow.substrait as pa_substrait

def sort_arrow(table: pyarrow.Table):
    import pyarrow.compute as pc

    sort_keys = [(name, "ascending") for name in table.column_names]
    sort_indices = pc.sort_indices(table, sort_keys)
    sorted_table = pc.take(table, sort_indices)
    return sorted_table


def assert_query_datafusion(query: str, data: dict, ignore_order=True):
    from datafusion import SessionContext
    from datafusion import substrait as ss

    ctx = SessionContext()

    for k, v in data.items():
        if isinstance(v, str):
            ctx.register_parquet(k, v)
        else:
            ctx.register_record_batches(k, [v.to_batches()])

    def df_schema_resolver(name: str):
        pa_schema = ctx.sql(f"SELECT * FROM {name} LIMIT 0").schema()
        return pa_substrait.serialize_schema(pa_schema).to_pysubstrait().base_schema

    plan = convert(query, "generic", df_schema_resolver)

    sql_arrow = ctx.sql(query).to_arrow_table()

    substrait_plan = ss.Serde.deserialize_bytes(plan.SerializeToString())
    df_logical_plan = ss.Consumer.from_substrait_plan(ctx, substrait_plan)
    df = ctx.create_dataframe_from_logical_plan(df_logical_plan)
    substrait_arrow = df.to_arrow_table()

    if ignore_order:
        substrait_arrow = sort_arrow(substrait_arrow)
        sql_arrow = sort_arrow(sql_arrow)

    from pprint import pprint

    pprint(substrait_arrow)
    pprint(sql_arrow)

    assert substrait_arrow.equals(sql_arrow, check_metadata=True)


def assert_query_duckdb(query: str, data: dict, ignore_order=True):
    import duckdb

    duckdb.install_extension("substrait", repository="community")
    duckdb.load_extension("substrait")

    with tempfile.TemporaryDirectory() as temp_dir:
        db = f"{temp_dir}/test.db"

        conn = duckdb.connect(db)

        def duckdb_schema_resolver(name: str):
            pa_schema = conn.sql(f"SELECT * FROM {name} LIMIT 0").arrow().schema
            return pa_substrait.serialize_schema(pa_schema).to_pysubstrait().base_schema
        
        for k, v in data.items():
            conn.register(k, v)

        plan = convert(query, "duckdb", duckdb_schema_resolver)

        conn.install_extension("substrait", repository="community")
        conn.load_extension("substrait")

        plan_json = substrait.json.dump_json(plan)
        sql = f"CALL from_substrait_json('{plan_json}')"

        substrait_out = conn.sql(sql)
        sql_out = conn.sql(query)

        substrait_arrow = substrait_out.arrow()
        sql_arrow = sql_out.arrow()

        if ignore_order:
            substrait_arrow = sort_arrow(substrait_arrow)
            sql_arrow = sort_arrow(sql_arrow)

        assert substrait_arrow.equals(sql_arrow, check_metadata=True)
