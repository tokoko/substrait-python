from .utils import assert_query_datafusion


data = {
    "lineitem": "./tests/tpch/lineitem.parquet"
}

def assert_query(sql: str):
    assert_query_datafusion(sql, data, ignore_order=False)

def test_tpch_1():
    assert_query("""
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (CAST(1 AS DECIMAL(15, 2)) - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (CAST(1 AS DECIMAL(15, 2)) - l_discount) * (CAST(1 AS DECIMAL(15, 2)) + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc
--	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval ':1' day
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
LIMIT 1;
""")
