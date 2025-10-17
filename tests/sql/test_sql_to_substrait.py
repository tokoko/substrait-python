import pyarrow
import pytest
import sys
from .utils import assert_query_datafusion, assert_query_duckdb

data: pyarrow.Table = pyarrow.Table.from_batches(
    [
        pyarrow.record_batch(
            [[2, 1, 3, 4], ["a", "b", "c", "d"]],
            names=["store_id", "name"],
        )
    ]
)


sales_data = pyarrow.Table.from_batches(
    [
        pyarrow.record_batch(
            [
                [1, 2, 3],
                [1, 1, 4],
                [10, 20, 50],
            ],
            names=["sale_id", "fk_store_id", "amount"],
        )
    ]
)


data = {
    "stores": data,
    "sales": sales_data
}


def assert_query(query: str, engine: str, ignore_order=True):
    if engine == "duckdb":
        assert_query_duckdb(query, data, ignore_order)
    elif engine == "datafusion":
        assert_query_datafusion(query, data, ignore_order)


engines = [
    pytest.param(
        "duckdb",
        marks=pytest.mark.skipif(
            sys.platform.startswith("win"),
            reason="duckdb substrait extension not found on windows",
        ),
    ),
    "datafusion",
]


@pytest.mark.parametrize("engine", engines)
def test_select_field(engine: str):
    assert_query("""SELECT store_id FROM stores""", engine)


@pytest.mark.parametrize("engine", engines)
def test_inner_join_filtered(engine: str):
    assert_query(
        """SELECT sale_id + 1 as sale_id, name
                    FROM sales
                    INNER JOIN stores ON store_id = fk_store_id
                    WHERE sale_id < 3
                    """,
        engine,
    )


@pytest.mark.parametrize("engine", engines)
def test_left_join(engine: str):
    assert_query(
        """SELECT sale_id + 1 as sale_id, name
                    FROM sales
                    LEFT JOIN stores ON store_id = fk_store_id
                    """,
        engine,
    )


@pytest.mark.parametrize("engine", engines)
def test_right_join(engine: str):
    assert_query(
        """SELECT sale_id + 1 as sale_id, name
                    FROM sales
                    RIGHT JOIN stores ON store_id = fk_store_id
                    """,
        engine,
    )


@pytest.mark.parametrize("engine", engines)
def test_group_by_empty_measures(engine: str):
    assert_query(
        """SELECT fk_store_id, sale_id
                    FROM sales
                    GROUP BY fk_store_id, sale_id
                    """,
        engine,
    )


@pytest.mark.parametrize("engine", engines)
def test_group_by_count(engine: str):
    assert_query(
        """SELECT fk_store_id, SUM(amount) as income
                    FROM sales
                    GROUP BY fk_store_id
                    """,
        engine,
    )


@pytest.mark.parametrize("engine", engines)
def test_group_by_unnamed_expr(engine: str):
    assert_query(
        """SELECT fk_store_id + 2 AS plustwo, SUM(amount) as income
                    FROM sales
                    GROUP BY fk_store_id + 2
                    """,
        engine,
    )


@pytest.mark.parametrize("engine", engines)
def test_sum(engine: str):
    assert_query(
        """SELECT SUM(amount) + SUM(fk_store_id) as income
                    FROM sales
                    """,
        engine,
    )


@pytest.mark.parametrize("engine", engines)
def test_group_by_hidden_dimension(engine: str):
    assert_query(
        """SELECT fk_store_id
                    FROM sales
                    GROUP BY fk_store_id, sale_id
                    """,
        engine,
    )


@pytest.mark.parametrize("engine", engines)
def test_group_by_having_no_duplicate(engine: str):
    assert_query(
        """SELECT fk_store_id, SUM(amount + 1) as income
                    FROM sales
                    GROUP BY fk_store_id
                    HAVING SUM(amount) < 40
                    """,
        engine,
    )


@pytest.mark.parametrize("engine", engines)
def test_group_by_having_duplicate(engine: str):
    assert_query(
        """SELECT fk_store_id, SUM(amount) as income
                    FROM sales
                    GROUP BY fk_store_id
                    HAVING SUM(amount) < 40
                    """,
        engine,
    )


@pytest.mark.parametrize("engine", engines)
def test_order_by(engine: str):
    assert_query(
        """SELECT store_id FROM stores ORDER BY store_id""", engine, ignore_order=False
    )


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(
            "duckdb",
            marks=[
                pytest.mark.skipif(
                    sys.platform.startswith("win"),
                    reason="duckdb substrait extension not found on windows",
                ),
                pytest.mark.xfail,
            ],
        ),
        "datafusion",
    ],
)
def test_select_limit(engine: str):
    assert_query("""SELECT store_id FROM stores ORDER BY store_id LIMIT 2""", engine)


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(
            "duckdb",
            marks=[
                pytest.mark.skipif(
                    sys.platform.startswith("win"),
                    reason="duckdb substrait extension not found on windows",
                ),
                pytest.mark.xfail,
            ],
        ),
        "datafusion",
    ],
)
def test_select_limit_offset(engine: str):
    assert_query(
        """SELECT store_id FROM stores ORDER BY store_id LIMIT 2 OFFSET 2""", engine
    )


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(
            "duckdb",
            marks=[
                pytest.mark.skipif(
                    sys.platform.startswith("win"),
                    reason="duckdb substrait extension not found on windows",
                ),
                pytest.mark.xfail,
            ],
        ),
        "datafusion",
    ],
)
def test_row_number(engine: str):
    assert_query(
        """SELECT sale_id, fk_store_id, row_number() over (partition by fk_store_id order by sale_id) as rn
                    FROM sales
                    """,
        engine,
    )
