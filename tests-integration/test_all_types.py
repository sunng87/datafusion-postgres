import psycopg
from datetime import date, datetime

conn: psycopg.connection.Connection = psycopg.connect(
    "host=127.0.0.1 port=5432 user=tom password=pencil dbname=public"
)
conn.autocommit = True


def assert_select_all(results: list[psycopg.rows.Row]):
    expected = [
        (
            1,
            1.0,
            "a",
            True,
            date(2012, 1, 1),
            datetime(2012, 1, 1),
            [1, 2],
            (10, "x", date(2012, 1, 1), datetime(2012, 1, 1)),
            [(10, "x", date(2012, 1, 1), datetime(2012, 1, 1))],
        ),
        (
            2,
            2.5,
            "b",
            False,
            date(2012, 1, 2),
            datetime(2012, 1, 2),
            [3, 4],
            (20, "y", date(2012, 1, 2), datetime(2012, 1, 2, 0, 0)),
            [(20, "y", date(2012, 1, 2), datetime(2012, 1, 2, 0, 0))],
        ),
        (
            3,
            3.3,
            "c",
            True,
            date(2012, 1, 3),
            datetime(2012, 1, 3),
            [5, 6],
            (30, "z", date(2012, 1, 3), datetime(2012, 1, 3, 0, 0)),
            [(30, "z", date(2012, 1, 3), datetime(2012, 1, 3, 0, 0))],
        ),
    ]

    assert len(results) == len(
        expected
    ), f"Expected {len(expected)} rows, got {len(results)}"

    for i, (res_row, exp_row) in enumerate(zip(results, expected)):
        assert len(res_row) == len(exp_row), f"Row {i} column count mismatch"
        for j, (res_val, exp_val) in enumerate(zip(res_row, exp_row)):
            assert (
                res_val == exp_val
            ), f"Mismatch at row {i}, column {j}: expected {exp_val}, got {res_val}"


with conn.cursor(binary=True) as cur:
    cur.execute("SELECT count(*) FROM all_types")
    results = cur.fetchone()
    assert results[0] == 3

with conn.cursor(binary=False) as cur:
    cur.execute("SELECT count(*) FROM all_types")
    results = cur.fetchone()
    assert results[0] == 3

with conn.cursor(binary=True) as cur:
    cur.execute("SELECT * FROM all_types")
    results = cur.fetchall()
    assert_select_all(results)

with conn.cursor(binary=False) as cur:
    cur.execute("SELECT * FROM all_types")
    results = cur.fetchall()
    assert_select_all(results)
