import psycopg
from datetime import date, datetime

conn: psycopg.connection.Connection = psycopg.connect(
    "host=127.0.0.1 port=5432 user=tom password=pencil dbname=public"
)
conn.autocommit = True


def data(format: str):
    return [
        (
            1,
            1.0,
            "a",
            True,
            date(2012, 1, 1),
            datetime(2012, 1, 1),
            [1, None, 2],
            [1.0, None, 2.0],
            ["a", None, "b"],
            [True, None, False],
            [date(2012, 1, 1), None, date(2012, 1, 2)],
            [datetime(2012, 1, 1), None, datetime(2012, 1, 2)],
            (
                (1, 1.0, "a", True, date(2012, 1, 1), datetime(2012, 1, 1))
                if format == "text"
                else (
                    "1",
                    "1",
                    "a",
                    "t",
                    "2012-01-01",
                    "2012-01-01 00:00:00.000000",
                )
            ),
            (
                [(1, 1.0, "a", True, date(2012, 1, 1), datetime(2012, 1, 1))]
                if format == "text"
                else [
                    (
                        "1",
                        "1",
                        "a",
                        "t",
                        "2012-01-01",
                        "2012-01-01 00:00:00.000000",
                    )
                ]
            ),
        ),
        (
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            (
                (None, None, None, None, None, None)
                if format == "text"
                else ("", "", "", "", "", "")
            ),
            (
                [(None, None, None, None, None, None)]
                if format == "text"
                else [("", "", "", "", "", "")]
            ),
        ),
        (
            2,
            2.0,
            "b",
            False,
            date(2012, 1, 2),
            datetime(2012, 1, 2),
            None,
            None,
            None,
            None,
            None,
            None,
            (
                (2, 2.0, "b", False, date(2012, 1, 2), datetime(2012, 1, 2))
                if format == "text"
                else (
                    "2",
                    "2",
                    "b",
                    "f",
                    "2012-01-02",
                    "2012-01-02 00:00:00.000000",
                )
            ),
            (
                [(2, 2.0, "b", False, date(2012, 1, 2), datetime(2012, 1, 2))]
                if format == "text"
                else [
                    (
                        "2",
                        "2",
                        "b",
                        "f",
                        "2012-01-02",
                        "2012-01-02 00:00:00.000000",
                    )
                ]
            ),
        ),
    ]


def assert_select_all(results: list[psycopg.rows.Row], format: str):
    expected = data(format)

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
    assert_select_all(results, "text")

with conn.cursor(binary=False) as cur:
    cur.execute("SELECT * FROM all_types")
    results = cur.fetchall()
    assert_select_all(results, "binary")
