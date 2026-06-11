from sqloxide import parse_sql


def build_plan(sql: str):
    tree = parse_sql(sql, dialect="ansi")[0]
    import pprint

    pprint.pprint(tree)


if __name__ == "__main__":
    build_plan("select a, b from X")

    build_plan("select a, b + 1 from X")

    build_plan("select a, b + 1 from X where b > 9000000")
