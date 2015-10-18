from djangit.engine import parser, runner


def git_run_sql(branch, sql, params):
    stmt = parser.parse(sql)
    runner.run(branch, stmt, params)

