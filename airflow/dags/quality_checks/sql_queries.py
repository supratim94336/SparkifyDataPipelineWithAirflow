class QualityChecks:
    count_check = """SELECT CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END AS non_empty FROM {}"""

