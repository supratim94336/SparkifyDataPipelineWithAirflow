class QualityChecks:
    """
    class to check data quality after table insertions and updates
    """
    check_row_count = """
                        SELECT CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END AS non_empty FROM {}
                      """



