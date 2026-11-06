from airflow.providers.standard.operators.branch import BaseBranchOperator


class SQLSkipBranchOperator(BaseBranchOperator):
    def __init__(
        self,
        *,
        source_task_id: str,
        skip_branch_id: str,
        continue_branch_id: str,
        target_columns: list[str] | None = None,  # NOTE: not implemented yet
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_task_id = source_task_id
        self.skip_branch_id = skip_branch_id
        self.continue_branch_id = continue_branch_id
        self.target_columns = target_columns

    def choose_branch(self, context):
        sql_result = context["ti"].xcom_pull(task_ids=self.source_task_id)
        if not sql_result:
            return self.skip_branch_id
        return self.continue_branch_id
