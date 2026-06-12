from airflow.providers.standard.operators.branch import BaseBranchOperator


class DictBranchOperator(BaseBranchOperator):
    def __init__(
        self,
        *,
        source_task_id: str,
        key: str,
        branch_map: dict[str, str],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_task_id = source_task_id
        self.key = key
        self.branch_map = branch_map

    def choose_branch(self, context):
        value = context["ti"].xcom_pull(task_ids=self.source_task_id)[self.key]
        return self.branch_map[value]
