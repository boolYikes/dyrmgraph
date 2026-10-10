from airflow.providers.standard.operators.branch import BaseBranchOperator


class DictBranchOperator(BaseBranchOperator):
    def __init__(
        self,
        *,
        source_task_id: str,
        key: str,
        branch_map: dict[str, str],
        fail_branch: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_task_id = source_task_id
        self.key = key
        self.branch_map = branch_map
        self.fail_branch = fail_branch

    def choose_branch(self, context):
        if not context["ti"].xcom_pull(task_ids=self.source_task_id):
            if self.fail_branch:
                return self.fail_branch
            else:
                raise ValueError(f"No XCom value found for task {self.source_task_id}. Please check the upstream task or provide a fail_branch.")
        # for pickled objects, keys are not specified
        if self.key == "status":
            value = context["ti"].xcom_pull(task_ids=self.source_task_id)[self.key]
        else:
            value = context["ti"].xcom_pull(task_ids=self.source_task_id, key=self.key)
        return self.branch_map[value]
