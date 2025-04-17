from typing import Dict, List, Tuple

from sfn_profiler.models import ExecutionArn
from sfn_profiler.utils.cache import filecache


class SfnClient:

    def __init__(self, boto_session):
        self.client = boto_session.client("stepfunctions")

    def __str__(self):
        return "SfnClient()"

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(str(self))

    @filecache
    def get_state_machine_info(self, execution_arn: ExecutionArn) -> Tuple[Dict, List[Dict]]:
        """Get state machine execution history and definition."""
        history = []
        params = dict(executionArn=str(execution_arn))
        while True:
            response = self.client.get_execution_history(**params)
            history.extend(response['events'])
            if 'nextToken' not in response:
                break
            params['nextToken'] = response['nextToken']

        execution_details = self.client.describe_execution(executionArn=str(execution_arn))
        return execution_details, history
