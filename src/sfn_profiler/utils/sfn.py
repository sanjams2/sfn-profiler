
from typing import List, Dict

from sfn_profiler.clients.boto import get_account, get_region
from sfn_profiler.models import ExecutionArn, Event


def get_execution_arn(id: str) -> ExecutionArn:
    split = id.split(':')
    if len(split) == 8:
        return ExecutionArn.parse(id)
    if len(split) == 2:
        account = get_account()
        region = get_region()
        return ExecutionArn(account=account, region=region, state_machine=split[0], execution=split[1])
    raise ValueError(f"Invalid execution id: {id}")


def process_execution_history(workflow: ExecutionArn, history: List[Dict], combine_consecutive=True) -> List[Event]:
    """Process execution history to get timing information for each state, including multi-state loops."""
    state_timings = []
    for event in history:
        if 'StateEntered' in event['type']:
            state_name = event['stateEnteredEventDetails']['name']
            start_time = event['timestamp']
            id = event['id']
            # Find the corresponding StateExited event
            for exit_event in history[history.index(event) + 1:]:
                if 'StateExited' in exit_event['type'] and exit_event['stateExitedEventDetails']['name'] == state_name:
                    end_time = exit_event['timestamp']
                    if state_timings and state_timings[-1].name == state_name and combine_consecutive:
                        state_timings[-1].extend_end(end_time)
                    else:
                        state_timings.append(Event(id=id, start=start_time, end=end_time, name=state_name, workflow=workflow))
                    break
    return state_timings