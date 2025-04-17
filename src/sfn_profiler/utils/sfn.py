
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


def process_execution_history(workflow: ExecutionArn, history: List[Dict], separate_retries=False) -> List[Event]:
    """Process execution history to get timing information for each state, including multi-state loops."""
    state_timings: List[Event] = []
    for start_i, event in enumerate(history):
        if 'StateEntered' in event['type']:
            state_name = event['stateEnteredEventDetails']['name']
            start_time = event['timestamp']
            # Find the corresponding StateExited event
            attempts = 1
            for curr_i, exit_event in enumerate(history[start_i + 1:], start_i + 1):
                if 'TaskFailed' in exit_event['type']:
                    next_event = history[curr_i + 1]
                    # Look ahead, if the next event is not scheduling the task again, then we dont want to handle
                    # the failure specifically and want to rely on the state itself exiting.
                    # If the task is rescheduled, then we want to emit an event if combine_consecutive is False
                    # or increment the attempts if combine consecutive is true
                    if 'TaskStateExited' == next_event['type']:
                        continue
                    if separate_retries:
                        end_time = exit_event['timestamp']
                        state_timings.append(Event(start=start_time, end=end_time, name=state_name, workflow=workflow))
                        start_time = next_event['timestamp']
                    else:
                        attempts += 1
                elif 'StateExited' in exit_event['type'] and exit_event['stateExitedEventDetails']['name'] == state_name:
                    end_time = exit_event['timestamp']
                    state_timings.append(
                        Event(
                            start=start_time,
                            end=end_time,
                            name=state_name,
                            workflow=workflow,
                            attempts=attempts))
                    break
    return state_timings
