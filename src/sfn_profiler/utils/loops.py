from typing import List, Set

from sfn_profiler.models import Event, Loop


def find_loops_in_execution(history: List[Event]) -> List[Loop]:
    """Find all loops in the Step Functions execution history."""
    loops: List[Loop] = []
    stack: List[Event] = []
    current_loop: Set[str] = set()

    for event in history:
        if not current_loop:
            curr_stack_names: List[str] = list(e.name for e in stack)
            if event.name in curr_stack_names:
                idx = curr_stack_names.index(event.name)
                current_loop = set(curr_stack_names[idx:])
                stack = stack[idx:] + [event]
            else:
                stack.append(event)
        elif current_loop and event.name not in current_loop:
            # Start a new potential loop
            loops.append(Loop.from_stack(stack))
            current_loop = set()
            stack = [event]
        else:  # current loop and state_name in current_loop
            stack.append(event)
    return loops


def coalesce_loop_events(execution_history: List[Event], loops: List[Loop]) -> List[Event]:
    """Remove loop events from the execution history."""
    loops_removed = [event for event in execution_history if not any(event in loop for loop in loops)]
    for loop in loops:
        loops_removed.append(loop.to_event())
    return loops_removed
