from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cached_property, cache
from typing import List, Set, Any, Union


@dataclass
class ExecutionArn:
    account: str
    region: str
    state_machine: str
    execution: str

    @staticmethod
    def parse(arn: str):
        parts = arn.split(':')
        if len(parts) != 8:
            raise ValueError(f"Invalid ARN: {arn}")
        return ExecutionArn(account=parts[4], region=parts[3], state_machine=parts[6], execution=parts[7])

    def __str__(self):
        return f"arn:aws:states:{self.region}:{self.account}:execution:{self.state_machine}:{self.execution}"

    def __hash__(self):
        return hash(str(self))

@dataclass
class Event:
    id: int
    start: datetime
    end: datetime
    name: str
    workflow: ExecutionArn

    def extend_end(self, other_end: datetime):
        self.end = max(self.end, other_end)

    @property
    def duration(self) -> timedelta:
        return self.end - self.start

    def total_seconds(self):
        return self.duration.total_seconds()

@dataclass
class AggregateEvent:
    id: int
    start: datetime
    end: datetime
    name: str
    values: List[timedelta]
    contributors: Set[ExecutionArn]

    @property
    def workflow(self):
        return "AGGREGATE"

    @property
    def duration(self) -> timedelta:
        return self.end - self.start

    def total_seconds(self):
        return self.duration.total_seconds()

    def durations(self) -> List[float]:
        return [v.total_seconds() for v in self.values]

    def add_event(self, event: Event):
        self.values.append(event.duration)
        self.contributors.add(event.workflow)
        self.start = min(self.start, event.start)
        self.end = max(self.end, event.end)

@dataclass
class Loop:
    name: str
    start: datetime
    end: datetime
    iterations: int
    events: List[Event]
    names: Set[str]

    @staticmethod
    def from_stack(stack: List[Event]):
        """Create a loop from a stack of state machine events."""
        name = '|'.join(sorted(e.name for e in stack))
        iterations = Counter(e.name for e in stack).most_common(1)[0][1]
        names = set([e.name for e in stack])
        return Loop(
            name=name,
            start=stack[0].start,
            end=stack[-1].end,
            iterations=iterations,
            events=stack,
            names=names,
        )

    @property
    def duration(self) -> timedelta:
        """Get the duration of the loop."""
        return self.end - self.start

    @cached_property
    def simple_name(self) -> str:
        """Get the simple name of the loop."""
        return '|'.join(self.names)

    def total_seconds(self) -> float:
        return self.duration.total_seconds()

    def __contains__(self, item):
        if isinstance(item, (Event, AggregateEvent)):
            return item.name in self.names and self.start <= item.start <= self.end
        raise ValueError(f'Invalid item type: {type(item)}')

    def to_event(self) -> Event:
        """Convert the loop to an event."""
        workflow = self.events[0].workflow
        return Event(start=self.start, end=self.end, name=self.simple_name, workflow=workflow)


@dataclass
class Workflow:
    id: Any
    events: List[Union[Event, AggregateEvent]]
    loops: List[Loop]

    def __hash__(self):
        return hash(self.id)

    def add_events(self, events: List[Event]):
        self.events.extend(events)

    def id_as_filename(self):
        return str(self.id).replace(":", "-").replace("/", "-")

    @property
    def start(self) -> datetime:
        return self.events[0].start

    @property
    def end(self) -> datetime:
        return self.events[-1].end

    @property
    def duration(self) -> timedelta:
        """Get the duration of the workflow."""
        return self.end - self.start

    def total_minutes(self) -> float:
        return self.total_seconds() / 60

    def total_seconds(self) -> float:
        return self.duration.total_seconds()

    def largest_contributors(self, n=10, with_loops=False):
        return self._largest_contributors(with_loops)[:n]

    def _largest_contributors(self, with_loops=False):
        durations = defaultdict(float)
        for event in self.events:
            if event.workflow != self.id:
                continue
            if not with_loops or all(event not in loop for loop in self.loops):
                durations[event.name] += event.total_seconds()
        if with_loops and self.loops:
            for loop in self.loops:
                durations[f"[LOOP] {loop.simple_name}"] += loop.total_seconds()
        return list(sorted(durations.items(), key=lambda x: x[1], reverse=True))