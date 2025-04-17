import pytest
from datetime import datetime
from sfn_profiler.models import Event, Loop, ExecutionArn
from sfn_profiler.utils.loops import find_loops_in_execution, coalesce_loop_events

# Test fixtures
@pytest.fixture
def execution_arn():
    """Create a sample execution ARN for testing."""
    return ExecutionArn(account="123456789012", region="us-east-1", state_machine="test", execution="test")

@pytest.fixture
def simple_events(execution_arn):
    """Create a simple list of events with no loops."""
    return [
        Event(
            start=datetime(2023, 1, 1, 10, 0, 0),
            end=datetime(2023, 1, 1, 10, 1, 0),
            name="State1",
            workflow=execution_arn
        ),
        Event(
            start=datetime(2023, 1, 1, 10, 1, 0),
            end=datetime(2023, 1, 1, 10, 2, 0),
            name="State2",
            workflow=execution_arn
        ),
        Event(
            start=datetime(2023, 1, 1, 10, 2, 0),
            end=datetime(2023, 1, 1, 10, 3, 0),
            name="State3",
            workflow=execution_arn
        ),
    ]

@pytest.fixture
def events_with_loop(execution_arn):
    """Create a list of events with a simple loop."""
    return [
        Event(
            start=datetime(2023, 1, 1, 10, 0, 0),
            end=datetime(2023, 1, 1, 10, 1, 0),
            name="State1",
            workflow=execution_arn
        ),
        Event(
            start=datetime(2023, 1, 1, 10, 1, 0),
            end=datetime(2023, 1, 1, 10, 2, 0),
            name="State2",
            workflow=execution_arn
        ),
        Event(
            start=datetime(2023, 1, 1, 10, 2, 0),
            end=datetime(2023, 1, 1, 10, 3, 0),
            name="State1",  # Loop back to State1
            workflow=execution_arn
        ),
        Event(
            start=datetime(2023, 1, 1, 10, 3, 0),
            end=datetime(2023, 1, 1, 10, 4, 0),
            name="State2",  # Continue loop
            workflow=execution_arn
        ),
        Event(
            start=datetime(2023, 1, 1, 10, 4, 0),
            end=datetime(2023, 1, 1, 10, 5, 0),
            name="State3",  # Exit the loop
            workflow=execution_arn
        ),
    ]

@pytest.fixture
def events_with_multiple_loops(execution_arn):
    """Create a list of events with multiple separate loops."""
    return [
        Event(
            start=datetime(2023, 1, 1, 10, 0, 0),
            end=datetime(2023, 1, 1, 10, 1, 0),
            name="State1",
            workflow=execution_arn
        ),
        Event(
            start=datetime(2023, 1, 1, 10, 1, 0),
            end=datetime(2023, 1, 1, 10, 2, 0),
            name="State2",
            workflow=execution_arn
        ),
        Event(
            start=datetime(2023, 1, 1, 10, 2, 0),
            end=datetime(2023, 1, 1, 10, 3, 0),
            name="State1",  # First loop
            workflow=execution_arn
        ),
        Event(
            start=datetime(2023, 1, 1, 10, 3, 0),
            end=datetime(2023, 1, 1, 10, 4, 0),
            name="State3",  # Exit first loop
            workflow=execution_arn
        ),
        Event(
            start=datetime(2023, 1, 1, 10, 4, 0),
            end=datetime(2023, 1, 1, 10, 5, 0),
            name="State4",
            workflow=execution_arn
        ),
        Event(
            start=datetime(2023, 1, 1, 10, 5, 0),
            end=datetime(2023, 1, 1, 10, 6, 0),
            name="State4",  # Second loop
            workflow=execution_arn
        ),
        Event(
            start=datetime(2023, 1, 1, 10, 6, 0),
            end=datetime(2023, 1, 1, 10, 7, 0),
            name="State5",  # Exit second loop
            workflow=execution_arn
        ),
    ]

@pytest.fixture
def nested_loop_events(execution_arn):
    """Create events with a nested loop structure."""
    return [
        Event(start=datetime(2023, 1, 1, 10, 0), end=datetime(2023, 1, 1, 10, 1), name="A", workflow=execution_arn),
        Event(start=datetime(2023, 1, 1, 10, 1), end=datetime(2023, 1, 1, 10, 2), name="B", workflow=execution_arn),
        Event(start=datetime(2023, 1, 1, 10, 2), end=datetime(2023, 1, 1, 10, 3), name="C", workflow=execution_arn),
        Event(start=datetime(2023, 1, 1, 10, 3), end=datetime(2023, 1, 1, 10, 4), name="B", workflow=execution_arn),  # Inner loop
        Event(start=datetime(2023, 1, 1, 10, 4), end=datetime(2023, 1, 1, 10, 5), name="C", workflow=execution_arn),  # Inner loop
        Event(start=datetime(2023, 1, 1, 10, 5), end=datetime(2023, 1, 1, 10, 6), name="A", workflow=execution_arn),  # Outer loop
        Event(start=datetime(2023, 1, 1, 10, 6), end=datetime(2023, 1, 1, 10, 7), name="D", workflow=execution_arn),  # Exit all loops
    ]


# Tests for find_loops_in_execution function
def test_find_loops_in_execution_empty():
    """Test find_loops_in_execution with empty input."""
    loops = find_loops_in_execution([])
    assert loops == []

def test_find_loops_in_execution_no_loops(simple_events):
    """Test find_loops_in_execution with no loops."""
    loops = find_loops_in_execution(simple_events)
    assert len(loops) == 0

def test_find_loops_in_execution_single_loop(events_with_loop):
    """Test find_loops_in_execution with a single loop."""
    loops = find_loops_in_execution(events_with_loop)
    assert len(loops) == 1

    loop = loops[0]
    # Check that the loop contains the right events
    assert len(loop.events) == 4  # State1, State2, State1, State2
    assert loop.names == {"State1", "State2"}
    # Check that the loop has the right timing
    assert loop.start == events_with_loop[0].start
    assert loop.end == events_with_loop[3].end
    # Check iterations count
    assert loop.iterations == 2  # Each state appears twice

def test_find_loops_in_execution_multiple_loops(events_with_multiple_loops):
    """Test find_loops_in_execution with multiple loops."""
    loops = find_loops_in_execution(events_with_multiple_loops)

    # We expect 2 loops:
    # - First loop: State1, State2, State1
    # - Second loop: State4, State4
    assert len(loops) == 2

    # Check first loop
    first_loop = loops[0]
    assert first_loop.names == {"State1", "State2"}
    assert first_loop.start == events_with_multiple_loops[0].start
    assert first_loop.end == events_with_multiple_loops[2].end

    # Check second loop
    second_loop = loops[1]
    assert second_loop.names == {"State4"}
    assert second_loop.start == events_with_multiple_loops[4].start
    assert second_loop.end == events_with_multiple_loops[5].end

def test_find_loops_nested(nested_loop_events):
    """Test finding nested loops."""
    loops = find_loops_in_execution(nested_loop_events)

    # The algorithm will identify the inner B→C→B→C loop, but will miss the outer A→...→A loop
    assert len(loops) == 1

    # The loop should contain B and C states
    loop = loops[0]
    assert loop.names == {"B", "C"}
    assert "A" not in loop.names
    assert "D" not in loop.names

    # Verify the loop covers the expected events
    assert loop.start == nested_loop_events[1].start  # Starts at first B
    assert loop.end == nested_loop_events[4].end      # Ends at second C


# Tests for coalesce_loop_events function
def test_coalesce_loop_events_empty():
    """Test coalesce_loop_events with empty inputs."""
    assert coalesce_loop_events([], []) == []

def test_coalesce_loop_events_no_loops(simple_events):
    """Test coalesce_loop_events with no loops."""
    result = coalesce_loop_events(simple_events, [])
    assert result == simple_events

def test_coalesce_loop_events_with_loops(events_with_loop):
    """Test coalesce_loop_events with loops."""
    loops = find_loops_in_execution(events_with_loop)
    result = coalesce_loop_events(events_with_loop, loops)

    # We expect 2 events: the loop event and State3 (which is not part of the loop)
    assert len(result) == 2

    # One event should be the loop event
    loop_simple_names = {loop.simple_name for loop in loops}
    loop_events = [e for e in result if e.name in loop_simple_names]
    assert len(loop_events) == 1

    # The other event should be State3
    non_loop_events = [e for e in result if e.name == "State3"]
    assert len(non_loop_events) == 1

def test_coalesce_loop_events_with_multiple_loops(events_with_multiple_loops):
    """Test coalesce_loop_events with multiple loops."""
    loops = find_loops_in_execution(events_with_multiple_loops)
    result = coalesce_loop_events(events_with_multiple_loops, loops)

    # We expect 4 events: 2 loop events, State3, and State5
    assert len(result) == 4

    # Check for loop events
    loop_simple_names = {loop.simple_name for loop in loops}
    loop_events = [e for e in result if e.name in loop_simple_names]
    assert len(loop_events) == 2

    # Check for non-loop events
    non_loop_events = [e for e in result if e.name in {"State3", "State5"}]
    assert len(non_loop_events) == 2
    assert any(e.name == "State3" for e in non_loop_events)
    assert any(e.name == "State5" for e in non_loop_events)

def test_coalesce_loop_events_nested(nested_loop_events):
    """Test coalescing nested loops."""
    loops = find_loops_in_execution(nested_loop_events)
    result = coalesce_loop_events(nested_loop_events, loops)

    # We should have at least the loop events plus State D
    assert len(result) >= 2

    # Verify that state D is preserved
    assert any(e.name == "D" for e in result)

    # Verify that all loop events are represented
    loop_names = {loop.simple_name for loop in loops}
    for name in loop_names:
        assert any(e.name == name for e in result)