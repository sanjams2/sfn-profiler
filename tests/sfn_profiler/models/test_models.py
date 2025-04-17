from datetime import datetime
from datetime import timedelta
from typing import List

import pytest
import unittest

from sfn_profiler.models import AggregateEvent
from sfn_profiler.models import Workflow, Event, Loop, ExecutionArn


class TestInit(unittest.TestCase):

    def test___contains___event_within_loop(self):
        """
        Test that the __contains__ method correctly identifies an Event that is within the Loop's time range and has a matching name.
        """
        loop_start = datetime(2023, 1, 1, 0, 0, 0)
        loop_end = datetime(2023, 1, 1, 1, 0, 0)
        loop = Loop(
            name="TestLoop",
            start=loop_start,
            end=loop_end,
            iterations=1,
            events=[],
            names={"EventA", "EventB"}
        )

        event = Event(
            start=datetime(2023, 1, 1, 0, 30, 0),
            end=datetime(2023, 1, 1, 0, 45, 0),
            name="EventA",
            workflow=ExecutionArn(account="123", region="us-west-2", state_machine="test", execution="test")
        )

        assert event in loop

    def test___contains___invalid_item_type(self):
        """
        Test that the __contains__ method raises a ValueError when given an item of invalid type.
        This tests the explicit error handling in the method for non-Event and non-AggregateEvent types.
        """
        loop = Loop(
            name="test_loop",
            start=datetime(2023, 1, 1),
            end=datetime(2023, 1, 2),
            iterations=1,
            events=[],
            names=set()
        )

        with pytest.raises(ValueError) as exc_info:
            "invalid_item" in loop

        assert str(exc_info.value) == "Invalid item type: <class 'str'>"

    def test___contains___raises_value_error_for_invalid_item_type(self):
        """
        Test that __contains__ method raises ValueError when the item is not an Event or AggregateEvent.
        """
        loop = Loop(
            name="TestLoop",
            start=datetime(2023, 1, 1),
            end=datetime(2023, 1, 2),
            iterations=1,
            events=[],
            names=set()
        )

        with self.assertRaises(ValueError) as context:
            "invalid_item" in loop

        self.assertEqual(str(context.exception), "Invalid item type: <class 'str'>")

    def test__largest_contributors_1(self):
        """
        Test the _largest_contributors method when:
        1. Some events belong to a different workflow
        2. with_loops is False, so loops are not considered
        3. There are loops present in the workflow

        Expects the method to return a sorted list of tuples (event_name, duration)
        in descending order of duration, excluding events from other workflows and loops.
        """
        # Create a workflow
        workflow_id = ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:my-state-machine:execution-1")

        # Create events
        event1 = Event(datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 5), "Event1", workflow_id)
        event2 = Event(datetime(2023, 1, 1, 10, 5), datetime(2023, 1, 1, 10, 8), "Event2", workflow_id)
        event3 = Event(datetime(2023, 1, 1, 10, 8), datetime(2023, 1, 1, 10, 10), "Event3", workflow_id)

        # Create an event from a different workflow
        other_workflow_id = ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:other-machine:execution-2")
        event4 = Event(datetime(2023, 1, 1, 10, 10), datetime(2023, 1, 1, 10, 15), "Event4", other_workflow_id)

        # Create a loop
        loop_events = [
            Event(datetime(2023, 1, 1, 10, 15), datetime(2023, 1, 1, 10, 16), "LoopEvent1", workflow_id),
            Event(datetime(2023, 1, 1, 10, 16), datetime(2023, 1, 1, 10, 17), "LoopEvent2", workflow_id)
        ]
        loop = Loop.from_stack(loop_events)

        # Create the workflow
        workflow = Workflow(workflow_id, [event1, event2, event3, event4] + loop_events, [loop])

        # Call the method under test
        result = workflow._largest_contributors(with_loops=False)

        # Assert the expected output
        expected = [
            ("Event1", 300.0),  # 5 minutes
            ("Event2", 180.0),  # 3 minutes
            ("Event3", 120.0),  # 2 minutes
            ("LoopEvent1", 60.0),  # 1 minute
            ("LoopEvent2", 60.0)   # 1 minute
        ]
        assert result == expected, f"Expected {expected}, but got {result}"

    def test__largest_contributors_2(self):
        """
        Test the _largest_contributors method when the workflow has events and loops.
        This test verifies that the method correctly calculates the largest contributors
        to the workflow duration, including both individual events and loops.
        """
        # Create a workflow with events and loops
        workflow_id = ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:my-state-machine:execution-1")
        events = [
            Event(datetime(2023, 1, 1, 0, 0), datetime(2023, 1, 1, 0, 4, 59), "Event1", workflow_id),
            Event(datetime(2023, 1, 1, 0, 5), datetime(2023, 1, 1, 0, 8), "Event2", workflow_id),
            Event(datetime(2023, 1, 1, 0, 8), datetime(2023, 1, 1, 0, 10), "Event3", workflow_id),
        ]
        loop = Loop("LoopEvent", datetime(2023, 1, 1, 0, 10), datetime(2023, 1, 1, 0, 15), 2, 
                    [Event(datetime(2023, 1, 1, 0, 10), datetime(2023, 1, 1, 0, 12), "LoopEvent", workflow_id),
                     Event(datetime(2023, 1, 1, 0, 12), datetime(2023, 1, 1, 0, 15), "LoopEvent", workflow_id)],
                    {"LoopEvent"})
        workflow = Workflow(workflow_id, events, [loop])

        # Call the method under test
        result = workflow._largest_contributors(with_loops=True)

        # Assert the expected results
        expected = [
            ("[LOOP] LoopEvent", 300.0),
            ("Event1", 299.0),
            ("Event2", 180.0),
            ("Event3", 120.0)
        ]
        assert result == expected, f"Expected {expected}, but got {result}"

    def test_add_event_1(self):
        """
        Test that add_event correctly updates the AggregateEvent's properties when a new Event is added.
        """
        # Create an AggregateEvent
        start = datetime(2023, 1, 1, 10, 0, 0)
        end = datetime(2023, 1, 1, 11, 0, 0)
        aggregate_event = AggregateEvent(
            start=start,
            end=end,
            name="TestAggregate",
            values=[],
            contributors=set()
        )

        # Create an Event to add
        event_start = datetime(2023, 1, 1, 9, 30, 0)
        event_end = datetime(2023, 1, 1, 11, 30, 0)
        workflow = ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:MyStateMachine:MyExecution")
        event = Event(
            start=event_start,
            end=event_end,
            name="TestEvent",
            workflow=workflow
        )

        # Add the event to the AggregateEvent
        aggregate_event.add_event(event)

        # Assert that the properties were updated correctly
        assert aggregate_event.start == event_start
        assert aggregate_event.end == event_end
        assert aggregate_event.values == [event.duration]
        assert aggregate_event.contributors == {workflow}

    def test_add_event_update_start_and_end(self):
        """
        Test that add_event updates the start and end times of the AggregateEvent
        when the new event's times are outside the current range.
        """
        aggregate = AggregateEvent(
            start=datetime(2023, 1, 1, 12, 0),
            end=datetime(2023, 1, 1, 13, 0),
            name="TestAggregate",
            values=[],
            contributors=set()
        )

        earlier_event = Event(
            start=datetime(2023, 1, 1, 11, 0),
            end=datetime(2023, 1, 1, 11, 30),
            name="EarlierEvent",
            workflow=ExecutionArn(account="123", region="us-west-2", state_machine="test", execution="earlier")
        )

        later_event = Event(
            start=datetime(2023, 1, 1, 13, 30),
            end=datetime(2023, 1, 1, 14, 0),
            name="LaterEvent",
            workflow=ExecutionArn(account="123", region="us-west-2", state_machine="test", execution="later")
        )

        aggregate.add_event(earlier_event)
        assert aggregate.start == earlier_event.start
        assert aggregate.end == datetime(2023, 1, 1, 13, 0)

        aggregate.add_event(later_event)
        assert aggregate.start == earlier_event.start
        assert aggregate.end == later_event.end

    def test_add_events_1(self):
        """
        Test that add_events correctly extends the events list and updates the start and end times.
        """
        # Create a workflow with initial events
        initial_events = [
            Event(datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 11, 0), "Event1", ExecutionArn("123", "us-west-2", "machine1", "exec1")),
            Event(datetime(2023, 1, 1, 12, 0), datetime(2023, 1, 1, 13, 0), "Event2", ExecutionArn("123", "us-west-2", "machine1", "exec1"))
        ]
        workflow = Workflow("test_workflow", initial_events, [])

        # Create new events to add
        new_events = [
            Event(datetime(2023, 1, 1, 9, 0), datetime(2023, 1, 1, 9, 30), "Event3", ExecutionArn("123", "us-west-2", "machine1", "exec1")),
            Event(datetime(2023, 1, 1, 14, 0), datetime(2023, 1, 1, 15, 0), "Event4", ExecutionArn("123", "us-west-2", "machine1", "exec1"))
        ]

        # Call the method under test
        workflow.add_events(new_events)

        # Assert that the events list has been extended
        assert len(workflow.events) == 4

        # Assert that the start time has been updated
        assert workflow.start == datetime(2023, 1, 1, 9, 0)

        # Assert that the end time has been updated
        assert workflow.end == datetime(2023, 1, 1, 15, 0)

    def test_add_events_empty_list(self):
        """
        Test the add_events method with an empty list of events.
        This edge case is implicitly handled by the method, as it doesn't throw an exception
        for an empty list and the min/max operations will not be performed.
        """
        workflow = Workflow(id="test", events=[], loops=[])
        initial_start = workflow._start
        initial_end = workflow._end

        workflow.add_events([])

        assert workflow._start == initial_start
        assert workflow._end == initial_end
        assert len(workflow.events) == 0

    def test_duration_returns_correct_timedelta(self):
        """
        Test that the duration property of Event returns the correct timedelta
        between the start and end times.
        """
        start = datetime(2023, 1, 1, 12, 0, 0)
        end = datetime(2023, 1, 1, 12, 30, 0)
        workflow = ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:my-state-machine:execution-1")
        event = Event(start=start, end=end, name="TestEvent", workflow=workflow)

        expected_duration = timedelta(minutes=30)
        assert event.duration == expected_duration

    def test_duration_returns_correct_timedelta_2(self):
        """
        Test that the duration property of Loop class returns the correct timedelta.

        This test verifies that the duration property correctly calculates and returns
        the time difference between the end and start times of the Loop instance.
        """
        start = datetime(2023, 1, 1, 12, 0, 0)
        end = datetime(2023, 1, 1, 12, 30, 0)
        loop = Loop(name="test_loop", start=start, end=end, iterations=1, events=[], names=set())

        expected_duration = timedelta(minutes=30)
        assert loop.duration == expected_duration

    def test_duration_returns_correct_timedelta_3(self):
        """
        Test that the duration property of Workflow returns the correct timedelta.
        It should return the difference between the end and start times of the workflow.
        """
        start_time = datetime(2023, 1, 1, 0, 0, 0)
        end_time = datetime(2023, 1, 1, 1, 30, 0)

        execution_arn = ExecutionArn("123456789012", "us-west-2", "MyStateMachine", "MyExecution")
        event = Event(start_time, end_time, "TestEvent", execution_arn)

        workflow = Workflow("test_workflow", [event], [])

        expected_duration = timedelta(hours=1, minutes=30)
        assert workflow.duration == expected_duration

    def test_duration_returns_difference_between_end_and_start(self):
        """
        Test that the duration property returns the difference between end and start times.
        """
        start = datetime(2023, 1, 1, 12, 0, 0)
        end = datetime(2023, 1, 1, 12, 30, 0)
        event = AggregateEvent(start=start, end=end, name="Test Event", values=[], contributors=set())

        expected_duration = timedelta(minutes=30)
        assert event.duration == expected_duration

    def test_duration_with_end_before_start(self):
        """
        Test the duration property when the end time is before the start time.
        This should result in a negative timedelta.
        """
        workflow = ExecutionArn.parse("arn:aws:states:us-east-1:123456789012:execution:my-state-machine:execution-id")
        event = Event(
            start=datetime(2023, 1, 1, 12, 0, 0),
            end=datetime(2023, 1, 1, 11, 59, 59),
            name="TestEvent",
            workflow=workflow
        )
        expected_duration = timedelta(seconds=-1)
        assert event.duration == expected_duration, f"Expected duration {expected_duration}, but got {event.duration}"

    def test_duration_with_end_before_start_2(self):
        """
        Test the duration property when the end time is before the start time.
        This should result in a negative timedelta.
        """
        start = datetime(2023, 1, 1, 12, 0, 0)
        end = datetime(2023, 1, 1, 11, 59, 59)
        event = AggregateEvent(start=start, end=end, name="Test", values=[], contributors=set())

        assert event.duration == timedelta(seconds=-1)

    def test_duration_with_identical_start_end(self):
        """
        Test the duration property when start and end times are identical.
        This is an edge case where the duration should be zero.
        """
        now = datetime.now()
        loop = Loop(name="test", start=now, end=now, iterations=1, events=[], names=set())
        assert loop.duration == timedelta(0), "Duration should be zero when start and end times are identical"

    def test_duration_with_no_events(self):
        """
        Test the duration property when the Workflow has no events.
        This is an edge case where the start and end times are not set.
        """
        workflow = Workflow(id="test", events=[], loops=[])
        assert workflow.duration == timedelta(0)


    def test_duration_with_single_event(self):
        """
        Test the duration property when the Workflow has only one event.
        This is an edge case where the start and end times are the same.
        """
        execution_arn = ExecutionArn(account="123456789012", region="us-west-2", state_machine="test", execution="test")
        event = Event(start=datetime(2023, 1, 1), end=datetime(2023, 1, 1), name="TestEvent", workflow=execution_arn)
        workflow = Workflow(id="test", events=[event], loops=[])

        assert workflow.duration == timedelta(0), "Duration should be zero for a single instantaneous event"

    def test_durations_empty_values(self):
        """
        Test the durations method when the values list is empty.
        This is an edge case that is implicitly handled by the list comprehension.
        """
        event = AggregateEvent(
            start=None,
            end=None,
            name="test",
            values=[],
            contributors=set()
        )
        result = event.durations()
        assert isinstance(result, List)
        assert len(result) == 0

    def test_durations_returns_list_of_seconds(self):
        """
        Test that the durations method returns a list of float values
        representing the total seconds of each timedelta in self.values.
        """
        # Arrange
        aggregate_event = AggregateEvent(
            start=None,
            end=None,
            name="test_event",
            values=[timedelta(seconds=10), timedelta(minutes=1), timedelta(hours=1)],
            contributors=set()
        )

        # Act
        result = aggregate_event.durations()

        # Assert
        assert isinstance(result, List)
        assert all(isinstance(duration, float) for duration in result)
        assert result == [10.0, 60.0, 3600.0]

    def test_end_1(self):
        """
        Test that the end property of Workflow returns the correct end datetime.

        This test creates a Workflow instance with two events and verifies that
        the end property returns the latest end datetime among the events.
        """
        # Create test data
        arn = ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:state-machine:execution-1234")
        event1 = Event(start=datetime(2023, 1, 1, 10, 0), end=datetime(2023, 1, 1, 11, 0), name="Event1", workflow=arn)
        event2 = Event(start=datetime(2023, 1, 1, 10, 30), end=datetime(2023, 1, 1, 11, 30), name="Event2", workflow=arn)

        # Create Workflow instance
        workflow = Workflow(id="test-workflow", events=[event1, event2], loops=[])

        # Assert that the end property returns the correct datetime
        assert workflow.end == datetime(2023, 1, 1, 11, 30)

    def test_end_when_no_events(self):
        """
        Test the end property when the Workflow has no events.
        This is an edge case where the Workflow is initialized with an empty list of events.
        """
        workflow = Workflow(id="test_workflow", events=[], loops=[])
        assert workflow.end is None

    def test_end_when_single_event(self):
        """
        Test the end property when the Workflow has only one event.
        This is an edge case where the Workflow is initialized with a single event.
        """
        event = Event(
            start=datetime(2023, 1, 1, 0, 0),
            end=datetime(2023, 1, 1, 1, 0),
            name="test_event",
            workflow=ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:test-state-machine:test-execution")
        )
        workflow = Workflow(id="test_workflow", events=[event], loops=[])
        assert workflow.end == datetime(2023, 1, 1, 1, 0)

    def test_from_stack_1(self):
        """
        Test the from_stack method of the Loop class.

        This test verifies that the from_stack method correctly creates a Loop object
        from a list of Event objects. It checks if the created Loop has the correct
        name, start time, end time, number of iterations, events, and names.
        """
        # Create test data
        workflow = ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:my-state-machine:execution-1")
        events = [
            Event(start=datetime(2023, 1, 1, 10, 0), end=datetime(2023, 1, 1, 10, 1), name="Event1", workflow=workflow),
            Event(start=datetime(2023, 1, 1, 10, 1), end=datetime(2023, 1, 1, 10, 2), name="Event2", workflow=workflow),
            Event(start=datetime(2023, 1, 1, 10, 2), end=datetime(2023, 1, 1, 10, 3), name="Event1", workflow=workflow)
        ]

        # Call the method under test
        loop = Loop.from_stack(events)

        # Assert the results
        assert loop.simple_name == "Event1|Event2" or loop.simple_name == "Event2|Event1"
        assert loop.start == datetime(2023, 1, 1, 10, 0)
        assert loop.end == datetime(2023, 1, 1, 10, 3)
        assert loop.iterations == 2
        assert loop.events == events
        assert loop.names == {"Event1", "Event2"}

    def test_from_stack_empty_list(self):
        """
        Test the from_stack method with an empty list input.
        This should raise an IndexError as the method tries to access stack[0] and stack[-1].
        """
        with self.assertRaises(IndexError):
            Loop.from_stack([])

    def test_from_stack_single_event(self):
        """
        Test the from_stack method with a single event in the stack.
        This is an edge case where the loop would have only one iteration.
        """
        event = Event(
            start=datetime(2023, 1, 1),
            end=datetime(2023, 1, 2),
            name="SingleEvent",
            workflow=ExecutionArn(account="123", region="us-west-2", state_machine="test", execution="test")
        )
        loop = Loop.from_stack([event])
        assert loop.iterations == 1
        assert loop.name == "SingleEvent"
        assert len(loop.events) == 1
        assert len(loop.names) == 1

    def test_id_as_filename_1(self):
        """
        Test that id_as_filename correctly replaces ':' and '/' with '-' in the workflow ID.
        """
        # Create a sample Workflow instance
        execution_arn = ExecutionArn(account="123456789012", region="us-west-2", state_machine="MyStateMachine", execution="MyExecution")
        sample_event = Event(start=datetime.now(), end=datetime.now() + timedelta(seconds=10), name="SampleEvent", workflow=execution_arn)
        workflow = Workflow(id="arn:aws:states:us-west-2:123456789012:execution:MyStateMachine:MyExecution", events=[sample_event], loops=[])

        # Call the method under test
        result = workflow.id_as_filename()

        # Assert the expected result
        expected = "arn-aws-states-us-west-2-123456789012-execution-MyStateMachine-MyExecution"
        assert result == expected, f"Expected {expected}, but got {result}"

    def test_id_as_filename_with_special_characters(self):
        """
        Test the id_as_filename method with an id containing special characters.
        This test verifies that ':' and '/' are replaced with '-' in the output.
        """
        workflow = Workflow(
            id="arn:aws:states:us-east-1:123456789012:execution:StateMachine-1234567890abcdef:execution-1234567890abcdef",
            events=[],
            loops=[],
            _start=datetime.now(),
            _end=datetime.now()
        )

        result = workflow.id_as_filename()

        assert ':' not in result
        assert '/' not in result
        assert result == "arn-aws-states-us-east-1-123456789012-execution-StateMachine-1234567890abcdef-execution-1234567890abcdef"

    def test_largest_contributors_1(self):
        """
        Test the largest_contributors method of the Workflow class.

        This test verifies that the method correctly returns the n largest contributors
        to the workflow duration, with and without including loops.
        """
        # Create a sample workflow
        workflow_id = ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:MyStateMachine:MyExecution")

        # Create sample events
        event1 = Event(start=datetime(2023, 1, 1, 0, 0), end=datetime(2023, 1, 1, 0, 5), name="Event1", workflow=workflow_id)
        event2 = Event(start=datetime(2023, 1, 1, 0, 5), end=datetime(2023, 1, 1, 0, 8), name="Event2", workflow=workflow_id)
        event3 = Event(start=datetime(2023, 1, 1, 0, 8), end=datetime(2023, 1, 1, 0, 10), name="Event3", workflow=workflow_id)

        # Create a sample loop
        loop_events = [
            Event(start=datetime(2023, 1, 1, 0, 10), end=datetime(2023, 1, 1, 0, 11), name="LoopEvent1", workflow=workflow_id),
            Event(start=datetime(2023, 1, 1, 0, 11), end=datetime(2023, 1, 1, 0, 12), name="LoopEvent2", workflow=workflow_id),
            Event(start=datetime(2023, 1, 1, 0, 12), end=datetime(2023, 1, 1, 0, 12, 30), name="LoopEvent1", workflow=workflow_id),
        ]
        loop = Loop.from_stack(loop_events)

        # Create the workflow
        workflow = Workflow(id=workflow_id, events=[event1, event2, event3] + loop_events, loops=[loop])

        # Test without loops
        result_without_loops = workflow.largest_contributors(n=2, with_loops=False)
        assert len(result_without_loops) == 2
        assert result_without_loops[0][0] == "Event1"
        assert result_without_loops[0][1] == 300.0  # 5 minutes in seconds
        assert result_without_loops[1][0] == "Event2"
        assert result_without_loops[1][1] == 180.0  # 3 minutes in seconds

        # Test with loops
        result_with_loops = workflow.largest_contributors(n=3, with_loops=True)
        assert len(result_with_loops) == 3
        assert result_with_loops[0][0] == "Event1"
        assert result_with_loops[0][1] == 300.0  # 5 minutes in seconds
        assert result_with_loops[1][0] == "Event2"
        assert result_with_loops[1][1] == 180.0  # 3 minutes in seconds
        contributor_name = result_with_loops[2][0]
        assert contributor_name.startswith("[LOOP]") and "LoopEvent1" in contributor_name and "LoopEvent2" in contributor_name
        assert result_with_loops[2][1] == 150.0  # 2 minutes in seconds

    def test_largest_contributors_all_events_in_loops(self):
        execution_arn = ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:state-machine:execution")
        event1 = Event(start=datetime(2023, 1, 1), end=datetime(2023, 1, 2), name="Event1", workflow=execution_arn)
        event2 = Event(start=datetime(2023, 1, 2), end=datetime(2023, 1, 2, 12), name="Event2", workflow=execution_arn)
        loop = Loop(name="TestLoop", start=datetime(2023, 1, 1), end=datetime(2023, 1, 2, 12),
                    iterations=2, events=[event1, event2], names={"Event1", "Event2"})
        workflow = Workflow(id=execution_arn, events=[event1, event2], loops=[loop])

        result = workflow._largest_contributors(with_loops=False)
        assert len(result) == 2, "Expected 2 contributors"
        assert result[0][0] == "Event1", "Expected Event1 is first"
        assert result[0][1] == 24*60*60
        assert result[1][0] == "Event2", "Expected Event2 is first"
        assert result[1][1] == 12*60*60

        result = workflow._largest_contributors(with_loops=True)
        assert len(result) == 1, "Expected 1 contributors (only the loop)"
        assert result[0][0].startswith("[LOOP]"), "Expected Event1 to be loop"
        assert result[0][1] == 36*60*60

    def test_largest_contributors_empty_events(self):
        """
        Test the _largest_contributors method when the Workflow has no events.
        This is an edge case where the method should return an empty list.
        """
        workflow = Workflow(id="test_workflow", events=[], loops=[])
        result = workflow._largest_contributors()
        assert result == [], "Expected an empty list for a Workflow with no events"

    def test_largest_contributors_with_negative_n(self):
        """
        Test the largest_contributors method with a negative value for n.
        This should return an empty list as the method slices the result with [:n].
        """
        workflow = Workflow(
            id="test_workflow",
            events=[
                Event(
                    start=datetime(2023, 1, 1),
                    end=datetime(2023, 1, 1, 0, 1),
                    name="Event1",
                    workflow=ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:StateMachine:execution")
                )
            ],
            loops=[]
        )

        result = workflow.largest_contributors(n=-1)
        assert result == [], "Expected an empty list when n is negative"

    def test_largest_contributors_with_zero_n(self):
        """
        Test the largest_contributors method with n set to zero.
        This should return an empty list as the method slices the result with [:n].
        """
        workflow = Workflow(
            id="test_workflow",
            events=[
                Event(
                    start=datetime(2023, 1, 1),
                    end=datetime(2023, 1, 1, 0, 1),
                    name="Event1",
                    workflow=ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:StateMachine:execution")
                )
            ],
            loops=[]
        )

        result = workflow.largest_contributors(n=0)
        assert result == [], "Expected an empty list when n is zero"

    def test_parse_empty_arn(self):
        """
        Test that parse method raises ValueError when given an empty ARN string.
        """
        empty_arn = ""
        with pytest.raises(ValueError) as exc_info:
            ExecutionArn.parse(empty_arn)
        assert str(exc_info.value) == f"Invalid ARN: {empty_arn}"

    def test_parse_invalid_arn_format(self):
        """
        Test that parse method raises ValueError when given an ARN with incorrect number of parts.
        """
        invalid_arn = "arn:aws:states:us-west-2:123456789012:execution:my-state-machine"
        with pytest.raises(ValueError) as exc_info:
            ExecutionArn.parse(invalid_arn)
        assert str(exc_info.value) == f"Invalid ARN: {invalid_arn}"

    def test_parse_valid_arn(self):
        """
        Test parsing a valid ARN string into an ExecutionArn object.
        Verifies that the parse method correctly extracts account, region, state machine, and execution components.
        """
        arn = "arn:aws:states:us-west-2:123456789012:execution:my-state-machine:my-execution"
        result = ExecutionArn.parse(arn)
        assert isinstance(result, ExecutionArn)
        assert result.account == "123456789012"
        assert result.region == "us-west-2"
        assert result.state_machine == "my-state-machine"
        assert result.execution == "my-execution"

    def test_parse_valid_arn_2(self):
        """
        Testcase 2 for def parse(arn: str):
        Test parsing a valid ARN with exactly 8 parts.
        """
        arn = "arn:aws:states:us-west-2:123456789012:execution:my-state-machine:my-execution"
        result = ExecutionArn.parse(arn)
        assert isinstance(result, ExecutionArn)
        assert result.account == "123456789012"
        assert result.region == "us-west-2"
        assert result.state_machine == "my-state-machine"
        assert result.execution == "my-execution"

    def test_simple_name_1(self):
        """
        Test that the simple_name property correctly joins the names in the Loop object.
        This test verifies that the simple_name method returns a string that is the result of
        joining the set of names in the Loop object with the '|' character.
        """
        # Create a sample Loop object
        loop = Loop(
            name="TestLoop",
            start=datetime(2023, 1, 1),
            end=datetime(2023, 1, 2),
            iterations=2,
            events=[
                Event(
                    start=datetime(2023, 1, 1),
                    end=datetime(2023, 1, 1, 1),
                    name="Event1",
                    workflow=ExecutionArn("123456789012", "us-west-2", "MyStateMachine", "MyExecution")
                ),
                Event(
                    start=datetime(2023, 1, 1, 1),
                    end=datetime(2023, 1, 2),
                    name="Event2",
                    workflow=ExecutionArn("123456789012", "us-west-2", "MyStateMachine", "MyExecution")
                )
            ],
            names={"Event1", "Event2"}
        )

        # Call the simple_name property
        result = loop.simple_name

        # Assert that the result is the expected joined string
        assert result == "Event1|Event2" or result == "Event2|Event1"

    def test_simple_name_with_empty_names_set(self):
        """
        Test the simple_name property when the names set is empty.
        This tests the edge case of an empty set, which is handled by the '|'.join() method.
        """
        loop = Loop(
            name="empty_loop",
            start=datetime.now(),
            end=datetime.now() + timedelta(seconds=10),
            iterations=0,
            events=[],
            names=set()
        )
        assert loop.simple_name == ""

    def test_start_returns_correct_start_time(self):
        """
        Test that the start property of Workflow returns the correct start time.
        This test verifies that the start property correctly returns the _start attribute,
        which is set to the earliest start time of all events in the workflow.
        """
        # Create test data
        arn = ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:my-state-machine:execution-1")
        event1 = Event(start=datetime(2023, 1, 1, 10, 0), end=datetime(2023, 1, 1, 11, 0), name="Event1", workflow=arn)
        event2 = Event(start=datetime(2023, 1, 1, 9, 0), end=datetime(2023, 1, 1, 10, 30), name="Event2", workflow=arn)

        # Create Workflow instance
        workflow = Workflow(id="test-workflow", events=[event1, event2], loops=[])

        # Assert that the start property returns the correct start time
        assert workflow.start == datetime(2023, 1, 1, 9, 0)

    def test_start_with_no_events(self):
        """
        Test the start property when the Workflow has no events.
        This is an edge case where the Workflow is initialized with an empty list of events.
        """
        workflow = Workflow(id="test_workflow", events=[], loops=[])
        assert workflow.start is None

    def test_start_with_single_event(self):
        """
        Test the start property when the Workflow has only one event.
        This is an edge case where the Workflow is initialized with a single event.
        """
        event = Event(
            start=datetime(2023, 1, 1),
            end=datetime(2023, 1, 2),
            name="test_event",
            workflow=ExecutionArn(account="123", region="us-west-2", state_machine="test", execution="test")
        )
        workflow = Workflow(id="test_workflow", events=[event], loops=[])
        assert workflow.start == datetime(2023, 1, 1)

    def test_to_event_creates_event_with_correct_attributes(self):
        """
        Test that the to_event method of Loop creates an Event object
        with the correct start, end, name, and workflow attributes.
        """
        # Create test data
        start = datetime(2023, 1, 1, 12, 0, 0)
        end = datetime(2023, 1, 1, 12, 30, 0)
        workflow = ExecutionArn(account="123456789012", region="us-west-2", state_machine="test-machine", execution="test-execution")
        event = Event(start=start, end=end, name="TestEvent", workflow=workflow)
        loop = Loop(name="TestLoop", start=start, end=end, iterations=1, events=[event], names={"TestEvent"})

        # Call the method under test
        result = loop.to_event()

        # Assert the result
        assert isinstance(result, Event)
        assert result.start == start
        assert result.end == end
        assert result.name == loop.simple_name
        assert result.workflow == workflow

    def test_to_event_empty_events_list(self):
        """
        Test the to_event method when the Loop object has an empty events list.
        This tests the edge case where the Loop is initialized with no events,
        which could lead to an IndexError when accessing the first event's workflow.
        """
        loop = Loop(
            name="EmptyLoop",
            start=datetime(2023, 1, 1),
            end=datetime(2023, 1, 2),
            iterations=0,
            events=[],
            names=set()
        )

        try:
            loop.to_event()
        except IndexError as e:
            assert str(e) == "list index out of range"
        else:
            assert False, "Expected IndexError was not raised"

    def test_total_minutes_1(self):
        """
        Test that the total_minutes method correctly calculates the duration in minutes.
        It should return the total seconds divided by 60.
        """
        execution_arn = ExecutionArn(account="123456789012", region="us-west-2", state_machine="test-state-machine", execution="test-execution")
        start_time = datetime(2023, 1, 1, 0, 0, 0)
        end_time = datetime(2023, 1, 1, 0, 5, 0)  # 5 minutes later
        event = Event(start=start_time, end=end_time, name="TestEvent", workflow=execution_arn)
        workflow = Workflow(id="test-workflow", events=[event], loops=[])

        expected_minutes = 5.0
        actual_minutes = workflow.total_minutes()

        assert actual_minutes == expected_minutes, f"Expected {expected_minutes} minutes, but got {actual_minutes} minutes"

    def test_total_seconds_1(self):
        """
        Test that the total_seconds method correctly returns the duration in seconds.
        """
        start = datetime(2023, 1, 1, 0, 0, 0)
        end = datetime(2023, 1, 1, 0, 0, 30)
        workflow = ExecutionArn(account="123456789012", region="us-west-2", state_machine="test_machine", execution="test_execution")
        event = Event(start=start, end=end, name="TestEvent", workflow=workflow)

        expected_seconds = 30.0
        actual_seconds = event.total_seconds()

        assert actual_seconds == expected_seconds, f"Expected {expected_seconds} seconds, but got {actual_seconds} seconds"

    def test_total_seconds_1_2(self):
        """
        Test that the total_seconds method of Loop class correctly calculates
        the total duration in seconds between the start and end times.
        """
        start_time = datetime(2023, 1, 1, 12, 0, 0)
        end_time = datetime(2023, 1, 1, 12, 5, 30)
        loop = Loop(
            name="TestLoop",
            start=start_time,
            end=end_time,
            iterations=1,
            events=[
                Event(
                    start=start_time,
                    end=end_time,
                    name="TestEvent",
                    workflow=ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:TestStateMachine:TestExecution")
                )
            ],
            names={"TestEvent"}
        )

        expected_seconds = 330  # 5 minutes and 30 seconds
        assert loop.total_seconds() == expected_seconds

    def test_total_seconds_negative_duration(self):
        """
        Test the total_seconds method with a negative duration.
        This edge case is handled implicitly by the timedelta.total_seconds() method.
        """
        execution_arn = ExecutionArn(account="123456789012", region="us-west-2", state_machine="test-state-machine", execution="test-execution")
        end_time = datetime(2023, 1, 1, 12, 0, 0)
        start_time = datetime(2023, 1, 1, 12, 0, 1)  # Start time is after end time
        event = Event(start=start_time, end=end_time, name="TestEvent", workflow=execution_arn)

        result = event.total_seconds()

        assert result == -1.0, f"Expected -1.0, but got {result}"

    def test_total_seconds_no_negative_scenarios(self):
        """
        This test method verifies that there are no negative scenarios to test for the total_seconds method.
        The total_seconds method simply calls the total_seconds method of the duration property,
        which is a timedelta object. There are no explicit edge cases or error conditions handled
        in the implementation of this method.
        """
        # Create a sample AggregateEvent
        start = datetime(2023, 1, 1, 0, 0, 0)
        end = datetime(2023, 1, 1, 0, 0, 10)
        event = AggregateEvent(start, end, "Test Event", [], set())

        # Call the total_seconds method
        result = event.total_seconds()

        # Assert that the result is as expected (10 seconds)
        assert result == 10.0, "Expected 10.0 seconds, but got {result}"

    def test_total_seconds_returns_correct_duration(self):
        """
        Test that the total_seconds method of Workflow returns the correct duration in seconds.
        This test creates a Workflow with a single Event and verifies that the total_seconds
        method returns the expected duration.
        """
        start_time = datetime(2023, 1, 1, 0, 0, 0)
        end_time = datetime(2023, 1, 1, 0, 1, 30)
        arn = ExecutionArn.parse("arn:aws:states:us-west-2:123456789012:execution:my-state-machine:execution-id")
        event = Event(start=start_time, end=end_time, name="TestEvent", workflow=arn)
        workflow = Workflow(id="test-workflow", events=[event], loops=[])

        expected_seconds = 90.0
        assert workflow.total_seconds() == expected_seconds

    def test_total_seconds_returns_duration_in_seconds(self):
        """
        Test that the total_seconds method of AggregateEvent returns the correct duration in seconds.
        """
        start = datetime(2023, 1, 1, 0, 0, 0)
        end = datetime(2023, 1, 1, 0, 1, 30)
        event = AggregateEvent(
            start=start,
            end=end,
            name="Test Event",
            values=[],
            contributors=set(),
            attempts=1
        )

        result = event.total_seconds()

        assert result == 90.0, f"Expected 90.0 seconds, but got {result}"

    def test_total_seconds_with_negative_duration(self):
        """
        Test the total_seconds method of Loop class when the duration is negative.
        This is an edge case where the end time is before the start time.
        """
        start_time = datetime(2023, 1, 1, 12, 0, 0)
        end_time = start_time - timedelta(seconds=10)
        loop = Loop(
            name="test_loop",
            start=start_time,
            end=end_time,
            iterations=1,
            events=[],
            names=set()
        )
        assert loop.total_seconds() == -10.0

    def test_total_seconds_with_negative_duration_2(self):
        """
        Test the total_seconds method with a Workflow that has a negative duration.
        This tests the edge case where the end time is before the start time.
        """
        start_time = datetime(2023, 1, 1, 0, 0, 0)
        end_time = start_time - timedelta(seconds=1)
        workflow = Workflow(id="test", events=[], loops=[])
        workflow._start = start_time
        workflow._end = end_time

        result = workflow.total_seconds()

        assert result < 0, f"Expected negative seconds, but got {result}"

    def test_total_seconds_with_zero_duration(self):
        """
        Test the total_seconds method of Loop class when the duration is zero.
        This is an edge case where the start and end times are the same.
        """
        start_time = datetime(2023, 1, 1, 12, 0, 0)
        end_time = start_time
        loop = Loop(
            name="test_loop",
            start=start_time,
            end=end_time,
            iterations=1,
            events=[],
            names=set()
        )
        assert loop.total_seconds() == 0.0

    def test_total_seconds_with_zero_duration_2(self):
        """
        Test the total_seconds method with a Workflow that has zero duration.
        This tests the edge case where start and end times are the same.
        """
        start_time = datetime(2023, 1, 1, 0, 0, 0)
        end_time = start_time
        workflow = Workflow(id="test", events=[], loops=[])
        workflow._start = start_time
        workflow._end = end_time

        result = workflow.total_seconds()

        assert result == 0.0, f"Expected 0.0 seconds, but got {result}"

    def test_workflow_return_value(self):
        """
        Test that the workflow property of AggregateEvent always returns the string "AGGREGATE".
        This is a negative test in the sense that it verifies the method doesn't return
        any other value, which would be incorrect based on the implementation.
        """
        event = AggregateEvent(
            start=datetime.now(),
            end=datetime.now() + timedelta(seconds=10),
            name="TestEvent",
            values=[timedelta(seconds=5)],
            contributors={ExecutionArn("123456789012", "us-west-2", "StateMachine", "Execution")}
        )
        assert event.workflow == "AGGREGATE", "workflow property should always return 'AGGREGATE'"

    def test_workflow_returns_aggregate(self):
        """
        Test that the workflow property of AggregateEvent returns "AGGREGATE"
        """
        event = AggregateEvent(
            start=datetime(2023, 1, 1),
            end=datetime(2023, 1, 2),
            name="Test Event",
            values=[timedelta(hours=1)],
            contributors=set()
        )
        assert event.workflow == "AGGREGATE"
