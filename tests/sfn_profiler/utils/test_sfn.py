from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from sfn_profiler.models import ExecutionArn
from sfn_profiler.utils.sfn import get_execution_arn, process_execution_history


class TestGetExecutionArn:

    def test_full_arn(self):
        """Test parsing a full AWS ARN."""
        arn_str = "arn:aws:states:us-east-1:123456789012:execution:my-state-machine:my-execution"
        result = get_execution_arn(arn_str)

        assert isinstance(result, ExecutionArn)
        assert result.account == "123456789012"
        assert result.region == "us-east-1"
        assert result.state_machine == "my-state-machine"
        assert result.execution == "my-execution"

    @patch('sfn_profiler.utils.sfn.get_account', return_value='987654321098')
    @patch('sfn_profiler.utils.sfn.get_region', return_value='us-west-2')
    def test_short_format(self, mock_region, mock_account):
        """Test parsing a shortened 'state_machine:execution' format."""
        short_id = "my-state-machine:my-execution"
        result = get_execution_arn(short_id)

        assert isinstance(result, ExecutionArn)
        assert result.account == "987654321098"
        assert result.region == "us-west-2"
        assert result.state_machine == "my-state-machine"
        assert result.execution == "my-execution"

        # Verify mocks were called
        mock_account.assert_called_once()
        mock_region.assert_called_once()

    def test_invalid_format(self):
        """Test that invalid formats raise a ValueError."""
        invalid_id = "invalid-format-without-colon"

        with pytest.raises(ValueError) as excinfo:
            get_execution_arn(invalid_id)

        assert "Invalid execution id" in str(excinfo.value)


class TestProcessExecutionHistory:

    @pytest.fixture
    def sample_workflow(self):
        """Create a sample ExecutionArn for testing."""
        return ExecutionArn(
            account="123456789012",
            region="us-east-1",
            state_machine="test-machine",
            execution="test-execution"
        )

    @pytest.fixture
    def simple_history(self):
        """Create a simple execution history with no failures."""
        return [
            {
                "type": "TaskStateEntered",
                "stateEnteredEventDetails": {"name": "State1"},
                "timestamp": datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
            },
            {
                "type": "StateExited",
                "stateExitedEventDetails": {"name": "State1"},
                "timestamp": datetime(2023, 1, 1, 10, 1, 0, tzinfo=timezone.utc)
            },
            {
                "type": "TaskStateEntered",
                "stateEnteredEventDetails": {"name": "State2"},
                "timestamp": datetime(2023, 1, 1, 10, 1, 30, tzinfo=timezone.utc)
            },
            {
                "type": "StateExited",
                "stateExitedEventDetails": {"name": "State2"},
                "timestamp": datetime(2023, 1, 1, 10, 2, 0, tzinfo=timezone.utc)
            }
        ]

    @pytest.fixture
    def history_with_retries(self):
        """Create an execution history with task failures and retries."""
        return [
            # First state (successful)
            {
                "type": "TaskStateEntered",
                "stateEnteredEventDetails": {"name": "SuccessState"},
                "timestamp": datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
            },
            {
                "type": "TaskStateExited",
                "stateExitedEventDetails": {"name": "SuccessState"},
                "timestamp": datetime(2023, 1, 1, 10, 1, 0, tzinfo=timezone.utc)
            },

            # Second state with retries
            {
                "type": "TaskStateEntered",
                "stateEnteredEventDetails": {"name": "RetryState"},
                "timestamp": datetime(2023, 1, 1, 10, 1, 30, tzinfo=timezone.utc)
            },
            {
                "type": "TaskScheduled",
                "timestamp": datetime(2023, 1, 1, 10, 1, 35, tzinfo=timezone.utc)
            },
            {
                "type": "TaskFailed",
                "timestamp": datetime(2023, 1, 1, 10, 1, 45, tzinfo=timezone.utc)
            },
            {
                "type": "TaskScheduled", # Retry
                "timestamp": datetime(2023, 1, 1, 10, 2, 0, tzinfo=timezone.utc)
            },
            {
                "type": "TaskFailed",
                "timestamp": datetime(2023, 1, 1, 10, 2, 15, tzinfo=timezone.utc)
            },
            {
                "type": "TaskScheduled", # Second retry
                "timestamp": datetime(2023, 1, 1, 10, 2, 30, tzinfo=timezone.utc)
            },
            {
                "type": "TaskSucceeded",
                "timestamp": datetime(2023, 1, 1, 10, 3, 0, tzinfo=timezone.utc)
            },
            {
                "type": "TaskStateExited",
                "stateExitedEventDetails": {"name": "RetryState"},
                "timestamp": datetime(2023, 1, 1, 10, 3, 5, tzinfo=timezone.utc)
            }
        ]

    @pytest.fixture
    def history_with_task_failure_then_exit(self):
        """Create a history where a task fails and the next event is StateExited (no retry)."""
        return [
            {
                "type": "TaskStateEntered",
                "stateEnteredEventDetails": {"name": "FailState"},
                "timestamp": datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
            },
            {
                "type": "TaskScheduled",
                "timestamp": datetime(2023, 1, 1, 10, 0, 5, tzinfo=timezone.utc)
            },
            {
                "type": "TaskFailed",
                "timestamp": datetime(2023, 1, 1, 10, 0, 15, tzinfo=timezone.utc)
            },
            {
                "type": "TaskStateExited", # Immediate exit after failure (no retry)
                "stateExitedEventDetails": {"name": "FailState"},
                "timestamp": datetime(2023, 1, 1, 10, 0, 16, tzinfo=timezone.utc)
            },
            {
                "type": "ExecutionSucceeded",
                "stateExitedEventDetails": {"name": "FailState"},
                "timestamp": datetime(2023, 1, 1, 10, 0, 20, tzinfo=timezone.utc)
            }
        ]

    def test_process_empty_history(self, sample_workflow):
        """Test processing an empty execution history."""
        result = process_execution_history(sample_workflow, [])
        assert result == []

    def test_process_simple_history(self, sample_workflow, simple_history):
        """Test processing a simple execution history with no failures."""
        result = process_execution_history(sample_workflow, simple_history)

        assert len(result) == 2

        # Check first state
        assert result[0].name == "State1"
        assert result[0].start == simple_history[0]["timestamp"]
        assert result[0].end == simple_history[1]["timestamp"]
        assert result[0].workflow == sample_workflow
        assert result[0].attempts == 1

        # Check second state
        assert result[1].name == "State2"
        assert result[1].start == simple_history[2]["timestamp"]
        assert result[1].end == simple_history[3]["timestamp"]
        assert result[1].workflow == sample_workflow
        assert result[1].attempts == 1

    def test_process_history_with_combined_retries(self, sample_workflow, history_with_retries):
        """Test processing history with failures where retries are combined."""
        result = process_execution_history(sample_workflow, history_with_retries, separate_retries=False)

        assert len(result) == 2

        # First state should be normal
        assert result[0].name == "SuccessState"
        assert result[0].attempts == 1

        # Second state should have 3 attempts
        assert result[1].name == "RetryState"
        assert result[1].start == history_with_retries[2]["timestamp"]
        assert result[1].end == history_with_retries[9]["timestamp"]
        assert result[1].attempts == 3  # Initial attempt + 2 retries

    def test_process_history_with_separate_retries(self, sample_workflow, history_with_retries):
        """Test processing history with failures where retries are separate events."""
        result = process_execution_history(sample_workflow, history_with_retries, separate_retries=True)

        assert len(result) == 4  # 1 success state + 3 attempts of retry state

        # First state should be normal
        assert result[0].name == "SuccessState"
        assert result[0].attempts == 1

        # Should have 3 separate events for the retry state
        retry_states = [e for e in result if e.name == "RetryState"]
        assert len(retry_states) == 3

        # Check each attempt has the right timing
        assert retry_states[0].start == history_with_retries[2]["timestamp"]
        assert retry_states[0].end == history_with_retries[4]["timestamp"]

        assert retry_states[1].start == history_with_retries[5]["timestamp"]
        assert retry_states[1].end == history_with_retries[6]["timestamp"]

        # Last attempt includes time to StateExited
        assert retry_states[2].start == history_with_retries[7]["timestamp"]
        assert retry_states[2].end == history_with_retries[9]["timestamp"]

    def test_task_failure_then_exit(self, sample_workflow, history_with_task_failure_then_exit):
        """Test a history where a task fails but the next event is a StateExited (no retry)."""
        result = process_execution_history(sample_workflow, history_with_task_failure_then_exit)

        assert len(result) == 1
        assert result[0].name == "FailState"
        assert result[0].start == history_with_task_failure_then_exit[0]["timestamp"]
        assert result[0].end == history_with_task_failure_then_exit[3]["timestamp"]
        assert result[0].attempts == 1  # Should only count as 1 attempt