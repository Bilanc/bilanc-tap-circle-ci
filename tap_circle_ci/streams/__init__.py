"""tap-circle-ci streams module."""

from .jobs import Jobs
from .pipelines import Pipelines
from .workflows import Workflows
from .workflow_metrics import WorkflowMetrics

STREAMS = {
    Pipelines.tap_stream_id: Pipelines,
    Workflows.tap_stream_id: Workflows,
    Jobs.tap_stream_id: Jobs,
    WorkflowMetrics.tap_stream_id: WorkflowMetrics,

}
