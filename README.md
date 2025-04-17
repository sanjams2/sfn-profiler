# SFN Profiler

![example workflow](https://github.com/sanjams2/sfn-profiler/actions/workflows/python-package.yml/badge.svg)

A package of utilities for profiling [AWS Step Function](https://aws.amazon.com/step-functions/) executions

## Installation

```bash
pip install git+https://github.com/sanjams2/sfn-profiler
```
## sfn-profiler

![alt text](https://raw.githubusercontent.com/sanjams2/sfn-profiler/refs/heads/main/docs/example.png)

This utility provides relevant performance metrics and information about particular Step Function executions
and their child workflows (called 'contributor' workflows) in your local web browser. It displays information 
such as the largest contributors to the overall duration as well as a gantt chart representation of the workflow 
execution.

### Usage
```bash
sfn-profiler arn:aws:states:us-east-1:1234567812:execution:MyStateMachine:execution-1234
```

You can provide a list of "contributor" or child workflows that can be displayed within the parent workflows gantt chart
directly or in an aggregated fashion. This is useful to understand what is happening when your parent workflow is waiting
on child workflows to perform some action(s). You can specify these with the `--contributors` option. There are also
several options to help with aggregation and filtering of child workflows:
* `--min-contributor-task-duration` - Minimum amount of time in seconds a task must take in order to display it 
                                      (for contributor workflows only)
* `--no-aggregate` - Do not aggregate contributor workflow steps, display each child workflow separately
* `--no-interleave` - If specified, contributor workflow tasks will be displayed separately below the parent workflow in 
                      the profile (vs interleaving contributor tasks)

## sfn2perfetto

![alt text](https://raw.githubusercontent.com/sanjams2/sfn-profiler/refs/heads/main/docs/perfetto-example.png)

A utility to turn step function executions into a [Perfetto](https://perfetto.dev/) 
[protobuf file](https://perfetto.dev/docs/reference/synthetic-track-event). The output can be uploaded
to https://ui.perfetto.dev/ and analyzed there. More of an experimental utility for now

### Usage

```bash
sfn2perfetto --executions arn:aws:states:us-east-1:1234567812:execution:MyStateMachine:execution-1234 \
    --output /tmp/sfn.pftrace
```

## Development

Perfetto protobuf file can be found here: https://github.com/google/perfetto/blob/main/protos/perfetto/trace/perfetto_trace.proto

Run the following to generate the python models from the local protobuf definition:
```bash
protoc --proto_path=proto --pyi_out=src/sfn_profiler/models/perfetto --python_out=src/sfn_profiler/models/perfetto proto/perfetto_trace.proto
```
