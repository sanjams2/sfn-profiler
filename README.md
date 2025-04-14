# SFN Profiler

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
sfn-profiler --execution arn:aws:states:us-east-1:1234567812:execution:MyStateMachine:execution-1234
```

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