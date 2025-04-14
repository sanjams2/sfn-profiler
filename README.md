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

