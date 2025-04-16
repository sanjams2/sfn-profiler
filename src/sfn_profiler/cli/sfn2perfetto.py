#!/usr/bin/env python3
"""
A script for turning a AWS Step Function execution into a json file consumable by Perfetto
"""

import argparse
import random
from typing import List, Dict

from sfn_profiler.clients.boto import session
from sfn_profiler.clients.sfn import SfnClient
from sfn_profiler.models import Event, ExecutionArn
from sfn_profiler.models.perfetto.perfetto_trace_pb2 import (
    TracePacket,
    ProcessDescriptor,
    TrackDescriptor,
    TrackEvent,
    ThreadDescriptor,
    Trace
)
from sfn_profiler.utils import file_arg_action
from sfn_profiler.utils.sfn import process_execution_history


def generate_trace_packets(num: int, execution: ExecutionArn, events: List[Event]) -> List[TracePacket]:
    process_uuid = num
    trusted_packet_sequence_id = random.randint(10**6, 10**7)
    task_ids: Dict[str, int] = {}

    packets: List[TracePacket] = list()
    packets.append(TracePacket(
        track_descriptor=TrackDescriptor(
            uuid=process_uuid,
            process=ProcessDescriptor(
                process_name=str(execution),
                pid=num,
            )
        ),
    ))

    start = events[0].start

    for event in events:
        rel_start = int((event.start - start).total_seconds() * 1e9)
        rel_end = int((event.end - start).total_seconds() * 1e9)

        if event.name not in task_ids:
            task_ids[event.name] = len(task_ids) + 1 + num
            packets.append(TracePacket(
                track_descriptor=TrackDescriptor(
                    uuid=task_ids[event.name],
                    thread=ThreadDescriptor(
                        pid=process_uuid,
                        tid=task_ids[event.name],
                        thread_name=event.name
                    )
                ),
            ))

        packets.append(TracePacket(
            timestamp=rel_start,
            track_event=TrackEvent(
                type=TrackEvent.TYPE_SLICE_BEGIN,
                track_uuid=task_ids[event.name],
                name=event.name,
            ),
            trusted_packet_sequence_id=trusted_packet_sequence_id,
        ))
        packets.append(TracePacket(
            timestamp=rel_end,
            track_event=TrackEvent(
                type=TrackEvent.TYPE_SLICE_END,
                track_uuid=task_ids[event.name],
                name=event.name,
            ),
            trusted_packet_sequence_id=trusted_packet_sequence_id,
        ))
    return packets


def write_trace(packets: List[TracePacket], output_file: str):
    with open(output_file, "wb") as f:
        f.write(Trace(packet=packets).SerializeToString())

def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--executions", nargs="+", action=file_arg_action(),
                        help="Step function execution arns to add to trace file. "
                             "Can be a list of full arns, or shortened ids. "
                             "Also can be a file by passing 'file:///path/to/file.txt'")
    parser.add_argument("-o", "--output", help="Output Perfetto proto file")
    return parser.parse_args()


def main():
    args = parse_args()
    sfn_client = SfnClient(session())

    packets = []
    for num, execution in enumerate(args.executions):
        arn = ExecutionArn.parse(execution)
        _, execution_history = sfn_client.get_state_machine_info(arn)
        events = process_execution_history(arn, execution_history)
        packets.extend(generate_trace_packets(num + len(packets), arn, events))

    write_trace(packets, args.output)
    print(f"Generated Perfetto proto file: {args.output}")
    print("To view the trace, visit https://ui.perfetto.dev/ and load this file.")

if __name__ == "__main__":
    main()