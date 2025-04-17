#!/usr/bin/env python3
"""
Profile an AWS Step Function

This script displays relevant metrics about a particular step function execution (and any potential
child or 'contributor' workflows) to understand its performance. In particular, this script will
display the steps contributing the most to an execution's duration as well as a gantt chart representation
of the workflow steps.

Current limitations:
* Does not support workflows with parallel step executions=
"""
import argparse
import os
import tempfile
from datetime import timedelta
from http.server import HTTPServer, SimpleHTTPRequestHandler
from typing import Dict, List, Any, Tuple

from sfn_profiler.clients.boto import session
from sfn_profiler.clients.sfn import SfnClient
from sfn_profiler.models import Event, AggregateEvent, Workflow
from sfn_profiler.utils import get_hostname, file_arg_action, noop_context
from sfn_profiler.utils.loops import find_loops_in_execution, coalesce_loop_events
from sfn_profiler.utils.sfn import get_execution_arn, process_execution_history


def filter_small_steps(state_timing: List[Event], min_duration_sec):
    return [e for e in state_timing if e.duration >= timedelta(seconds=min_duration_sec)]


def aggregate(aggregate_data: Dict[str, AggregateEvent], contributor_data: List[Event]):
    """Aggregate data from contributors into aggregate."""
    for event in contributor_data:
        if event.name not in aggregate_data:
            aggregate_data[event.name] = AggregateEvent.from_event(event)
        aggregate_data[event.name].add_event(event)
    return aggregate_data


def fill_missing_steps(aggregated_events: Dict[str, AggregateEvent], contributors: List[Workflow]):
    for workflow in contributors:
        for event in workflow.events:
            if event.name in aggregated_events and workflow.id not in aggregated_events[event.name].contributors:
                aggregated_events[event.name].add_event(event)


def create_timeline(workflow: Workflow, tmpdir: str) -> str:
    """Create a timeline using plotly.graph_objects and save it as an HTML file."""
    import pandas as pd
    import plotly.graph_objects as go
    import plotly.express as px
    import numpy as np
    from plotly.subplots import make_subplots

    hover_bg_color = "#f0eee6"
    hover_font_color = '#141413'

    # Rest of the function remains the same
    timeline_data = []

    for event in workflow.events:
        is_aggregate = isinstance(event, AggregateEvent)
        timeline_data.append({
            'Task': event.name,
            'Start': event.start,
            'Finish': event.end,
            'Duration': event.total_seconds(),
            'Attempts': event.attempts,
            'IsContributor': event.workflow != workflow.id,
            'IsAggregate': is_aggregate,
            'EventDurations': event.durations() if is_aggregate else [],
        })

    df = pd.DataFrame(timeline_data)
    workflow_df = df[df['IsContributor'] == False]

    # Sort tasks by their first start time (reversed)
    task_order = df.groupby('Task')['Start'].min().sort_values(ascending=True).index.tolist()
    task_order.reverse()  # Reverse the order
    df['Task'] = pd.Categorical(df['Task'], categories=task_order, ordered=True)
    df = df.sort_values(by=['Start'])

    color_scale = px.colors.sequential.YlOrRd

    # Create figure with subplots for the main timeline and hidden histograms
    fig = make_subplots(
        rows=1, cols=1,
        specs=[[{"type": "bar"}]],
        subplot_titles=["Execution Timeline"]
    )

    # Add bars for each task
    for task in task_order:
        task_df = df[df['Task'] == task]

        for idx, row in task_df.iterrows():
            attempts = row['Attempts']
            attempt_hover_piece = f"<br><b style='color:#550000'>Attempts: {attempts}</b>" if attempts > 1 else ""
            hover_template = (f"State: {task}"
                              f"<br>Start: {row['Start'].strftime('%H:%M:%S')}"
                              f"<br>End: {row['Finish'].strftime('%H:%M:%S')}"
                              f"<br>Duration: {row['Duration']:.2f}s"
                              f"{attempt_hover_piece}"
                              f"<br>")

            # Add histogram info for contributor steps
            if row['IsAggregate'] and len(row['EventDurations']) > 1:
                values = row['EventDurations']

                # Calculate statistics
                mean_duration = np.mean(values)
                median_duration = np.median(values)
                min_duration = min(values)
                max_duration = max(values)
                vcount = len(values)

                # Create histogram data for hover info
                hist_data, bin_edges = np.histogram(values, bins=min(10, vcount))
                bin_centers = 0.5 * (bin_edges[:-1] + bin_edges[1:])

                # Create ASCII histogram representation
                max_bar_width = 20
                max_count = max(hist_data)
                ascii_hist = "<br>Duration distribution:<br>"
                for i, (center, count) in enumerate(zip(bin_centers, hist_data)):
                    bar_width = int(count / max_count * max_bar_width) if max_count > 0 else 0
                    bar = "█" * bar_width
                    ascii_hist += f"{center:.1f}s: {bar} ({count})<br>"

                hover_template += (f"<br>Contributor workflow statistics:"
                                   f"<br>Count: {vcount}"
                                   f"<br>Mean: {mean_duration:.2f}s"
                                   f"<br>Median: {median_duration:.2f}s"
                                   f"<br>Range: {min_duration:.2f}s - {max_duration:.2f}s"
                                   f"<br>{ascii_hist}")

            if row['IsContributor']:
                color = 'rgba(50, 50, 50, 0.3)'
            else:
                color = px.colors.sample_colorscale(color_scale, task_df['Duration'] / workflow_df['Duration'].max())[0]

            fig.add_trace(go.Bar(
                x=[row['Duration']],
                y=[row['Task']],
                orientation='h',
                name=task,
                hovertemplate=hover_template + "<extra></extra>",
                text='',
                textposition='none',
                marker=dict(
                    color=color,
                    line=dict(width=1, color='rgba(50,50,50,0.5)')
                ),
                base=[(row['Start'] - df['Start'].min()).total_seconds()],
                showlegend=False,
                hoverlabel=dict(bgcolor=hover_bg_color, font_color=hover_font_color)
            ))

    # Add translucent boxes for loops
    for loop_num, loop in enumerate(workflow.loops, 1):
        if loop.iterations <= 1:
            continue
        start = (loop.start - df['Start'].min()).total_seconds() - 10
        end = (loop.end - df['Start'].min()).total_seconds() + 10

        y_min = min(task_order.index(name) for name in loop.names) - 1
        y_max = max(task_order.index(name) for name in loop.names) + 1

        fig.add_shape(
            type="rect",
            x0=start,
            x1=end,
            y0=y_min,
            y1=y_max,
            line=dict(color="rgba(0,0,255,0.5)", width=2),  # Blue border
            fillcolor="rgba(173,216,230,0.3)",  # Light blue fill with transparency
            layer='below',
            name=f"Loop {loop_num}",
        )

        # Add annotation with both visible text and hover information
        fig.add_annotation(
            x=(start + end) / 2,
            y=y_max,  # Position at the top of the box
            text=f"Loop {loop_num}",
            hovertext=f"Loop {loop_num}"
                      f"<br>Steps: {', '.join(loop.names)}"
                      f"<br>Start: {loop.start.strftime('%H:%M:%S')}"
                      f"<br>End: {loop.end.strftime('%H:%M:%S')}"
                      f"<br>Iterations: {loop.iterations}"
                      f"<br>Duration: {loop.total_seconds():.2f}s",
            showarrow=False,
            font=dict(
                family="Arial",
                size=10,
                color="rgba(0,0,0,0.8)"  # Dark text for contrast
            ),
            align="center",
            valign="top",  # Align to the top
            bgcolor="rgba(255,255,255,0.8)",  # Semi-transparent white background for text
            bordercolor="rgba(0,0,255,0.5)",
            borderwidth=1,
            borderpad=4,
            opacity=0.8,
            yshift=-5,  # Shift slightly down from the top edge
            hoverlabel=dict(
                bgcolor=hover_bg_color,
                font_size=10,
                font_family="Arial",
                font_color=hover_font_color,
            )
        )

    fig.update_layout(
        xaxis_title='Time (seconds)',
        yaxis_title='States',
        barmode='stack',
        height=200 + len(task_order) * 30,
        showlegend=False,
        hovermode='closest',
        coloraxis_showscale=False,
        # Set global hover mode and style
        hoverlabel=dict(
            bgcolor=hover_bg_color,
            bordercolor="rgba(0,0,0,0.1)",
            font_size=10,
            font_family="Arial",
            font_color=hover_font_color,
        )
    )

    fig.update_xaxes(type='linear')

    html_file = workflow.id_as_filename() + '.html'
    full_path = os.path.join(tmpdir, html_file)
    fig.write_html(full_path)
    return full_path


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "execution",
        help="The arn of the Step Function workflow execution "
             "(ex: arn:aws:states:us-west-2:123456789012:execution:MyStateMachine:b325cdd5-b4fb-424b-a84b-efcad5364c1e). "
             "Can also be a shortened workflow id like 'MyStateMachine:a616c2d6-2441-4245-aca4-c0c0bec010f2'",
    )
    parser.add_argument("--contributors", action=file_arg_action(), required=False, nargs="+",
                        help="List of workflows that contribute to the duration of the parent. "
                             "Can be a list of full arns, or shortened ids. "
                             "Also can be a file by passing 'file:///path/to/file.txt'")
    parser.add_argument(
        "--min-contributor-task-duration",
        type=int,
        required=False,
        default=120,
        help="Minimum amount of time in seconds a task must take in order to display it (for contributor workflows only)"
    )
    parser.add_argument("--no-aggregate", action="store_true", help="Do not aggregate contributor workflow steps")
    parser.add_argument("--no-interleave", action="store_true",
                        help="If specified, contributor workflow tasks will be displayed separately below the parent "
                             "workflow in the profile (vs interleaving contributor tasks)")
    parser.add_argument("--separate-retries", action="store_true",
                        help="Dont display retries of a task as the same task, separate them")
    parser.add_argument("--port", required=False, type=int, default=8888, help="Port to serve the HTML file on (default 8888)")
    parser.add_argument("--out-dir", required=False, help="Directory to write the HTML files to")
    return parser.parse_args()

# TODO: move this to a jinja template


def write_profile(name: Any, execution_profiles: List[Tuple[Workflow, str]], tmp_dir: str):
    full_path = os.path.join(tmp_dir, str(name).replace(":", "-").replace("/", "-") + '.html')
    html = "<html>\n"
    html += "<head>\n"
    html += "<script>\n"
    html += '''
    function resizeIframe(iframeId) {
      const iframe = document.getElementById(iframeId);
      iframe.onload = function() {
        const height = iframe.contentWindow.document.body.scrollHeight;
        iframe.style.height = height + 'px';
      };
    };
    '''
    html += "window.addEventListener('DOMContentLoaded', function() {\n"
    for i in range(len(execution_profiles)):
        html += '  resizeIframe("iframe{num}");\n'.format(num=i)
    html += "});\n"
    html += "</script>\n"
    html += "</head>\n"
    html += "<body>\n"
    for num, (workflow, file) in enumerate(execution_profiles):
        relative_location = file.replace(tmp_dir, "")
        html += f"<h3>{workflow.id}</h3>\n"
        html += "<h4>Info</h4>\n"
        html += "<ul>"
        html += f"<li>Duration: {workflow.total_minutes():.2f} min ({workflow.total_seconds():.2f} sec)</li>\n"
        html += f"<li>Start: {workflow.start.strftime('%Y-%m-%d %H:%M:%S %Z')}</li>\n"
        html += f"<li>End: {workflow.end.strftime('%Y-%m-%d %H:%M:%S %Z')}</li>\n"
        html += f"<li>Events: {len(workflow.events)}</li>\n"
        html += "</ul>"
        html += "<h4>Contributors</h4>\n"
        html += "<div style='display: flex;'>\n"
        html += "<div style='flex: 1'>\n"
        html += "<b>Without Loops</b>\n"
        html += "<table>\n"
        html += "<tr><th>Task</th><th>Total Duration</th></tr>\n"
        for task, duration in workflow.largest_contributors():
            html += f"<tr><td>{task}</td><td>{duration:.2f}s</td></tr>\n"
        html += "</table>\n"
        html += "</div>\n"
        html += "<div style='flex: 1'>\n"
        html += "<b>Including Loops</b>\n"
        html += "<table>\n"
        html += "<tr><th>Task</th><th>Total Duration</th></tr>\n"
        for task, duration in workflow.largest_contributors(with_loops=True):
            html += f"<tr><td>{task}</td><td>{duration:.2f}s</td></tr>\n"
        html += "</table>\n"
        html += "</div>\n"
        html += "</div>\n"
        html += "<h4>Timeline</h4>\n"
        html += f"<iframe id=\"iframe{num}\" src=\"{relative_location}\" width=\"100%\"></iframe>\n"
    html += "</body>\n"
    html += "</html>\n"
    with open(full_path, "w") as f:
        f.write(html)
    return full_path


def serve_html(fname: str, port: int):
    """Serve the HTML file using a simple HTTP server."""
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=os.path.dirname(fname), **kwargs)

    httpd = HTTPServer(('0.0.0.0', port), Handler)
    hostname = get_hostname()
    basename = os.path.basename(fname)
    print(f"Serving on http://{hostname}:{port}/{basename} (http://localhost:{port}/{basename} if tunneling)")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down the server...")
    finally:
        httpd.server_close()


def main():
    """Main function to generate the Gantt chart and serve it."""
    args = parse_args()
    aggregate_contributors = not args.no_aggregate
    interleave_contributors = not args.no_interleave
    contributors = args.contributors
    if contributors and contributors[0].startswith("file://"):
        with open(contributors[0][len('file://'):], 'r') as f:
            contributors = [line.strip() for line in f.readlines()]

    sfn_client = SfnClient(session())

    execution_arn = get_execution_arn(args.execution)
    print(f"Profiling {execution_arn}")
    _, history = sfn_client.get_state_machine_info(execution_arn)
    events = process_execution_history(execution_arn, history, separate_retries=args.separate_retries)
    print(f"Found {len(events)} events")
    loops = find_loops_in_execution(events)

    workflows = [Workflow(id=execution_arn, events=events, loops=loops)]
    aggregated_contributor_events: Dict[str, AggregateEvent] = {}

    for contributor in contributors or []:
        print(f"Profiling contributor {contributor}")
        contributor_execution_arn = get_execution_arn(contributor)
        _, contributor_history = sfn_client.get_state_machine_info(contributor_execution_arn)
        contributor_events = process_execution_history(
            contributor_execution_arn,
            contributor_history,
            separate_retries=args.separate_retries)
        contributor_loops = find_loops_in_execution(contributor_events)
        if aggregate_contributors:
            contributor_events = coalesce_loop_events(contributor_events, contributor_loops)
        contributor_events = filter_small_steps(contributor_events, args.min_contributor_task_duration)
        if aggregate_contributors:
            aggregated_contributor_events = aggregate(aggregated_contributor_events, contributor_events)
        else:
            workflows.append(Workflow(id=contributor_execution_arn, events=contributor_events, loops=contributor_loops))

    if aggregate_contributors:
        fill_missing_steps(aggregated_contributor_events, workflows[1:])
        workflows = [workflows[0]] + [Workflow(id='AGG CONTR', events=list(aggregated_contributor_events.values()), loops=[])]

    if interleave_contributors:
        # merge everything into the first state timings
        for workflow in workflows[1:]:
            for event in workflow.events:
                event.name = f"[{workflow.id}] {event.name}"
            workflows[0].add_events(workflow.events)
        workflows = [workflows[0]]

    dir = noop_context(args.out_dir) or tempfile.TemporaryDirectory(prefix='sfn-profiles')
    with dir as out:
        files = []
        for workflow in workflows:
            html_file = create_timeline(workflow, out)
            files.append((workflow, html_file))
        path = write_profile('full-' + str(execution_arn), files, out)
        print('Serving profile...')
        serve_html(path, args.port)


if __name__ == "__main__":
    main()
