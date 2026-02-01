# pyright: strict

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from webilastik.classifiers.pixel_classifier import VigraPixelClassifier
from webilastik.datasink import DataSink
from webilastik.datasource import DataSource
from webilastik.simple_segmenter import SimpleSegmenter
from webilastik.ui.applet.export_jobs import ExportJob
from executor_getter import get_executor


def main():
    parser = argparse.ArgumentParser(description="Run a single export job on the cluster")
    parser.add_argument("--job_spec", required=True, help="JSON file containing the job specification")
    args = parser.parse_args()

    # Load job spec
    with open(args.job_spec, 'r') as f:
        job_spec = json.load(f)

    # Deserialize components
    datasource = DataSource.from_dto(job_spec["datasource"])
    if isinstance(datasource, Exception):
        print(f"Failed to deserialize datasource: {datasource}", file=sys.stderr)
        sys.exit(1)

    datasink = DataSink.create_from_message(job_spec["datasink"])
    if isinstance(datasink, Exception):
        print(f"Failed to deserialize datasink: {datasink}", file=sys.stderr)
        sys.exit(1)

    # For pixel probabilities export
    if "pixel_probabilities" in job_spec:
        classifier_dto = job_spec["pixel_probabilities"]["classifier"]
        classifier = VigraPixelClassifier.from_dto(classifier_dto)
        if isinstance(classifier, Exception):
            print(f"Failed to deserialize classifier: {classifier}", file=sys.stderr)
            sys.exit(1)

        # Open datasink
        sink_writer = datasink.open()
        if isinstance(sink_writer, Exception):
            print(f"Failed to open datasink: {sink_writer}", file=sys.stderr)
            sys.exit(1)

        # Create and run export job
        export_job = ExportJob(
            name="Cluster Export - Pixel Probabilities",
            on_progress=lambda job_id, step_index: print(f"Progress: {step_index}"),
            operator=classifier,
            sink_writer=sink_writer,
            args=datasource.roi.split(block_shape=sink_writer.data_sink.tile_shape.updated(c=datasource.shape.c)),
            num_args=datasource.roi.get_num_tiles(tile_shape=sink_writer.data_sink.tile_shape),
        )

        # Run synchronously for simplicity
        for step_arg in export_job.args:
            result = export_job.target(step_arg)
            if isinstance(result, Exception):
                print(f"Export step failed: {result}", file=sys.stderr)
                sys.exit(1)

    # For simple segmentation export
    elif "simple_segmentation" in job_spec:
        classifier_dto = job_spec["simple_segmentation"]["classifier"]
        label_index = job_spec["simple_segmentation"]["label_index"]
        classifier = VigraPixelClassifier.from_dto(classifier_dto)
        if isinstance(classifier, Exception):
            print(f"Failed to deserialize classifier: {classifier}", file=sys.stderr)
            sys.exit(1)

        segmenter = SimpleSegmenter(preprocessor=classifier, channel_index=label_index)

        # Open datasink
        sink_writer = datasink.open()
        if isinstance(sink_writer, Exception):
            print(f"Failed to open datasink: {sink_writer}", file=sys.stderr)
            sys.exit(1)

        # Create and run export job
        export_job = ExportJob(
            name="Cluster Export - Simple Segmentation",
            on_progress=lambda job_id, step_index: print(f"Progress: {step_index}"),
            operator=segmenter,
            sink_writer=sink_writer,
            args=datasource.roi.split(block_shape=sink_writer.data_sink.tile_shape.updated(c=datasource.shape.c)),
            num_args=datasource.roi.get_num_tiles(tile_shape=sink_writer.data_sink.tile_shape),
        )

        # Run synchronously for simplicity
        for step_arg in export_job.args:
            result = export_job.target(step_arg)
            if isinstance(result, Exception):
                print(f"Export step failed: {result}", file=sys.stderr)
                sys.exit(1)
    else:
        print("Unknown export type", file=sys.stderr)
        sys.exit(1)

    print("Export completed successfully")


if __name__ == "__main__":
    main()
