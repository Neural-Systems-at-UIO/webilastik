from typing import List, Dict, Tuple, Sequence
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from collections import defaultdict
import io
import json
import flask
from flask import Flask, request, Response, send_file
from flask_cors import CORS
import urllib
from PIL import Image as PilImage
import argparse
from pathlib import Path
import multiprocessing
import math
from urllib.parse import urlparse

from ndstructs import Point5D, Slice5D, Shape5D, Array5D
from ndstructs.datasource import DataSource, DataSourceSlice, SequenceDataSource, PrecomputedChunksDataSource
from ndstructs.utils import JsonSerializable, from_json_data, to_json_data, JsonReference

from webilastik.filesystem import HttpPyFs
from webilastik.workflows.pixel_classification_workflow import PixelClassificationWorkflow
from webilastik.server.pixel_classification_web_adapter import PixelClassificationWorkflow2WebAdapter
from webilastik.features.feature_extractor import FeatureDataMismatchException
from webilastik.classifiers.pixel_classifier import Predictions, PixelClassifier
from webilastik.server.WebContext import WebContext, EntityNotFoundException


parser = argparse.ArgumentParser(description="Runs ilastik prediction web server")
parser.add_argument("--host", default="localhost", help="ip or hostname where the server will listen")
parser.add_argument("--port", default=5000, type=int, help="port to listen on")
parser.add_argument("--ngurl", default="http://localhost:8080", help="url where neuroglancer is being served")
parser.add_argument("--sample-dirs", type=Path, help="List of directories containing samples", nargs="+")
parser.add_argument(
    "--num-workers",
    required=False,
    type=int,
    default=multiprocessing.cpu_count(),
    help="Number of process workers to run predictions",
)
args = parser.parse_args()

executors = [ProcessPoolExecutor(max_workers=1) for i in range(args.num_workers)]

app = Flask("WebserverHack")
CORS(app)

rpc_methods = {
    PixelClassificationWorkflow: {
        "add_annotations",
        "add_feature_extractors",
        "upload_to_cloud_ilastik",
        "add_lane_for_url",
        "add_ilp_feature_extractors",
        "get_ilp_feature_extractors",
        "remove_annotations",
        "remove_feature_extractors",
        "clear_feature_extractors",
        "get_classifier",
        "ilp_project",
    }
}

rpc_adapters = {PixelClassificationWorkflow: PixelClassificationWorkflow2WebAdapter}


@app.route("/rpc/<method_name>", methods=["POST"])
def run_rpc(method_name: str):
    """Runs method 'method_name' on object specified by the JsonReference data found in the '__self__' key
    of the POST payload"""

    method_params = WebContext.get_request_payload()
    self_ref = JsonReference.from_json_data(method_params.pop("__self__"), dereferencer=None)
    self = WebContext.dereferencer(self_ref)
    class_name = self.__class__.__name__
    try:
        allowed_methods = rpc_methods[self.__class__]
    except KeyError:
        return flask.Response(f"Cannot run methods on class {class_name}", status=403)
    if method_name not in allowed_methods:
        return flask.Response(f"Can't call {method_name} on {class_name}", status=403)

    adapter = rpc_adapters[self.__class__](WebContext, self)
    return from_json_data(getattr(adapter, method_name), method_params, dereferencer=WebContext.dereferencer)


def do_worker_predict(slice_batch: Tuple[PixelClassifier, Sequence[DataSourceSlice]]) -> List[Predictions]:
    classifier = slice_batch[0]
    out = []
    for datasource_slc in slice_batch[1]:
        pred_tile = classifier.predict(datasource_slc)
        out.append(pred_tile)
    return out


def do_predictions(roi: Slice5D, classifier_id: str, datasource_id: str) -> Predictions:
    classifier = WebContext.load(classifier_id)
    datasource = WebContext.load(datasource_id)
    if isinstance(datasource, PrecomputedChunksDataSource) and datasource.url.lstrip("precomputed://").startswith(
        get_base_url()
    ):
        ds_id = datasource.url.rstrip("/").split("/")[-2]
        datasource = WebContext.load(ds_id)
    backed_roi = DataSourceSlice(datasource, **roi.to_dict()).defined()

    predictions = classifier.allocate_predictions(backed_roi)

    all_slices = list(backed_roi.get_tiles())
    slc_batches = defaultdict(list)
    for slc in backed_roi.get_tiles():
        batch_idx = hash(slc) % args.num_workers
        slc_batches[batch_idx].append(slc)

    result_batch_futures = []
    for idx, batch in slc_batches.items():
        executor = executors[idx]
        result_batch_futures.append(executor.submit(do_worker_predict, (classifier, batch)))
    for future in result_batch_futures:
        for result in future.result():
            predictions.set(result)
    return predictions


@app.route("/predict/", methods=["GET"])
def predict():
    roi_params = {}
    for axis, v in request.args.items():
        if axis in "tcxyz":
            start, stop = [int(part) for part in v.split("_")]
            roi_params[axis] = slice(start, stop)

    predictions = do_predictions(
        roi=Slice5D(**roi_params),
        classifier_id=request.args["pixel_classifier_id"],
        datasource_id=request.args["data_source_id"],
    )

    channel = int(request.args.get("channel", 0))
    data = predictions.cut(Slice5D(c=channel)).as_uint8(normalized=True).raw("yx")
    out_image = PilImage.fromarray(data)
    out_file = io.BytesIO()
    out_image.save(out_file, "png")
    out_file.seek(0)
    return send_file(out_file, mimetype="image/png")


# https://github.com/google/neuroglancer/tree/master/src/neuroglancer/datasource/precomputed#unsharded-chunk-storage
@app.route(
    "/predictions/<classifier_id>/<datasource_id>/data/<int:xBegin>-<int:xEnd>_<int:yBegin>-<int:yEnd>_<int:zBegin>-<int:zEnd>"
)
def ng_predict(
    classifier_id: str, datasource_id: str, xBegin: int, xEnd: int, yBegin: int, yEnd: int, zBegin: int, zEnd: int
):
    requested_roi = Slice5D(x=slice(xBegin, xEnd), y=slice(yBegin, yEnd), z=slice(zBegin, zEnd))
    predictions = do_predictions(roi=requested_roi, classifier_id=classifier_id, datasource_id=datasource_id)

    # https://github.com/google/neuroglancer/tree/master/src/neuroglancer/datasource/precomputed#raw-chunk-encoding
    # "(...) data for the chunk is stored directly in little-endian binary format in [x, y, z, channel] Fortran order"
    resp = flask.make_response(predictions.as_uint8().raw("xyzc").tobytes("F"))
    resp.headers["Content-Type"] = "application/octet-stream"
    return resp


@app.route("/predictions/<classifier_id>/<datasource_id>/info/")
def info_dict(classifier_id: str, datasource_id: str) -> Dict:
    classifier = WebContext.load(classifier_id)
    datasource = WebContext.load(datasource_id)

    expected_predictions_shape = classifier.get_expected_roi(datasource.roi).shape

    resp = flask.jsonify(
        {
            "@type": "neuroglancer_multiscale_volume",
            "type": "image",
            "data_type": "uint8",  # DONT FORGET TO CONVERT PREDICTIONS TO UINT8!
            "num_channels": int(expected_predictions_shape.c),
            "scales": [
                {
                    "key": "data",
                    "size": [int(v) for v in expected_predictions_shape.to_tuple("xyz")],
                    "resolution": [1, 1, 1],
                    "voxel_offset": [0, 0, 0],
                    "chunk_sizes": [datasource.tile_shape.to_tuple("xyz")],
                    "encoding": "raw",
                }
            ],
        }
    )
    return resp


@app.route("/predictions/<classifier_id>/neuroglancer_shader", methods=["GET"])
def get_predictions_shader(classifier_id: str):
    classifier = WebContext.load(classifier_id)

    color_lines: List[str] = []
    colors_to_mix: List[str] = []

    for idx, color in enumerate(classifier.color_map.keys()):
        color_line = (
            f"vec3 color{idx} = (vec3({color.r}, {color.g}, {color.b}) / 255.0) * toNormalized(getDataValue({idx}));"
        )
        color_lines.append(color_line)
        colors_to_mix.append(f"color{idx}")

    shader_lines = [
        "void main() {",
        "    " + "\n    ".join(color_lines),
        "    emitRGBA(",
        f"        vec4({' + '.join(colors_to_mix)}, 1.0)",
        "    );",
        "}",
    ]
    return flask.Response("\n".join(shader_lines), mimetype="text/plain")


@app.route("/datasource/<datasource_id>/data/<int:xBegin>-<int:xEnd>_<int:yBegin>-<int:yEnd>_<int:zBegin>-<int:zEnd>")
def ng_raw(datasource_id: str, xBegin: int, xEnd: int, yBegin: int, yEnd: int, zBegin: int, zEnd: int):
    requested_roi = Slice5D(x=slice(xBegin, xEnd), y=slice(yBegin, yEnd), z=slice(zBegin, zEnd))
    datasource = WebContext.load(datasource_id)
    ds_url = datasource.url.lstrip("precomputed://").rstrip("/")
    if isinstance(datasource, PrecomputedChunksDataSource) and urlparse(ds_url).netloc != get_base_netloc():
        foreign_url = f"{ds_url}/{xBegin}-{xEnd}_{yBegin}-{yEnd}_{zBegin}-{zEnd}"
        return flask.redirect(foreign_url)

    data = datasource.retrieve(requested_roi)
    resp = flask.make_response(data.raw("xyzc").tobytes("F"))
    resp.headers["Content-Type"] = "application/octet-stream"
    return resp

def get_base_netloc() -> str:
    host = request.headers.get("X-Forwarded-Host", args.host)
    port = "" if "X-Forwarded-Host" in request.headers else f":{args.port}"
    return f"{host}{port}"

def get_base_url() -> str:
    protocol = request.headers.get("X-Forwarded-Proto", "http")
    netloc = get_base_netloc()
    prefix = request.headers.get("X-Forwarded-Prefix", "/")
    return f"{protocol}://{netloc}{prefix}"

def create_precomputed_chunks_datasource(url: str) -> PrecomputedChunksDataSource:
    parsed_url = urlparse(url.lstrip("precomputed://"))
    return PrecomputedChunksDataSource(
        path=Path(parsed_url.path),
        filesystem=HttpPyFs(parsed_url._replace(path="/").geturl())
    )

@app.route("/open_remote", methods=["GET"])
def open_remote():
    url = request.args.get('url')
    ds = create_precomputed_chunks_datasource(url)
    WebContext.store(ds)
    return flask.redirect(datasource_to_ng_url(ds))

def datasource_to_ng_url(datasource: DataSource) -> str:
    rgb_shader = """void main() {
      emitRGB(vec3(
        toNormalized(getDataValue(0)),
        toNormalized(getDataValue(1)),
        toNormalized(getDataValue(2))
      ));
    }
    """

    grayscale_shader = """void main() {
      emitGrayscale(toNormalized(getDataValue(0)));
    }
    """
    base_url = get_base_url()
    datasource_id = WebContext.get_ref(datasource).to_str()
    url_data = {
        "layers": [
            {
                "source": f"precomputed://{base_url}datasource/{datasource_id}",
                "type": "image",
                "blend": "default",
                "shader": grayscale_shader if datasource.shape.c == 1 else rgb_shader,
                "shaderControls": {},
                "name": datasource.name,
            },
            {"type": "annotation", "annotations": [], "voxelSize": [1, 1, 1], "name": "annotation"},
        ],
        "navigation": {"zoomFactor": 1},
        "selectedLayer": {"layer": "annotation", "visible": True},
        "layout": "xy",
    }
    return f"{args.ngurl}#!" + urllib.parse.quote(str(json.dumps(url_data)))

def get_sample_datasets() -> List[Dict]:
    return [{"url": datasource_to_ng_url(ds), "name": ds.name} for ds in WebContext.get_all(DataSource)]


@app.route("/datasets")
def get_datasets():
    return flask.jsonify(list(get_sample_datasets()))


@app.route("/neuroglancer-samples")
def ng_samples():
    link_tags = [f'<a href="{datasource_to_ng_url(ds)}">{ds.name}</a><br/>' for ds in WebContext.get_all(DataSource)]
    links = "\n".join(link_tags)
    return f"""
        <html>
            <head>
                <meta charset="UTF-8">
                <link rel=icon href="https://www.ilastik.org/assets/ilastik-logo.png">
            </head>

            <body>
                {links}
            </body>
        </html>
    """


@app.route("/datasource/<datasource_id>/info")
def datasource_info_dict(datasource_id: str) -> Dict:
    datasource = WebContext.load(datasource_id)

    resp = flask.jsonify(
        {
            "@type": "neuroglancer_multiscale_volume",
            "type": "image",
            "data_type": "uint8",  # DONT FORGET TO CONVERT PREDICTIONS TO UINT8!
            "num_channels": int(datasource.shape.c),
            "scales": [
                {
                    "key": "data",
                    "size": [int(v) for v in datasource.shape.to_tuple("xyz")],
                    "resolution": [1, 1, 1],
                    "voxel_offset": [0, 0, 0],
                    "chunk_sizes": [datasource.tile_shape.to_tuple("xyz")],
                    "encoding": "raw",
                }
            ],
        }
    )
    return resp


@app.errorhandler(FeatureDataMismatchException)
def handle_feature_data_mismatch(error):
    return flask.Response(str(error), status=400)


@app.errorhandler(EntityNotFoundException)
def handle_feature_data_mismatch(error):
    return flask.Response(str(error), status=404)


@app.route("/<class_name>/<object_id>", methods=["DELETE"])
def remove_object(class_name, object_id: str):
    WebContext.remove(WebContext.get_class_named(class_name), object_id)
    return flask.jsonify({"id": object_id})


@app.route("/<class_name>/", methods=["POST"])
def create_object(class_name: str):
    obj, ref = WebContext.create(WebContext.get_class_named(class_name))
    return flask.jsonify(ref.to_json_data())


@app.route("/<class_name>/", methods=["GET"])
def list_objects(class_name):
    klass = WebContext.get_class_named(class_name)
    payload = [value.to_json_data(referencer=WebContext.referencer) for value in WebContext.get_all(klass)]
    return flask.jsonify(payload)


@app.route("/<class_name>/<object_id>", methods=["GET"])
def show_object(class_name: str, object_id: str):
    klass = WebContext.get_class_named(class_name)
    obj = WebContext.load(object_id)
    assert isinstance(obj, klass)
    payload = obj.to_json_data(referencer=WebContext.referencer)
    return flask.jsonify(payload)


def _add_sample_datasource(path: Path):
    datasource = DataSource.create(path.absolute())
    ref = WebContext.store(datasource)
    print(path.name, " ", f"http://{args.host}:{args.port}/datasource/{ref.to_str()}/data")


for sample_dir_path in args.sample_dirs or ():
    for sample_path in sample_dir_path.iterdir():
        if sample_path.is_dir() and sample_path.suffix in (".n5", ".N5"):
            [_add_sample_datasource(dataset_path) for dataset_path in sample_path.iterdir() if dataset_path.is_dir()]
        if sample_path.is_file() and sample_path.suffix in (".png", ".jpg"):
            _add_sample_datasource(sample_path)

WebContext.store(
    create_precomputed_chunks_datasource(
        "precomputed://https://neuroglancer.humanbrainproject.org/precomputed/BigBrainRelease.2015/8bit/20um"
    )
)

app.run(host=args.host, port=args.port)
