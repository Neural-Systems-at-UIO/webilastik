from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import List, Iterator, Tuple, Dict, Iterable, Sequence, Any, Optional
from dataclasses import dataclass
from collections.abc import Mapping as BaseMapping
from numbers import Number
from pathlib import Path

import numpy as np
from ndstructs import Slice5D, Point5D, Shape5D
from ndstructs import Array5D, Image, ScalarData, StaticLine, LinearData
from webilastik.features.feature_extractor import FeatureExtractor, FeatureData
from ndstructs.datasource import DataSource, DataSourceSlice
from ndstructs.utils import JsonSerializable, from_json_data, Dereferencer
from PIL import Image as PilImage


class Color:
    def __init__(
        self,
        r: np.uint8 = np.uint8(0),
        g: np.uint8 = np.uint8(0),
        b: np.uint8 = np.uint8(0),
        a: np.uint8 = np.uint8(255),
        name: str = "",
    ):
        self.r = r
        self.g = g
        self.b = b
        self.a = a
        self.name = name or f"Label {self.rgba}"

    @classmethod
    def from_channels(cls, channels: List[np.uint8], name: str = "") -> "Color":
        if len(channels) == 0 or len(channels) > 4:
            raise ValueError(f"Cannnot create color from {channels}")
        if len(channels) == 1:
            channels = (channels[0] * 3) + [255]
        return cls(r=channels[0], g=channels[1], b=channels[2], a=channels[3], name=name)

    @property
    def rgba(self) -> Tuple[np.uint8, np.uint8, np.uint8, np.uint8]:
        return (self.r, self.g, self.b, self.a)

    @property
    def q_rgba(self) -> int:
        return sum(c * (16 ** (3 - idx)) for idx, c in enumerate(self.rgba))

    @property
    def ilp_data(self) -> np.ndarray:
        return np.asarray(self.rgba, dtype=np.int64)

    def __hash__(self):
        return hash(self.rgba)

    def __eq__(self, other):
        return not isinstance(other, Color) or self.rgba == other.rgba

    @classmethod
    def sort(cls, colors: Iterable["Color"]) -> List["Color"]:
        return sorted(colors, key=lambda c: c.q_rgba)

    @classmethod
    def create_color_map(cls, colors: Iterable["Color"]) -> Dict["Color", np.uint8]:
        return {color: np.uint8(idx + 1) for idx, color in enumerate(cls.sort(set(colors)))}


class FeatureSamples(FeatureData, StaticLine):
    """A multi-channel array with a single spacial dimension, with each channel representing a feature calculated on
    top of an annotated pixel. Features are assumed to be relative to a single label (annotation color)"""

    @classmethod
    def create(cls, annotation: "Annotation", data: FeatureData):
        samples = data.sample_channels(annotation.as_mask())
        return cls.fromArray5D(samples)

    @property
    def X(self) -> np.ndarray:
        return self.linear_raw()

    def get_y(self, label_class: np.uint8) -> np.ndarray:
        return np.full((self.shape.volume, 1), label_class, dtype=np.uint32)


class AnnotationOutOfBounds(Exception):
    def __init__(self, annotation_roi: Slice5D, raw_data: DataSource):
        super().__init__(f"Annotation roi {annotation_roi} exceeds bounds of raw_data {raw_data}")


class Annotation(ScalarData):
    """User annotation attached to the raw data onto which they were drawn"""

    def __hash__(self):
        return hash((self._data.tobytes(), self.color))

    def __eq__(self, other):
        if isinstance(other, Annotation):
            return False
        return self.color == other.color and np.all(self._data == other._data)

    def __init__(
        self, arr: np.ndarray, *, axiskeys: str, location: Point5D = Point5D.zero(), color: Color, raw_data: DataSource
    ):
        super().__init__(arr.astype(bool), axiskeys=axiskeys, location=location)
        if not raw_data.roi.contains(self.roi):
            raise AnnotationOutOfBounds(annotation_roi=self.roi, raw_data=raw_data)
        self.color = color
        self.raw_data = raw_data

    def rebuild(self, arr: np.ndarray, axiskeys: str, location: Point5D = None) -> "Annotation":
        location = self.location if location is None else location
        return self.__class__(arr, axiskeys=axiskeys, location=location, color=self.color, raw_data=self.raw_data)

    @classmethod
    def from_file(cls, path: Path, raw_data: DataSource, location: Point5D = Point5D.zero()) -> Iterable["Annotation"]:
        labels = DataSource.create(path)
        label_data = labels.retrieve(Slice5D.all())
        for color_array in label_data.unique_colors().colors:
            if np.count_nonzero(color_array.linear_raw()) == 0:
                continue
            single_color_labels = label_data.color_filtered(color_array)
            axiskeys = "tzyxc"
            color = Color.from_channels(color_array.raw("c"))
            yield cls(
                single_color_labels.raw(axiskeys), axiskeys=axiskeys, location=location, raw_data=raw_data, color=color
            )

    @classmethod
    def interpolate_from_points(cls, color: Color, voxels: List[Point5D], raw_data: DataSource):
        start = Point5D.min_coords(voxels)
        stop = Point5D.max_coords(voxels) + 1  # +1 because slice.stop is exclusive, but max_point isinclusive
        scribbling_roi = Slice5D.create_from_start_stop(start=start, stop=stop)
        if scribbling_roi.shape.c != 1:
            raise ValueError(f"Annotations must not span multiple channels: {voxels}")
        scribblings = Array5D.allocate(scribbling_roi, dtype=np.dtype(bool), value=False)

        anchor = voxels[0]
        for voxel in voxels:
            for interp_voxel in anchor.interpolate_until(voxel):
                scribblings.paint_point(point=interp_voxel, value=True)
            anchor = voxel

        return cls(scribblings._data, axiskeys=scribblings.axiskeys, color=color, raw_data=raw_data, location=start)

    @classmethod
    def from_json_data(cls, data, dereferencer: Optional[Dereferencer] = None):
        return from_json_data(cls.interpolate_from_points, data, dereferencer=dereferencer)

    def get_feature_samples(self, feature_extractor: FeatureExtractor) -> FeatureSamples:
        all_feature_samples = []
        annotated_roi = self.roi.with_full_c()

        with ThreadPoolExecutor() as executor:
            for data_tile in DataSourceSlice(self.raw_data).clamped(annotated_roi).get_tiles(clamp=False):

                def make_samples(data_tile):
                    annotation_tile = self.clamped(data_tile)
                    feature_tile = feature_extractor.compute(data_tile).clamped(annotation_tile.roi.with_full_c())

                    feature_samples = FeatureSamples.create(annotation_tile, feature_tile)
                    assert feature_samples.shape.c == feature_extractor.get_expected_shape(data_tile.shape).c
                    all_feature_samples.append(feature_samples)

                executor.submit(make_samples, data_tile)
        return all_feature_samples[0].concatenate(*all_feature_samples[1:])

    @classmethod
    def sort(self, annotations: Sequence["Annotation"]) -> List["Annotation"]:
        return sorted(annotations, key=lambda a: a.color.q_rgba)

    def colored(self, value: np.uint8) -> Array5D:
        return Array5D(self._data * value, axiskeys=self.axiskeys, location=self.location)

    @staticmethod
    def merge(annotations: Sequence["Annotation"], color_map: Optional[Dict[Color, np.uint8]] = None) -> Array5D:
        out_roi = Slice5D.enclosing(annot.roi for annot in annotations)
        out = Array5D.allocate(slc=out_roi, value=0, dtype=np.uint8)
        color_map = color_map or Color.create_color_map(annot.color for annot in annotations)
        for annot in annotations:
            out.set(annot.colored(color_map[annot.color]), mask_value=0)
        return out

    @staticmethod
    def dump_as_ilp_data(
        annotations: Sequence["Annotation"],
        color_map: Optional[Dict[Color, np.uint8]] = None,
        block_size: Optional[Shape5D] = None,
    ) -> Dict[str, Any]:
        if len(annotations) == 0:
            return {}
        if len(set(annot.raw_data for annot in annotations)) > 1:
            raise ValueError(f"All Annotations must come from the same datasource!")
        axiskeys = annotations[0].raw_data.axiskeys
        merged_annotations = Annotation.merge(annotations, color_map=color_map)
        block_size = block_size or merged_annotations.shape

        out = {}
        for block_index, block in enumerate(merged_annotations.split(block_size)):
            out[f"block{block_index:04d}"] = {
                "__data__": block.raw(axiskeys),
                "__attrs__": {
                    "blockSlice": "["
                    + ",".join(f"{slc.start}:{slc.stop}" for slc in block.roi.to_slices(axiskeys))
                    + "]"
                },
            }
        return out

    @property
    def ilp_data(self) -> dict:
        axiskeys = self.raw_data.axiskeys
        return {
            "__data__": self.raw(axiskeys),
            "__attrs__": {
                "blockSlice": "[" + ",".join(f"{slc.start}:{slc.stop}" for slc in self.roi.to_slices(axiskeys)) + "]"
            },
        }

    def __repr__(self):
        return f"<Annotation {self.shape} for raw_data: {self.raw_data}>"
