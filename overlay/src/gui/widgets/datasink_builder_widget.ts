import { DataSinkUnion, DziImageElement, DziLevelSink, DziSizeElement, Filesystem, Interval5D, Shape5D, ZipFs } from "../../client/ilastik";
import { Path } from "../../util/parsed_url";
import { DataType } from "../../util/precomputed_chunks";
import { Select } from "./input_widget";
import { BooleanInput, NumberInput } from "./value_input_widget";
import { Div, Label, Paragraph, Span } from "./widget";


export class DziSinkCreationError extends Error{}

export class UnsupportedDziDataType extends DziSinkCreationError{
    public readonly dtype: Exclude<DataType, "uint8">;
    constructor(params: {dtype: Exclude<DataType, "uint8">}){
        super(`This data type is incompatible with the DZI format: ${params.dtype}`)
        this.dtype = params.dtype
    }
}
export class UnsupportedDziDimensions extends DziSinkCreationError{
    public readonly z: number;
    constructor(params: {z: number}){
        super(`DZI only supports 2D images. Provided z: ${params.z}`)
        this.z = params.z
    }
}
export class UnsupportedDziNumChannels extends DziSinkCreationError{
    public readonly c: number;
    constructor(params: {c: number}){
        super(`DZI only supports 2D images. Provided c: ${params.c}`)
        this.c = params.c
    }
}
export class UnsupportedDziTileNumChannels extends DziSinkCreationError{
    public readonly c: number;
    constructor(params: {c: number}){
        super(`DZI tiles only support 2D images. Provided c: ${params.c}`)
        this.c = params.c
    }
}
export class UnsupportedZippedDziPath extends DziSinkCreationError{
    public readonly path: Path;
    constructor(params: {path: Path}){
        super(`Zipped DZI path names must be like 'some_name.dzip'. Provided: ${params.path.raw}`)
        this.path = params.path
    }
}
export class UnsupportedDziPath extends DziSinkCreationError{
    public readonly path: Path;
    constructor(params: {path: Path}){
        super(`DZI paths must be like 'some_name.xml' or 'some_name.dzi' . Provided: ${params.path.raw}`)
        this.path = params.path
    }
}

class DziDatasinkConfigWidget extends Div{
    private imageFormatSelector: Select<"png" | "jpg">;
    private overlapInput: NumberInput;
    public zipCheckbox: BooleanInput

    constructor(params: {parentElement: HTMLElement | undefined}){
        const imageFormatSelector = new Select<"png" | "jpg">({
            popupTitle: "Select a Deep Zoom image Format",
            parentElement: undefined,
            options: ["png", "jpg"],
            renderer: (opt) => new Span({parentElement: undefined, innerText: opt}),
            title: "Image file format of the individual Deep Zoom tiles"
        });

        const overlapInput = new NumberInput({
            parentElement: undefined,
            disabled: true,
            value: 0,
            title: "border width that is replicated amongst neighboring tiles. Unsupported for now."
        })

        const zipCheckbox = new BooleanInput({parentElement: undefined, value: true})

        super({
            ...params,
            children: [
                new Paragraph({
                    parentElement: undefined,
                    children: [
                        new Label({
                            innerText: "Image Format: ",
                            parentElement: undefined,
                            title: "The file type of the individual tiles of the dzi dataset.\n" +
                                "PNG is highly recommeded since its compression is lossless."
                        }),
                        imageFormatSelector,
                    ],
                }),

                new Paragraph({
                    parentElement: undefined,
                    children: [
                        new Label({
                            innerText: "Overlap: ",
                            parentElement: undefined,
                            title: "Number of pixels that should overlap inbetween tiles.\n" +
                                "Values different from 0 not supported yet."
                        }),
                        overlapInput
                    ]
                }),

                new Paragraph({
                    parentElement: undefined,
                    children: [
                        new Label({
                            innerText: "Zip: ",
                            parentElement: undefined,
                            title: "Produce a .dzip zip archive instead of a typical dzi directory."
                        }),
                        zipCheckbox
                    ]
                })

            ]
        })
        this.imageFormatSelector = imageFormatSelector
        this.overlapInput = overlapInput
        this.zipCheckbox = zipCheckbox
    }

    public tryMakeDataSink(params: {
        filesystem: Filesystem,
        path: Path,
        interval: Interval5D,
        dtype: "uint8" | "uint16" | "uint32" | "uint64" | "int64" | "float32",
        resolution: [number, number, number],
        tile_shape: Shape5D,
    }): DziLevelSink | undefined | DziSinkCreationError {
        const overlap = this.overlapInput.value
        if(overlap === undefined){
            return undefined
        }
        if(params.dtype != "uint8"){
            return new UnsupportedDziDataType({dtype: params.dtype})
        }
        if(params.interval.shape.z > 1 || params.tile_shape.z > 1){
            return new UnsupportedDziDimensions({z: params.interval.shape.z})
        }
        const suffix = params.path.suffix.toLowerCase()
        let filesystem: Filesystem
        let xml_path: Path
        if(this.zipCheckbox.value){
            if(suffix != "dzip"|| params.path.equals(Path.root)){
                return new UnsupportedZippedDziPath({path: params.path})
            }
            filesystem = new ZipFs(params.filesystem, params.path);
            xml_path = new Path({components: [params.path.stem + ".dzi"]})
        }else{
            if((suffix != "xml" && suffix != "dzi") || params.path.equals(Path.root)){
                return new UnsupportedDziPath({path: params.path})
            }
            filesystem = params.filesystem
            xml_path = params.path
        }
        const num_channels = params.interval.shape.c;
        if(num_channels != 1 && num_channels != 3){
            return new UnsupportedDziNumChannels({c: num_channels})
        }
        if(params.tile_shape.c != params.interval.shape.c){
            return new UnsupportedDziTileNumChannels({c: params.tile_shape.c})
        }
        const dzi_image = new DziImageElement({
            Format: this.imageFormatSelector.value,
            Overlap: overlap,
            Size: new DziSizeElement({
                Width: params.interval.shape.x,
                Height: params.interval.shape.y,
            }),
            TileSize: Math.max(params.tile_shape.x, params.tile_shape.y),
        })
        return new DziLevelSink({
            dzi_image,
            num_channels,
            filesystem,
            xml_path,
            level_index:dzi_image.max_level_index,
        })
    }
}

export class DatasinkConfigWidget{
    public readonly element: Div;
    private readonly dziConfig: DziDatasinkConfigWidget;

    constructor(params: {parentElement: HTMLElement}){
        this.dziConfig = new DziDatasinkConfigWidget({parentElement: params.parentElement})
        this.element = this.dziConfig
    }

    public get extension(): "dzi" | "dzip"{
        return this.dziConfig.zipCheckbox.value ? "dzip" : "dzi"
    }

    public tryMakeDataSink(params: {
        filesystem: Filesystem,
        path: Path,
        interval: Interval5D,
        dtype: DataType,
        resolution: [number, number, number],
        tile_shape: Shape5D,
    }): DataSinkUnion | undefined | Error{
        return this.dziConfig.tryMakeDataSink(params)
    }
}
