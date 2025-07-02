#pyright: strict

import json
from typing import Iterator, Literal, Optional, Tuple, List
from pathlib import Path, PurePosixPath
import time

import requests
from ndstructs.utils.json_serializable import ensureJsonArray, ensureJsonObject, ensureJsonString
from requests.models import CaseInsensitiveDict

from webilastik.filesystem import IFilesystem, FsIoException, FsFileNotFoundException, FsDirectoryContents
from webilastik.filesystem.http_fs import HttpFs
from webilastik.filesystem.os_fs import OsFs
from webilastik.serialization.json_serialization import parse_json
from webilastik.utility.log import Logger
from webilastik.utility.url import Url
from webilastik.server.rpc.dto import BucketFSDto, DataProxyObjectUrlResponse
from webilastik.utility import Seconds
from webilastik.utility.request import ErrRequestCompletedAsFailure, request_size, request as safe_request, ErrRequestCrashed

_cscs_session = requests.Session()
_data_proxy_session = requests.Session()

logger = Logger()

def _requests_from_data_proxy(
    method: Literal["get", "put", "delete"],
    url: Url,
    data: Optional[bytes],
    refresh_on_401: bool = True,
) -> "Tuple[bytes, CaseInsensitiveDict[str]] | FsFileNotFoundException | Exception":
    from webilastik.libebrains.global_user_login import GlobalLogin
    user_token = GlobalLogin.get_token()
    # Changed safe_request to a normal request after object storage migration - Consulation with Oliver S
    try:
        logger.debug(f"BucketFS: Making {method.upper()} request to data proxy: {url.schemeless_raw}")
        response = _data_proxy_session.request(
            method=method, 
            url=url.schemeless_raw, 
            data=data, 
            headers=user_token.as_ebrains_auth_header()
        )
        logger.debug(f"BucketFS: Data proxy response status: {response.status_code}")
        
        if response.status_code == 404:
            logger.debug(f"BucketFS: File not found at data proxy: {url.path}")
            return FsFileNotFoundException(url.path) #FIXME
        if response.status_code == 401 and refresh_on_401:
            logger.warning(f"BucketFS: Authentication failed (401) for data proxy, attempting token refresh")
            refreshed_token_result = GlobalLogin.refresh_token(stale_token=user_token)
            if isinstance(refreshed_token_result, Exception):
                logger.error(f"Could not refresh ebrains token in BucketFS: {refreshed_token_result}")
                return refreshed_token_result
            return _requests_from_data_proxy(
                method=method, url=url, data=data, refresh_on_401=False
            )
        if not response.ok:
            logger.error(f"BucketFS: Data proxy request failed with status {response.status_code}")
            logger.error(f"BucketFS: Data proxy response text: {response.text[:1000]}")
            logger.error(f"BucketFS: Data proxy response headers: {dict(response.headers)}")
            return ErrRequestCompletedAsFailure(response.status_code)
        return (response.content, response.headers)
    except Exception as e:
        logger.error(f"BucketFS: Data proxy request crashed: {e}")
        return ErrRequestCrashed(e)


class BucketFs(IFilesystem):
    API_URL = Url(protocol="https", hostname="data-proxy.ebrains.eu", path=PurePosixPath("/api/v1/buckets"))

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.url = self.API_URL.concatpath(bucket_name)
        super().__init__()

    @classmethod
    def recognizes(cls, url: Url) -> bool:
        return (
            url.protocol == "https" and
            url.hostname == cls.API_URL.hostname and
            url.port == cls.API_URL.port and
            url.path.as_posix().startswith(cls.API_URL.path.as_posix())
        )

    @classmethod
    def try_from(cls, url: Url) -> "Tuple[BucketFs, PurePosixPath] | None | Exception":
        if not cls.recognizes(url):
            return None
        bucket_name_part_index = len(cls.API_URL.path.parts)
        if len(url.path.parts) <= bucket_name_part_index:
            return Exception(f"Bad bucket url: {url}")
        return (
            BucketFs(bucket_name=url.path.parts[bucket_name_part_index]),
            PurePosixPath("/".join(url.path.parts[bucket_name_part_index + 1:]) or "/")
        )

    @classmethod
    def from_dto(cls, dto: BucketFSDto) -> "BucketFs":
        return BucketFs(bucket_name=dto.bucket_name)

    def to_dto(self) -> BucketFSDto:
        return BucketFSDto(bucket_name=self.bucket_name)

    def list_contents(self, path: PurePosixPath, limit: Optional[int] = 500) -> "FsDirectoryContents | FsIoException":
        list_objects_path = self.url.updated_with(extra_search={
            "delimiter": "/",
            "prefix": "" if path.as_posix() == "/" else path.as_posix().lstrip("/").rstrip("/") + "/",
            "limit": str(limit)
        })
        logger.debug(f"BucketFS: Listing contents for path: {path}")
        logger.debug(f"BucketFS: List objects URL: {list_objects_path.schemeless_raw}")
        
        response = _requests_from_data_proxy(method="get", url=list_objects_path, data=None)
        if isinstance(response, Exception):
            logger.error(f"BucketFS: Failed to list contents: {response}")
            return FsIoException(response)
        
        try:
            payload_obj = ensureJsonObject(json.loads(response[0])) #FIXME: use DTOs everywhere?
            raw_objects = ensureJsonArray(payload_obj.get("objects"))
            logger.debug(f"BucketFS: Found {len(raw_objects)} objects")
        except Exception as e:
            logger.error(f"BucketFS: Failed to parse list response: {e}")
            logger.error(f"BucketFS: Response content: {response[0][:500]}")
            return FsIoException(e)

        files: List[PurePosixPath] = []
        directories: List[PurePosixPath] = []
        for raw_obj in raw_objects:
            obj = ensureJsonObject(raw_obj)
            if "subdir" in obj:
                path = PurePosixPath("/") / ensureJsonString(obj.get("subdir")) #FIXME: Use DTO ?
                directories.append(path)
            else:
                path = PurePosixPath("/") / ensureJsonString(obj.get("name")) #FIXME: Use DTO ?
                files.append(path)
        logger.debug(f"BucketFS: Listed {len(files)} files and {len(directories)} directories")
        return FsDirectoryContents(files=files, directories=directories)

    def _parse_url_from_data_proxy_response(self, response_payload: bytes) -> "Url | Exception":
        response_json_result = parse_json(response_payload)
        if isinstance(response_json_result, Exception):
            return response_json_result
        response_dto_result = DataProxyObjectUrlResponse.from_json_value(response_json_result)
        if isinstance(response_dto_result, Exception):
            return response_dto_result
        out_url = Url.parse(response_dto_result.url)
        if out_url is None:
            return Exception("Could not parse URL from data proxy response")
        return out_url

    def create_file(self, *, path: PurePosixPath, contents: bytes) -> "None | FsIoException":
        logger.debug(f"BucketFS: Creating file at path: {path}")
        response = _requests_from_data_proxy(method="put", url=self.url.concatpath(path), data=None)
        if isinstance(response, Exception):
            logger.error(f"BucketFS: Failed to get CSCS URL for file creation: {response}")
            return FsIoException(response)
        cscs_url_result = self._parse_url_from_data_proxy_response(response[0])
        if isinstance(cscs_url_result, Exception):
            logger.error(f"BucketFS: Could not parse CSCS object URL (write): {cscs_url_result}")
            return FsIoException(f"Could not parse CSCS object URL (write): {cscs_url_result}")
        
        logger.debug(f"BucketFS: Uploading to CSCS URL: {cscs_url_result.schemeless_raw}")
        logger.debug(f"BucketFS: Upload payload size: {len(contents)} bytes")
        response = safe_request(session=_cscs_session, method="put", url=cscs_url_result, data=contents)
        if isinstance(response, Exception):
            logger.error(f"BucketFS: CSCS upload failed: {response}")
            if isinstance(response, ErrRequestCompletedAsFailure):
                logger.error(f"BucketFS: CSCS upload status code: {response.status_code}")
                logger.error(f"BucketFS: CSCS upload response text: {response.response_text}")
                logger.error(f"BucketFS: CSCS upload URL: {response.url}")
                logger.error(f"BucketFS: CSCS upload response headers: {dict(response.headers)}")
            return FsIoException(response)
        logger.debug(f"BucketFS: File successfully uploaded to CSCS")
        return None

    def create_directory(self, path: PurePosixPath) -> "None | FsIoException":
        return None

    def get_swift_object_url(self, path: PurePosixPath) -> "Url | FsIoException | FsFileNotFoundException":
        file_url = self.url.concatpath(path).updated_with(extra_search={"redirect": "false"})
        logger.debug(f"BucketFS: Getting Swift object URL for path: {path}")
        logger.debug(f"BucketFS: Data proxy URL: {file_url.schemeless_raw}")
        
        data_proxy_response = _requests_from_data_proxy(method="get", url=file_url, data=None)
        if isinstance(data_proxy_response, FsFileNotFoundException):
            logger.debug(f"BucketFS: File not found in data proxy: {path}")
            return FsFileNotFoundException(path)
        if isinstance(data_proxy_response, Exception):
            logger.error(f"BucketFS: Data proxy failed when getting Swift URL: {data_proxy_response}")
            return FsIoException(data_proxy_response) # FIXME: pass exception directly into other?
        
        cscs_url_result = self._parse_url_from_data_proxy_response(data_proxy_response[0])
        if isinstance(cscs_url_result, Exception):
            logger.error(f"BucketFS: Could not parse CSCS object URL (read): {cscs_url_result}")
            logger.error(f"BucketFS: Data proxy response content: {data_proxy_response[0][:500]}")
            return FsIoException(f"Could not parse CSCS object URL (read): {cscs_url_result}")
        
        logger.debug(f"BucketFS: Retrieved CSCS URL: {cscs_url_result.schemeless_raw}")
        return cscs_url_result

    def read_file(self, path: PurePosixPath, offset: int = 0, num_bytes: "int | None"  = None) -> "bytes | FsIoException | FsFileNotFoundException":
        logger.debug(f"BucketFS: Reading file at path: {path}, offset: {offset}, num_bytes: {num_bytes}")
        cscs_url_result = self.get_swift_object_url(path=path)
        if isinstance(cscs_url_result, Exception):
            logger.error(f"BucketFS: Failed to get CSCS URL for file read: {cscs_url_result}")
            return cscs_url_result
        
        logger.debug(f"BucketFS: Reading from CSCS URL: {cscs_url_result.schemeless_raw}")
        cscs_response = safe_request(session=_cscs_session, method="get", url=cscs_url_result, offset=offset, num_bytes=num_bytes)
        if isinstance(cscs_response, Exception):
            logger.error(f"BucketFS: CSCS read failed: {cscs_response}")
            if isinstance(cscs_response, ErrRequestCompletedAsFailure):
                logger.error(f"BucketFS: CSCS read status code: {cscs_response.status_code}")
                logger.error(f"BucketFS: CSCS read response text: {cscs_response.response_text}")
                logger.error(f"BucketFS: CSCS read URL: {cscs_response.url}")
                logger.error(f"BucketFS: CSCS read response headers: {dict(cscs_response.headers)}")
            return FsIoException(cscs_response) # FIXME: pass exception directly into other?
        logger.debug(f"BucketFS: Successfully read {len(cscs_response[0])} bytes from CSCS")
        return cscs_response[0]

    def get_size(self, path: PurePosixPath) -> "int | FsIoException | FsFileNotFoundException":
        logger.debug(f"BucketFS: Getting size for path: {path}")
        cscs_url_result = self.get_swift_object_url(path=path)
        if isinstance(cscs_url_result, Exception):
            logger.error(f"BucketFS: Failed to get CSCS URL for size check: {cscs_url_result}")
            return cscs_url_result
        
        logger.debug(f"BucketFS: Checking size at CSCS URL: {cscs_url_result.schemeless_raw}")
        size_result = request_size(session=_cscs_session, url=cscs_url_result)
        if isinstance(size_result, ErrRequestCompletedAsFailure):
            logger.error(f"BucketFS: CSCS size check failed with status: {size_result.status_code}")
            if hasattr(size_result, 'response_text'):
                logger.error(f"BucketFS: CSCS size check response text: {size_result.response_text}")
            if hasattr(size_result, 'url'):
                logger.error(f"BucketFS: CSCS size check URL: {size_result.url}")
            if hasattr(size_result, 'headers'):
                logger.error(f"BucketFS: CSCS size check response headers: {dict(size_result.headers)}")
            if size_result.status_code == 404:
                return FsFileNotFoundException(path)
            else:
                return FsIoException(size_result)
        if isinstance(size_result, Exception):
            logger.error(f"BucketFS: CSCS size check crashed: {size_result}")
            return FsIoException(size_result)
        logger.debug(f"BucketFS: File size: {size_result} bytes")
        return size_result

    def delete(
        self, path: PurePosixPath, dir_wait_time: Seconds = Seconds(5), dir_wait_interval: Seconds = Seconds(0.2)
    ) -> "None | FsIoException":
        dir_contents_result = self.list_contents(path.parent)
        if isinstance(dir_contents_result, Exception):
            return dir_contents_result

        deletion_response = _requests_from_data_proxy(method="delete", url=self.url.concatpath(path), data=None)
        #FIXME: what about not found?
        if isinstance(deletion_response, Exception):
            return FsIoException(deletion_response)

        if path in dir_contents_result.files:
            return None
        if path in dir_contents_result.directories:
            while dir_wait_time > Seconds(0):
                parent_contents_result = self.list_contents(path.parent)
                if isinstance(parent_contents_result, Exception):
                    return parent_contents_result
                if path not in parent_contents_result.directories:
                    return None
                time.sleep(dir_wait_interval.to_float())
                dir_wait_time -= dir_wait_interval
        # FIXME: i think this might me unreachable
        return FsIoException("Not found")

    def geturl(self, path: PurePosixPath) -> Url:
        return self.url.concatpath(path)

    def download_to_disk(
        self, *, source: PurePosixPath, destination: Path, chunk_size: int
    ) -> Iterator["Exception | float"]:
        cscs_url_result = self.get_swift_object_url(path=source)
        if isinstance(cscs_url_result, Exception):
            yield cscs_url_result
            return
        if cscs_url_result.protocol == "file" or cscs_url_result.protocol == "memory":
           yield Exception(f"Unexpected cscs URL: {cscs_url_result.raw}")
           return
        cscs_httpfs = HttpFs(
            protocol=cscs_url_result.protocol,
            hostname=cscs_url_result.hostname,
            path=PurePosixPath("/"),
            port=cscs_url_result.port,
            search=cscs_url_result.search,
        )
        yield from cscs_httpfs.download_to_disk(source=cscs_url_result.path, destination=destination, chunk_size=chunk_size)

    def transfer_file(
        self, *, source_fs: IFilesystem, source_path: PurePosixPath, target_path: PurePosixPath
    ) -> "FsIoException | FsFileNotFoundException | None":
        if not isinstance(source_fs, OsFs):
            return super().transfer_file(source_fs=source_fs, source_path=source_path, target_path=target_path)

        logger.debug(f"BucketFS: Transferring file from {source_path} to {target_path}")
        response = _requests_from_data_proxy(method="put", url=self.url.concatpath(target_path), data=None)
        if isinstance(response, Exception):
            logger.error(f"BucketFS: Failed to get CSCS URL for file transfer: {response}")
            return FsIoException(response)
        response_obj = ensureJsonObject(json.loads(response[0]))
        cscs_url = Url.parse_or_raise(ensureJsonString(response_obj.get("url"))) #FIXME: could raise

        logger.debug(f"BucketFS: Transferring to CSCS URL: {cscs_url.schemeless_raw}")
        source_file = source_fs.resolve_path(source_path).open("rb")
        response = safe_request(session=_cscs_session, method="put", url=cscs_url, data=source_file)
        if isinstance(response, Exception):
            logger.error(f"BucketFS: CSCS transfer failed: {response}")
            if isinstance(response, ErrRequestCompletedAsFailure):
                logger.error(f"BucketFS: CSCS transfer status code: {response.status_code}")
                logger.error(f"BucketFS: CSCS transfer response text: {response.response_text}")
                logger.error(f"BucketFS: CSCS transfer URL: {response.url}")
                logger.error(f"BucketFS: CSCS transfer response headers: {dict(response.headers)}")
            return FsIoException(response)
        logger.debug(f"BucketFS: File successfully transferred to CSCS")
        return None