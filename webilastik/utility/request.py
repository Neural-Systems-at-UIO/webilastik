# pyright: strict

from io import IOBase
from typing import Literal, Mapping, Tuple
import requests
import sys

from requests.models import CaseInsensitiveDict

from webilastik.utility.url import Url

class ErrRequestCompletedAsFailure(Exception):
    def __init__(self, status_code: int, response_text: str = "", url: str = "", headers: "CaseInsensitiveDict[str] | None" = None) -> None:
        self.status_code = status_code
        self.response_text = response_text
        self.url = url
        self.headers = headers or CaseInsensitiveDict()
        super().__init__(f"Request completed but with a failure response: {status_code} for URL: {url}")

class ErrRequestCrashed(Exception):
    def __init__(self, cause: Exception) -> None:
        self.request_exception = cause
        super().__init__(f"Request crashed")

class ErrBadContentLength(Exception):
    pass

def request(
    session: requests.Session,
    method: Literal["get", "put", "post", "delete"],
    url: Url,
    data: "bytes | IOBase | None" = None,
    offset: int = 0,
    num_bytes: "int | None" = None,
    headers: "Mapping[str, str] | None" = None,
) -> "Tuple[bytes, CaseInsensitiveDict[str]] | ErrRequestCompletedAsFailure | ErrRequestCrashed":
    range_header_value: str
    if offset >= 0:
        range_header_value = f"bytes={offset}-"
        if num_bytes is not None:
            range_end = max(offset, offset + num_bytes - 1)
            range_header_value += str(range_end)
    else:
        range_header_value = f"bytes={offset}"

    headers = {**(headers or {}), "Range": range_header_value}

    try:
        response = session.request(method=method, url=url.schemeless_raw, data=data, headers=headers)
        if not response.ok:
            return ErrRequestCompletedAsFailure(
                status_code=response.status_code,
                response_text=response.text[:1000],  # Limit to first 1000 chars
                url=url.schemeless_raw,
                headers=response.headers
            )
        content = response.content
        if num_bytes is not None:
            content = content[:num_bytes]
        return (content, response.headers)
    except Exception as e:
        print(f"HTTP ERROR: {e}", file=sys.stderr)
        return ErrRequestCrashed(e)

def request_size(
    session: requests.Session,
    url: Url,
    headers: "Mapping[str, str] | None" = None,
) -> "int | ErrRequestCompletedAsFailure | ErrRequestCrashed | ErrBadContentLength":
    # Use a partial GET request with Range header to get file size without downloading the entire file
    # This is more efficient than the original HEAD request approach since HEAD is now forbidden
    response = request(session=session, method="get", url=url, headers=headers, offset=0, num_bytes=1)
    if isinstance(response, Exception):
        return response
    try:
        # Try to get content-length from response headers
        content_length = response[1].get("content-length")
        if content_length is not None:
            return int(content_length)
        
        # Fallback: try content-range header for partial responses
        content_range = response[1].get("content-range")
        if content_range is not None:
            # Content-Range format: "bytes start-end/total" (e.g., "bytes 0-0/12345")
            if "/" in content_range:
                total_size = content_range.split("/")[-1]
                if total_size.isdigit():
                    return int(total_size)
        
        # If we can't determine size from headers, this is an error
        return ErrBadContentLength()
    except Exception:
        return ErrBadContentLength()