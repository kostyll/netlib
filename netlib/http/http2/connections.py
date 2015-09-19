from __future__ import absolute_import, print_function, division
import itertools
import threading
import time

from hpack.hpack import Encoder, Decoder
from hyperframe.frame import (
    FRAME_MAX_LEN,
    FRAME_MAX_ALLOWED_LEN,
    Frame,
    HeadersFrame,
    ContinuationFrame,
    DataFrame,
    WindowUpdateFrame,
    SettingsFrame,
)

from ...exceptions import Http2Exception
from .. import Headers, Request, Response

CLIENT_CONNECTION_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

# TODO: remove once a new version is released
# Polyfill for hyperframe <= 1.1.1
# Taken from https://github.com/python-hyper/hyperframe/commit/51186d4840881950f014a8124e558ead67cd053b
def __frame_repr__(self):
    flags = ", ".join(self.flags) or "None"
    body = self.serialize_body()
    if len(body) > 100:
        body = str(body[:100]) + "..."
    return (
        "{type}(Stream: {stream}; Flags: {flags}): {body}"
    ).format(type=type(self).__name__, stream=self.stream_id, flags=flags, body=body)
Frame.__repr__ = __frame_repr__


class Http2Connection(object):
    def __init__(self, connection):
        self.encoder = Encoder()
        self.decoder = Decoder()
        self.lock = threading.RLock()

        self._connection = connection

        self.initiated_streams = 0
        self.streams = {}

    def read_frame(self):
        with self.lock:
            raw_header = self.rfile.safe_read(9)
            if raw_header[:4] == b'HTTP':  # pragma no cover
                raise HttpSyntaxException("Expected HTTP2 Frame, got HTTP/1 connection")
            frame, length = Frame.parse_frame_header(raw_header)
            payload = self.rfile.safe_read(length)
            frame.parse_body(memoryview(payload))
            # TODO: max_body_size
            return frame

    def send_frame(self, *frames):
        """
        Should only be called with multiple frames that MUST be sent in sequence,
        e.g. a HEADEDRS frame and its CONTINUATION frames
        """
        with self.lock:
            for frame in frames:
                print("send frame", self.__class__.__name__, repr(frame))
                d = frame.serialize()
                self._connection.wfile.write(d)
                self._connection.wfile.flush()


    def send_headers(self, headers, stream_id, end_stream=False):
        with self.lock:
            if stream_id is None:
                stream_id = self._next_stream_id()
            raw_headers = self.encoder.encode(headers.fields)
            # FIXME: We should respect the actual connection settings
            frames = _make_header_frames(raw_headers, stream_id, FRAME_MAX_LEN, end_stream)
            self.send_frame(*frames)
            return stream_id

    def send_data(self, data, stream_id, end_stream=False):
        # FIXME: We should respect the actual connection settings
        frames = _make_data_frames(data, stream_id, FRAME_MAX_LEN, end_stream)
        for frame in frames:
            self.send_frame(frame)

    def read_headers(self, headers_frame):
        all_header_frames = self._read_all_header_frames(headers_frame)
        header_block_fragment = b"".join(frame.header_block_fragment for frame in all_header_frames)
        decoded = self.decoder.decode(header_block_fragment)
        headers = Headers(
            [[str(k).encode(), str(v).encode()] for k, v in decoded]
        )
        return all_header_frames, headers

    def _read_all_header_frames(self, headers_frame):
        frames = [headers_frame]
        while 'END_HEADERS' not in frames[-1].flags:
            # This blocks the whole connection if the client does not send header frames,
            # but that should not matter in practice.
            frame = self.read_frame()

            if not isinstance(frame, ContinuationFrame) or frame.stream_id != frames[-1].stream_id:
                raise Http2Exception("Unexpected frame: %s" % repr(frame))

            frames.append(frame)
        return frames

    def __nonzero__(self):
        return bool(self._connection)

    def __getattr__(self, item):
        return getattr(self._connection, item)

    def preface(self):
        raise NotImplementedError()

    def _next_stream_id(self):
        """
        Gets the next stream id. The caller must already hold the lock.
        """
        raise NotImplementedError()


class Http2ClientConnection(Http2Connection):
    def preface(self):
        # Check Client Preface
        expected_client_preface = CLIENT_CONNECTION_PREFACE
        actual_client_preface = self._connection.rfile.read(len(CLIENT_CONNECTION_PREFACE))
        if expected_client_preface != actual_client_preface:
            raise Http2Exception("Invalid Client preface: %s" % actual_client_preface)

        # Send Settings Frame
        settings_frame = SettingsFrame(0)  # TODO: use new hyperframe initializer
        settings_frame.settings = {
            SettingsFrame.MAX_CONCURRENT_STREAMS: 50,
            SettingsFrame.INITIAL_WINDOW_SIZE: 2 ** 31 - 1  # yolo flow control
        }
        self.send_frame(settings_frame)

        # yolo flow control
        window_update_frame = WindowUpdateFrame(0)  # TODO: use new hyperframe initializer
        window_update_frame.window_increment = 2 ** 31 - 2 ** 16
        self.send_frame(window_update_frame)

    def _next_stream_id(self):
        self.initiated_streams += 1
        # RFC 7540 5.1.1: stream 0x1 cannot be selected as a new stream identifier
        # by a client that upgrades from HTTP/1.1.
        return self.initiated_streams * 2


class Http2ServerConnection(Http2Connection):
    def connect(self):
        with self.lock:
            self._connection.connect()
            self.preface()

    def preface(self):
        self._connection.wfile.write(CLIENT_CONNECTION_PREFACE)
        self._connection.wfile.flush()

        # Send Settings Frame
        settings_frame = SettingsFrame(0)  # TODO: use new hyperframe initializer
        settings_frame.settings = {
            SettingsFrame.ENABLE_PUSH: 0,
            SettingsFrame.MAX_CONCURRENT_STREAMS: 50,
            SettingsFrame.INITIAL_WINDOW_SIZE: 2 ** 31 - 1  # yolo flow control
        }
        self.send_frame(settings_frame)

        # yolo flow control
        window_update_frame = WindowUpdateFrame(0)  # TODO: use new hyperframe initializer
        window_update_frame.window_increment = 2 ** 31 - 2 ** 16
        self.send_frame(window_update_frame)

    def _next_stream_id(self):
        self.initiated_streams += 1
        return self.initiated_streams * 2 + 1


def _make_header_frames(raw_headers, stream_id, max_frame_size, end_stream):
    frame_cls = itertools.chain((HeadersFrame,), itertools.cycle((ContinuationFrame,)))

    # TODO: We can combine _make_header_frames and _make_data_frames into a
    # `split_into_frames` method if they would have a common way of setting the data/content.
    frames = []
    for i in range(0, len(raw_headers), max_frame_size):
        frame = next(frame_cls)(stream_id)
        frame.data = raw_headers[i:i + max_frame_size]
        frames.append(frame)
    frames[-1].flags.add('END_HEADERS')
    if end_stream:
        frames[-1].flags.add('END_STREAM')
    return frames


def _make_data_frames(data, stream_id, max_frame_size, end_stream):
    frames = []
    frame_offsets = range(0, len(data), max_frame_size) or [0]
    for i in frame_offsets:
        frame = DataFrame(stream_id)
        frame.data = data[i:i + max_frame_size]
        frames.append(frame)
    if end_stream:
        frames[-1].flags.add('END_STREAM')
    return frames


def make_request(headers, body, timestamp_start, timestamp_end):
    # All HTTP/2 requests MUST include exactly one valid value for the :method, :scheme, and
    # :path pseudo-header fields, unless it is a CONNECT request
    try:
        method = headers.pop(':method')
        if method == b"CONNECT":
            raise NotImplementedError("HTTP2 CONNECT not supported")
        else:
            host = None
            port = None
        scheme = headers.pop(':scheme')
        path = headers.pop(':path')
    except KeyError:
        raise Http2Exception("Malformed HTTP2 request headers")

    return Request(
        "relative", method, scheme, host, port, path, b"HTTP/2.0",
        headers,
        body,
        timestamp_start, timestamp_end
    )


def make_response(headers, body, timestamp_start, timestamp_end):
    # All HTTP/2 responses MUST include exactly one valid value for the :status
    try:
        status = int(headers.pop(':status'))
    except (KeyError, ValueError):
        raise Http2Exception("Malformed HTTP2 response headers")

    return Response(
        b"HTTP/2.0", status, b"",
        headers,
        body,
        timestamp_start, timestamp_end
    )


def assemble_request_headers(request):
    headers = request.headers.copy()
    if request.form_out == "relative":
        # RFC 7540, 8.1.2.1:
        # All pseudo-header fields MUST appear in the header block before regular header fields.
        headers.pop(":method", None)
        headers.pop(":scheme", None)
        headers.pop(":path", None)
        headers.fields = (
            [
                [b":method", request.method],
                [b":scheme", request.scheme],
                [b":path", request.path],
            ] + headers.fields
        )
    else:
        raise NotImplementedError()
    return headers


def assemble_response_headers(response):
    headers = response.headers.copy()
    headers.pop(b":status", None)
    headers.fields.insert(0, [b":status", str(response.status_code).encode()])
    return headers


def read_nonmultiplexed_message(connection, unexpected_frame_callback=lambda _: None):
    headers_frame = connection.read_frame()

    while not isinstance(headers_frame, HeadersFrame):
        unexpected_frame_callback(headers_frame)
        headers_frame = connection.read_frame()

    all_header_frames, headers = connection.read_headers(headers_frame)
    body = b""

    if 'END_STREAM' not in all_header_frames[-1].flags:
        while True:
            f = connection.read_frame()
            if isinstance(f, DataFrame) and f.stream_id == headers_frame.stream_id:
                body += f.data
                if 'END_STREAM' in f.flags:
                    break
            else:
                unexpected_frame_callback(f)

    return headers_frame.stream_id, headers, body


def read_nonmultiplexed_request(connection, unexpected_frame_callback=lambda _: None):
    timestamp_start = time.time()
    stream_id, headers, body = read_nonmultiplexed_message(connection, unexpected_frame_callback)
    timestamp_end = time.time()
    request = make_request(headers, body, timestamp_start, timestamp_end)
    request.stream_id = stream_id
    return request


def read_nonmultiplexed_response(connection, unexpected_frame_callback=lambda _: None):
    timestamp_start = time.time()
    stream_id, headers, body = read_nonmultiplexed_message(connection, unexpected_frame_callback)
    timestamp_end = time.time()
    response = make_response(headers, body, timestamp_start, timestamp_end)
    response.stream_id = stream_id
    return response
