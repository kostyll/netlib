from __future__ import absolute_import, print_function, division
import itertools
import threading
import time

from hpack.hpack import Encoder, Decoder

from ...exceptions import Http2Exception
from .. import Headers, Request, Response
from .frame import (
    Frame, WindowUpdateFrame, SettingsFrame, ContinuationFrame, HeadersFrame, DataFrame
)

CLIENT_CONNECTION_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"


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
            return Frame.from_file(self.rfile)  # TODO: max_body_size

    def send_frame(self, *frames):
        """
        Should only be called with multiple frames that MUST be sent in sequence,
        e.g. a HEADEDRS frame and its CONTINUATION frames
        """
        with self.lock:
            for frame in frames:
                print("send frame", self.__class__.__name__, repr(frame))
                # TODO: Remove once we decided which frames to use.
                d = frame.to_bytes() if hasattr(frame, "to_bytes") else frame.serialize()
                self._connection.wfile.write(d)
                self._connection.wfile.flush()

    # FIXME: We should respect the actual connection settings
    max_frame_size = 2 ** 14

    def send_headers(self, headers, stream_id, end_stream=False):
        with self.lock:
            if stream_id is None:
                stream_id = self._next_stream_id()
            frames = self._make_header_frames(headers, stream_id)
            if end_stream:
                frames[-1].flags |= Frame.FLAG_END_STREAM
            self.send_frame(*frames)
            return stream_id

    def _make_header_frames(self, headers, stream_id):
        frame_cls = itertools.chain((HeadersFrame,), itertools.cycle((ContinuationFrame,)))
        raw_headers = self.encoder.encode(headers.fields)

        # TODO: We can combine _make_header_frames and _make_data_frames into a
        # `split_into_frames` method if they would have a common way of setting the data/content.
        frames = []
        for i in range(0, len(raw_headers), self.max_frame_size):
            frame = next(frame_cls)(stream_id=stream_id)
            frame.header_block_fragment = raw_headers[i:i+self.max_frame_size]
            frames.append(frame)
        frames[-1].flags = Frame.FLAG_END_HEADERS
        return frames

    def send_data(self, data, stream_id, end_stream=False):
        frames = self._make_data_frames(data, stream_id)
        if end_stream:
            frames[-1].flags |= Frame.FLAG_END_STREAM
        for frame in frames:
            self.send_frame(frame)

    def _make_data_frames(self, data, stream_id):
        frames = []
        for i in range(0, len(data), self.max_frame_size):
            frame = DataFrame(stream_id=stream_id)
            frame.payload = data[i:i + self.max_frame_size]
            frames.append(frame)
        return frames

    def read_headers(self, headers_frame):
        all_header_frames = self._read_all_header_frames(headers_frame)
        header_block_fragment = b"".join(frame.header_block_fragment for frame in all_header_frames)
        headers = Headers(
            [[str(k), str(v)] for k, v in self.decoder.decode(header_block_fragment)]
        )
        return all_header_frames, headers

    def _read_all_header_frames(self, headers_frame):
        frames = [headers_frame]
        while not frames[-1].flags & Frame.FLAG_END_HEADERS:
            # This blocks the whole connection if the client does not send header frames,
            # but that should not matter in practice.
            with self.lock:
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
        settings_frame = SettingsFrame(settings={
            SettingsFrame.SETTINGS.SETTINGS_MAX_CONCURRENT_STREAMS: 50,
            SettingsFrame.SETTINGS.SETTINGS_INITIAL_WINDOW_SIZE: 2 ** 31 - 1  # yolo flow control
        })
        self.send_frame(settings_frame)

        # yolo flow control
        window_update_frame = WindowUpdateFrame(stream_id=0,
                                                window_size_increment=2 ** 31 - 2 ** 16)
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
        settings_frame = SettingsFrame(settings={
            SettingsFrame.SETTINGS.SETTINGS_ENABLE_PUSH: 0,
            SettingsFrame.SETTINGS.SETTINGS_MAX_CONCURRENT_STREAMS: 50,
            SettingsFrame.SETTINGS.SETTINGS_INITIAL_WINDOW_SIZE: 2 ** 31 - 1  # yolo flow control
        })
        self.send_frame(settings_frame)

        # yolo flow control
        window_update_frame = WindowUpdateFrame(stream_id=0,
                                                window_size_increment=2 ** 31 - 2 ** 16)
        self.send_frame(window_update_frame)

    def _next_stream_id(self):
        self.initiated_streams += 1
        return self.initiated_streams * 2 + 1


def make_request(headers, body, timestamp_start, timestamp_end):
    # All HTTP/2 requests MUST include exactly one valid value for the :method, :scheme, and
    # :path pseudo-header fields, unless it is a CONNECT request
    try:
        # TODO: Possibly .pop()?
        method = headers[':method']
        if method == "CONNECT":
            raise NotImplementedError("HTTP2 CONNECT not supported")
        else:
            host = None
            port = None
        scheme = headers[':scheme']
        path = headers[':path']
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
        # TODO: Possibly .pop()?
        status = int(headers[':status'])
    except KeyError:
        raise Http2Exception("Malformed HTTP2 response headers")

    return Response(
        b"HTTP/2.0", status, b"",
        headers,
        body,
        timestamp_start, timestamp_end
    )


def read_nonmultiplexed_message(connection, unexpected_frame_callback=lambda _: None):
    headers_frame = connection.read_frame()

    while not isinstance(headers_frame, HeadersFrame):
        unexpected_frame_callback(headers_frame)
        headers_frame = connection.read_frame()

    all_header_frames, headers = connection.read_headers(headers_frame)
    body = b""

    if not all_header_frames[-1].flags & Frame.FLAG_END_STREAM:
        while True:
            f = connection.read_frame()
            if isinstance(f, DataFrame) and f.stream_id == headers_frame.stream_id:
                body += f.payload
                if f.flags & Frame.FLAG_END_STREAM:
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


def assemble_request_headers(request):
    # TODO: We now have duplication between the pseudo-headers (:method, ...)
    # and request.method. Ideally, we should unfiddle that here.
    raise NotImplementedError()


def assemble_response_headers(response):
    # See assemble_request_headers
    raise NotImplementedError()
