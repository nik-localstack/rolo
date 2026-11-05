"""
Streaming HTTP/2 for the twisted server: a request is dispatched to the gateway as soon as its headers arrive,
with a request body that the handler reads while the client is still sending it, and a response that can end
with trailers. This allows full-duplex streaming over a single HTTP/2 stream. Twisted's own ``H2Connection``
dispatches a request only once its whole body has arrived, and can't send trailers.
"""

import io
import logging
import threading
import time
from collections import deque

import h2.connection
import h2.errors
import h2.exceptions
from twisted.web._http2 import H2Connection

LOG = logging.getLogger(__name__)

H2Headers = list[tuple[bytes, bytes]]

# connection-specific fields, which HTTP/2 does not allow (RFC 9113, section 8.2.2)
CONNECTION_HEADERS = (
    b"connection",
    b"keep-alive",
    b"proxy-connection",
    b"transfer-encoding",
    b"upgrade",
)
# HTTP/2's initial connection flow control window, which SETTINGS do not change
CONNECTION_WINDOW = 65535


class RequestBodyPipe(io.RawIOBase):
    """
    The body of a request that is still arriving, as the request's ``content``. Reads block until the client has
    sent more, and the stream's flow control window opens again only as the handler takes the data, so a handler
    that reads slowly slows the client down instead of the server buffering the body. Its ``tell()`` is 0, so
    twisted leaves form submissions to the application. A read gives up with a ``TimeoutError`` once the stream
    was silent both ways for the connection's ``timeOut``, so a stream waiting on a silent client frees its thread.
    """

    def __init__(self, connection: "StreamingH2Connection", streamID: int):
        super().__init__()
        self._connection = connection
        self._streamID = streamID
        self._condition = threading.Condition()
        self._chunks: deque[tuple[bytes, int]] = deque()
        self._buffer = bytearray()
        self._ended = False
        self._error: Exception | None = None
        self._discarded = False
        self._active = time.monotonic()

    @property
    def remoteClosed(self) -> bool:
        return self._ended or self._error is not None

    # called in the reactor thread

    def touch(self) -> None:
        self._active = time.monotonic()

    def feed(self, data: bytes, flowControlledLength: int) -> None:
        self.touch()
        with self._condition:
            if self._discarded:
                # the response is complete, and the client may finish sending a request nobody reads
                self._connection._creditWindow(flowControlledLength, self._streamID)
                return
            self._chunks.append((data, flowControlledLength))
            self._condition.notify_all()

    def end(self) -> None:
        with self._condition:
            self._ended = True
            self._condition.notify_all()

    def fail(self, error: Exception) -> None:
        with self._condition:
            # a body that ended but wasn't read yet is lost too
            if not self._ended or self._chunks:
                self._error = error
            self._condition.notify_all()

    def close(self) -> None:
        # twisted closes the content once the response finished: what the handler never read is dropped,
        # and its window opened. It stays open, so a late read answers b"" instead of raising.
        with self._condition:
            if self._discarded:
                return
            self._discarded = True
            unread = sum(flowControlledLength for _, flowControlledLength in self._chunks)
            self._chunks.clear()
            self._condition.notify_all()
        self._connection._reactor.callFromThread(
            self._connection._creditWindow, unread, self._streamID
        )

    # called by the handler

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        if not self._buffer and not self._fill():
            return 0
        size = min(len(buffer), len(self._buffer))
        buffer[:size] = self._buffer[:size]
        del self._buffer[:size]
        return size

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if offset or whence != io.SEEK_SET:
            raise io.UnsupportedOperation("the request body is not seekable")
        return 0

    def tell(self) -> int:
        return 0

    def _fill(self) -> bool:
        """Waits for data; answers whether there is any (``False`` at the end of the body)."""
        timeout = self._connection.timeOut
        with self._condition:
            while not self._chunks:
                if self._error is not None:
                    raise ConnectionResetError(f"the request body was aborted: {self._error}")
                if self._ended or self._discarded:
                    return False
                if timeout is None:
                    self._condition.wait()
                    continue
                if (remaining := self._active + timeout - time.monotonic()) <= 0:
                    raise TimeoutError(f"no request body data for {timeout} seconds")
                self._condition.wait(remaining)
            data, flowControlledLength = self._chunks.popleft()
        self._buffer += data
        self._connection._reactor.callFromThread(
            self._connection._creditWindow, flowControlledLength, self._streamID
        )
        return True


class _TrailingH2Connection(h2.connection.H2Connection):
    """
    Ends a stream with its pending trailers instead of an empty DATA frame. Twisted's send loop ends a stream
    once it has sent all data queued before the end, which is exactly when the trailers are due.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pendingTrailers: dict[int, H2Headers] = {}

    def end_stream(self, stream_id: int) -> None:
        if not (trailers := self.pendingTrailers.pop(stream_id, None)):
            super().end_stream(stream_id)
            return
        try:
            self.send_headers(stream_id, trailers, end_stream=True)
        except (h2.exceptions.ProtocolError, IndexError) as e:
            # twisted's send loop doesn't expect an error here, so only this stream fails
            LOG.warning("Invalid trailers for stream %s: %s", stream_id, e)
            self.reset_stream(stream_id, h2.errors.ErrorCodes.INTERNAL_ERROR)


class StreamingH2Connection(H2Connection):
    """
    An ``H2Connection`` that dispatches each request at its headers, with a ``RequestBodyPipe`` as its content,
    and ends a response with the trailers set via ``setTrailers``. A response without any body is sent as a
    single HEADERS frame that ends the stream (RFC 9113, section 8.1).
    """

    def __init__(self, reactor=None):
        super().__init__(reactor)
        self.conn = _TrailingH2Connection(config=self.conn.config)
        self._bodies: dict[int, RequestBodyPipe] = {}
        # held back until the first data or the end of the response, to end an empty response with them
        self._pendingHeaders: dict[int, H2Headers] = {}
        # streams whose response is complete while the client still sends
        self._responded: set[int] = set()
        # flow control credit not yet sent, by stream (``None`` for the connection)
        self._unsentWindow: dict[int | None, int] = {}

    def setTrailers(self, streamID: int, trailers: H2Headers) -> None:
        """Sets the trailers that end the stream's response, without the fields HTTP/2 forbids in them."""
        if streamID not in self.streams:
            return
        if trailers := [
            (name, value)
            for name, value in trailers
            if not name.startswith(b":")
            and name not in CONNECTION_HEADERS
            and (name != b"te" or value == b"trailers")
        ]:
            self.conn.pendingTrailers[streamID] = trailers

    # inbound

    def _requestReceived(self, event):
        super()._requestReceived(event)
        stream = self.streams[event.stream_id]
        body = self._bodies[event.stream_id] = RequestBodyPipe(self, event.stream_id)
        # twisted made a temporary file for the body
        stream._request.content.close()
        stream._request.content = body
        stream._request.requestReceived(stream.command, stream.path, b"HTTP/2")

    def _requestDataReceived(self, event):
        # only the stream's window waits for the handler: a stream whose body isn't read must not stall the
        # connection's other streams
        self._creditWindow(event.flow_controlled_length)
        # a response completed right away may have ended the stream already
        if body := self._bodies.get(event.stream_id):
            body.feed(event.data, event.flow_controlled_length)

    def _requestEnded(self, event):
        if body := self._bodies.get(event.stream_id):
            body.end()
        if event.stream_id in self._responded:
            self._requestDone(event.stream_id)

    def _requestAborted(self, event):
        if body := self._bodies.get(event.stream_id):
            body.fail(ConnectionError(f"stream reset with code {event.error_code}"))
        super()._requestAborted(event)

    def connectionLost(self, reason, _cancelTimeouts=True):
        for body in self._bodies.values():
            body.fail(reason.value)
        super().connectionLost(reason, _cancelTimeouts)

    # outbound

    def writeHeaders(self, version, code, reason, headers, streamID):
        self._pendingHeaders[streamID] = [(b":status", code), *headers]

    def writeDataToStream(self, streamID, data):
        self._bodies[streamID].touch()
        self._flushHeaders(streamID)
        if data:
            super().writeDataToStream(streamID, data)

    def endRequest(self, streamID):
        if streamID in self._pendingHeaders and streamID not in self.conn.pendingTrailers:
            if self._flushHeaders(streamID, endStream=True) and self._streamIsActive(streamID):
                self._requestDone(streamID)
            return
        self._flushHeaders(streamID)
        super().endRequest(streamID)

    def _flushHeaders(self, streamID: int, endStream: bool = False) -> bool:
        if (headers := self._pendingHeaders.pop(streamID, None)) is None:
            return False
        try:
            self.conn.send_headers(streamID, headers, end_stream=endStream)
        except h2.exceptions.StreamClosedError:
            return False
        return self._tryToWriteControlData()

    def _requestDone(self, streamID):
        stream = self.conn.streams.get(streamID)
        if (
            streamID not in self._responded
            and not self._bodies[streamID].remoteClosed
            and stream is not None
            and not stream.closed
        ):
            # the client may still send: the stream stays until it is done, so twisted keeps accounting for it
            self._responded.add(streamID)
            self.priority.block(streamID)
            return
        self._responded.discard(streamID)
        self._bodies.pop(streamID)
        self._pendingHeaders.pop(streamID, None)
        self.conn.pendingTrailers.pop(streamID, None)
        self._unsentWindow.pop(streamID, None)
        super()._requestDone(streamID)

    def _creditWindow(self, length: int, streamID: int | None = None) -> None:
        # sent once half the window is due: twisted drops a connection that buffers too many control frames
        # while the client doesn't read
        if self.transport is None or (streamID is not None and streamID not in self.streams):
            return
        length += self._unsentWindow.pop(streamID, 0)
        window = (
            CONNECTION_WINDOW if streamID is None else self.conn.local_settings.initial_window_size
        )
        if length < window // 2:
            self._unsentWindow[streamID] = length
            return
        try:
            self.conn.increment_flow_control_window(length, stream_id=streamID)
        except h2.exceptions.ProtocolError:
            return  # the stream ended meanwhile
        self._tryToWriteControlData()

    def timeoutConnection(self):
        if any(
            self._bodies[streamID].remoteClosed for streamID in set(self.streams) - self._responded
        ):
            # a handler still answering a complete request may be silent between messages; streams that wait
            # on the client time out with it
            self.resetTimeout()
            return
        super().timeoutConnection()
