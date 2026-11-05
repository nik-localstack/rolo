import asyncio
import inspect
import json
import socket
import ssl
import threading
import time

import h2.config
import h2.connection
import h2.errors
import h2.events
import httpx
import pytest
import requests
from twisted.internet import reactor
from twisted.internet.task import Clock
from twisted.web._http2 import H2Connection, H2Stream
from twisted.web.resource import Resource
from twisted.web.server import Site
from werkzeug.test import Client as WerkzeugClient

from rolo import Request, Response, Router, route
from rolo.dispatcher import handler_dispatcher
from rolo.gateway import Gateway
from rolo.gateway.handlers import RouterHandler
from rolo.gateway.wsgi import WsgiGateway
from rolo.serving.h2 import StreamingH2Connection
from rolo.serving.twisted import HeaderPreservingHTTPChannel, TwistedGateway


@pytest.fixture
def resets() -> list[str]:
    return []


@pytest.fixture
def reset_seen(resets):
    seen = threading.Event()
    yield seen


@pytest.fixture
def stall_release():
    release = threading.Event()
    yield release
    release.set()


@pytest.fixture
def connections(monkeypatch) -> list[StreamingH2Connection]:
    created = []
    init = StreamingH2Connection.__init__

    def _init(self, *args, **kwargs):
        init(self, *args, **kwargs)
        created.append(self)

    monkeypatch.setattr(StreamingH2Connection, "__init__", _init)
    return created


@pytest.fixture
def edge_case_routes(resets, reset_seen, stall_release) -> list:
    @route("/read-later", methods=["POST"])
    def read_later(request: Request):
        stall_release.wait(timeout=10)
        try:
            return {"length": len(request.get_data())}
        except ConnectionResetError:
            resets.append(request.path)
            reset_seen.set()
            return Response(status=500)

    @route("/trailers", methods=["GET", "HEAD", "POST"])
    def trailers(request: Request):
        response = Response(b"" if request.args.get("empty") else b"body", status=200)
        response.trailers["x-done"] = "yes"
        return response

    @route("/bad-trailers", methods=["GET"])
    def bad_trailers(request: Request):
        response = Response(b"body", status=200)
        response.trailers["connection"] = "close"
        if not request.args.get("only-connection"):
            response.trailers["te"] = "gzip"
            response.trailers["x-kept"] = "yes"
        return response

    @route("/slow", methods=["GET"])
    def slow(request: Request):
        def body():
            yield b"first"
            time.sleep(2.5)
            yield b"second"

        return Response(body())

    return [read_later, trailers, bad_trailers, slow]


@pytest.fixture
def gateway(resets, reset_seen, stall_release, edge_case_routes) -> Gateway:
    router = Router(handler_dispatcher())

    @route("/echo", methods=["POST"])
    def echo(request: Request):
        # answers each chunk of the body as it arrives, and ends with the number of chunks as trailer
        response = Response(status=200, mimetype="application/echo")

        def body():
            chunks = length = 0
            try:
                while chunk := request.stream.read(65536):
                    chunks += 1
                    length += len(chunk)
                    yield chunk
            except ConnectionResetError:
                resets.append(request.path)
                reset_seen.set()
                # too late: the stream is gone
                response.trailers["x-echo-chunks"] = str(chunks)
                return
            response.trailers["x-echo-chunks"] = str(chunks)
            response.trailers["x-echo-bytes"] = str(length)

        response.response = body()
        return response

    @route("/no-body", methods=["POST", "GET"])
    def no_body(request: Request):
        response = Response(status=200)
        del response.headers["Content-Type"]
        response.headers["x-status"] = "done"
        return response

    @route("/refuse", methods=["POST"])
    def refuse(request: Request):
        return Response("refused", status=403)

    @route("/whole", methods=["POST", "GET"])
    def whole(request: Request):
        return {
            "protocol": request.environ["SERVER_PROTOCOL"],
            "length": len(request.get_data()),
            "form": request.form.to_dict(),
        }

    @route("/stall", methods=["POST"])
    def stall(request: Request):
        stall_release.wait(timeout=10)
        return Response("released")

    router.add([echo, no_body, refuse, whole, stall, *edge_case_routes])
    return Gateway(request_handlers=[RouterHandler(router, True)])


@pytest.fixture(params=["cleartext", "tls"])
def server(request, serve_twisted_tcp_server, serve_twisted_tls_server, gateway):
    site = TwistedGateway(gateway)
    if request.param == "cleartext":
        return serve_twisted_tcp_server(site), None
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    context.set_alpn_protocols(["h2"])
    return serve_twisted_tls_server(site), context


class Client:
    def __init__(self, server):
        srv, context = server
        self.server = server
        sock = socket.create_connection((srv.host, srv.port), timeout=10)
        if context:
            sock = context.wrap_socket(sock, server_hostname="localhost")
            assert sock.selected_alpn_protocol() == "h2"
        self.sock = sock
        self.conn = h2.connection.H2Connection(h2.config.H2Configuration(client_side=True))
        self.conn.initiate_connection()
        self.flush()
        self.streams = {}

    def flush(self):
        self.sock.sendall(self.conn.data_to_send())

    def request(self, path, method="POST", end_stream=False, headers=()):
        stream_id = self.conn.get_next_available_stream_id()
        self.conn.send_headers(
            stream_id,
            [
                (":method", method),
                (":path", path),
                (":scheme", "http"),
                (":authority", "localhost"),
                *headers,
            ],
            end_stream=end_stream,
        )
        self.flush()
        self.streams[stream_id] = {
            "headers": None,
            "data": b"",
            "trailers": None,
            "ended": False,
            "frames": [],
        }
        return stream_id

    def send(self, stream_id, data, end_stream=False):
        while data:
            size = min(
                self.conn.local_flow_control_window(stream_id),
                self.conn.max_outbound_frame_size,
                len(data),
            )
            if size <= 0:
                self.receive()
                continue
            self.conn.send_data(stream_id, data[:size], end_stream=end_stream and size == len(data))
            self.flush()
            data = data[size:]

    def end(self, stream_id):
        self.conn.end_stream(stream_id)
        self.flush()

    def receive(self):
        data = self.sock.recv(65535)
        assert data, "server closed the connection"
        for event in self.conn.receive_data(data):
            stream = self.streams.get(getattr(event, "stream_id", None))
            if stream is None:
                continue
            stream["frames"].append(type(event).__name__)
            if isinstance(event, h2.events.ResponseReceived):
                stream["headers"] = event.headers
                stream["headers_ended_stream"] = event.stream_ended is not None
            elif isinstance(event, h2.events.TrailersReceived):
                stream["trailers"] = event.headers
            elif isinstance(event, h2.events.DataReceived):
                stream["data"] += event.data
                self.conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
            elif isinstance(event, h2.events.StreamEnded):
                stream["ended"] = True
            elif isinstance(event, h2.events.StreamReset):
                stream["ended"] = True
                stream["reset"] = event.error_code
        self.flush()

    def wait(self, condition, timeout=10):
        deadline = time.monotonic() + timeout
        while not condition():
            assert time.monotonic() < deadline, "timed out"
            self.receive()

    def ping(self):
        self.conn.ping(b"keepbusy")
        self.flush()

    def close(self):
        self.sock.close()


def test_request_dispatched_at_headers_and_streamed_both_ways(server):
    client = Client(server)
    stream_id = client.request("/echo")

    # each chunk comes back before the request ends, so the handler runs while the client still sends
    for i in range(3):
        chunk = f"chunk-{i};".encode()
        client.send(stream_id, chunk)
        client.wait(lambda chunk=chunk: client.streams[stream_id]["data"].endswith(chunk))
    assert dict(client.streams[stream_id]["headers"])[b"content-type"] == b"application/echo"

    client.end(stream_id)
    client.wait(lambda: client.streams[stream_id]["ended"])
    assert client.streams[stream_id]["trailers"] == [
        (b"x-echo-chunks", b"3"),
        (b"x-echo-bytes", b"24"),
    ]
    client.close()


def test_request_body_flow_control(server):
    # more data than the initial flow control windows, both ways
    client = Client(server)
    stream_id = client.request("/echo")
    body = bytes(range(256)) * 4096 * 2  # 2 MiB
    client.send(stream_id, body, end_stream=True)
    client.wait(lambda: client.streams[stream_id]["ended"])
    assert client.streams[stream_id]["data"] == body
    assert dict(client.streams[stream_id]["trailers"])[b"x-echo-bytes"] == str(len(body)).encode()
    client.close()


def test_empty_response_is_a_single_headers_frame(server):
    client = Client(server)
    stream_id = client.request("/no-body", end_stream=True)
    client.wait(lambda: client.streams[stream_id]["ended"])
    stream = client.streams[stream_id]
    assert stream["headers_ended_stream"]
    assert stream["frames"] == ["ResponseReceived", "StreamEnded"]
    assert (b"x-status", b"done") in stream["headers"]
    client.close()


def test_response_before_the_request_ends(server):
    client = Client(server)
    stream_id = client.request("/refuse")
    # more than the stream's window, which the handler never reads to open
    client.send(stream_id, b"x" * 100_000)
    client.wait(lambda: client.streams[stream_id]["ended"])
    assert client.streams[stream_id]["data"] == b"refused"
    # the client may still finish its request
    client.send(stream_id, b"late data" * 10_000, end_stream=True)

    # the connection still serves other streams, and its flow control window was not used up
    other = client.request("/whole")
    client.send(other, b"y" * 200_000, end_stream=True)
    client.wait(lambda: client.streams[other]["ended"])
    assert json.loads(client.streams[other]["data"])["length"] == 200_000
    client.close()


def test_early_response_to_an_upload_with_httpx(server):
    srv, context = server

    async def upload():
        async with httpx.AsyncClient(
            http1=False, http2=True, verify=False, trust_env=False
        ) as client:
            return await client.post(srv.url + "/refuse", content=b"x" * 300_000)

    # the handler answers before reading a body larger than the windows
    response = asyncio.run(upload())
    assert response.status_code == 403
    assert response.text == "refused"


def test_responded_stream_lets_the_connection_idle_out(serve_twisted_tcp_server, gateway):
    site = TwistedGateway(gateway)
    site.timeOut = 1
    client = Client((serve_twisted_tcp_server(site), None))
    stream_id = client.request("/refuse")
    client.send(stream_id, b"x" * 1000)
    client.wait(lambda: client.streams[stream_id]["ended"])

    # the request stays open, but its response is complete, so the idle connection is closed
    client.sock.settimeout(5)
    try:
        while client.sock.recv(65535):
            pass
    except socket.timeout:
        pytest.fail("the idle connection was not closed")
    client.close()


def test_handler_answering_slowly_keeps_the_connection(serve_twisted_tcp_server, gateway):
    site = TwistedGateway(gateway)
    site.timeOut = 1
    client = Client((serve_twisted_tcp_server(site), None))
    stream_id = client.request("/slow", method="GET", end_stream=True)

    # silent for longer than the idle timeout, while the handler answers a complete request
    client.wait(lambda: client.streams[stream_id]["ended"])
    assert client.streams[stream_id]["data"] == b"firstsecond"
    client.close()


def test_silent_streams_free_their_threads(serve_twisted_tcp_server, gateway):
    site = TwistedGateway(gateway)
    site.timeOut = 1
    srv = serve_twisted_tcp_server(site)
    client = Client((srv, None))

    # more handlers waiting for a body that never comes than the server has threads
    for _ in range(reactor.getThreadPool().max + 2):
        client.request("/whole")

    # the connection itself stays busy, so only the streams' own silence can time them out
    def keep_busy():
        while not done.is_set():
            client.ping()
            done.wait(0.2)

    done = threading.Event()
    pinger = threading.Thread(target=keep_busy)
    pinger.start()
    try:
        response = requests.get(srv.url + "/whole", timeout=10)
    except requests.Timeout:
        pytest.fail("the silent streams kept every thread")
    finally:
        done.set()
        pinger.join()
    assert response.json()["protocol"] == "HTTP/1.1"
    client.close()


def test_client_reset_reaches_the_handler(server, resets, reset_seen, connections):
    client = Client(server)
    stream_id = client.request("/echo")
    client.send(stream_id, b"hello")
    client.wait(lambda: client.streams[stream_id]["data"] == b"hello")
    client.conn.reset_stream(stream_id, h2.errors.ErrorCodes.CANCEL)
    client.flush()
    assert reset_seen.wait(5)

    other = client.request("/whole", method="GET", end_stream=True)
    client.wait(lambda: client.streams[other]["ended"])
    # trailers the handler set after the reset are dropped with the stream
    assert all(not connection.conn.pendingTrailers for connection in connections)
    client.close()


def test_connection_lost_mid_body_reaches_the_handler(server, resets, reset_seen):
    client = Client(server)
    stream_id = client.request("/echo")
    client.send(stream_id, b"hello")
    client.wait(lambda: client.streams[stream_id]["data"] == b"hello")
    client.close()
    assert reset_seen.wait(5)
    assert resets == ["/echo"]


def test_reset_after_the_body_ended_reaches_the_handler(server, resets, reset_seen, stall_release):
    client = Client(server)
    stream_id = client.request("/read-later")
    client.send(stream_id, b"x" * 1000, end_stream=True)
    client.conn.reset_stream(stream_id, h2.errors.ErrorCodes.CANCEL)
    client.flush()
    # a body the client sent but reset before the handler read it is lost, not silently empty
    time.sleep(0.2)
    stall_release.set()
    assert reset_seen.wait(5)
    assert resets == ["/read-later"]
    client.close()


def test_trailers_end_an_empty_response(server):
    client = Client(server)
    stream_id = client.request("/trailers?empty=1", method="GET", end_stream=True)
    client.wait(lambda: client.streams[stream_id]["ended"])
    stream = client.streams[stream_id]
    assert stream["frames"] == ["ResponseReceived", "TrailersReceived", "StreamEnded"]
    assert stream["trailers"] == [(b"x-done", b"yes")]
    client.close()


def test_head_response_has_no_trailers(server):
    client = Client(server)
    stream_id = client.request("/trailers", method="HEAD", end_stream=True)
    client.wait(lambda: client.streams[stream_id]["ended"])
    assert client.streams[stream_id]["frames"] == ["ResponseReceived", "StreamEnded"]
    client.close()


@pytest.mark.parametrize("query, kept", [("", [(b"x-kept", b"yes")]), ("?only-connection=1", None)])
def test_trailers_forbidden_in_http2_are_dropped(server, query, kept):
    client = Client(server)
    stream_id = client.request("/bad-trailers" + query, method="GET", end_stream=True)
    client.wait(lambda: client.streams[stream_id]["ended"])
    assert "reset" not in client.streams[stream_id]
    assert client.streams[stream_id]["data"] == b"body"
    assert client.streams[stream_id]["trailers"] == kept

    # the connection still serves other streams
    other = client.request("/whole", method="GET", end_stream=True)
    client.wait(lambda: client.streams[other]["ended"])
    client.close()


def test_empty_response_completed_synchronously(serve_twisted_tcp_server):
    class NoContent(Resource):
        isLeaf = True

        def render_GET(self, request):
            request.setResponseCode(204)
            return b""

    site = Site(NoContent())
    site.protocol = HeaderPreservingHTTPChannel.protocol_factory
    client = Client((serve_twisted_tcp_server(site), None))
    first = client.request("/", method="GET", end_stream=True)
    client.wait(lambda: client.streams[first]["ended"])
    second = client.request("/", method="GET", end_stream=True)
    client.wait(lambda: client.streams[second]["ended"])
    assert dict(client.streams[second]["headers"])[b":status"] == b"204"
    client.close()


def test_form_and_whole_body_requests(server):
    srv, context = server
    url = srv.url + "/whole"

    async def requests():
        # one connection, which a sync client can't share between threads over TLS
        async with httpx.AsyncClient(
            http1=False, http2=True, verify=False, trust_env=False
        ) as client:
            form = await client.post(url, data={"a": "1", "b": "two"})
            large = await client.post(url, content=b"z" * 3_000_000)
            concurrent = await asyncio.gather(
                *[client.post(url, content=b"c" * 100_000) for _ in range(10)]
            )
            return form, large, concurrent

    form, large, concurrent = asyncio.run(requests())
    assert form.http_version == "HTTP/2"
    assert form.json() == {"protocol": "HTTP/2", "length": 9, "form": {"a": "1", "b": "two"}}
    assert large.json()["length"] == 3_000_000
    assert [response.json()["length"] for response in concurrent] == [100_000] * 10


def test_unread_body_does_not_stall_the_connection(server, stall_release):
    # a handler that never reads its body holds back only its own stream's window
    client = Client(server)
    stalled = client.request("/stall")
    # nearly all of the connection's initial 64 KiB window
    client.send(stalled, b"s" * 65_000)
    other = client.request("/whole")
    client.send(other, b"o" * 200_000, end_stream=True)
    client.wait(lambda: client.streams[other]["ended"])
    assert json.loads(client.streams[other]["data"])["length"] == 200_000
    stall_release.set()
    client.close()


@pytest.mark.parametrize("serve", ["werkzeug", "wsgi", "asgi", "twisted"])
def test_trailers_ignored_without_http2_streaming(
    serve, gateway, serve_wsgi_gateway, serve_asgi_gateway, serve_twisted_gateway
):
    if serve == "werkzeug":
        response = WerkzeugClient(WsgiGateway(gateway)).get("/trailers")
        assert response.get_data() == b"body"
        return
    srv = {
        "wsgi": serve_wsgi_gateway,
        "asgi": serve_asgi_gateway,
        "twisted": serve_twisted_gateway,
    }[serve](gateway)
    response = requests.get(srv.url + "/trailers")
    assert response.status_code == 200
    assert response.content == b"body"


def test_twisted_internals_streaming_relies_on():
    # StreamingH2Connection overrides or calls these private parts of twisted's HTTP/2 support
    signatures = {
        "_requestReceived": ["event"],
        "_requestDataReceived": ["event"],
        "_requestEnded": ["event"],
        "_requestAborted": ["event"],
        "_requestDone": ["streamID"],
        "_streamIsActive": ["streamID"],
        "_tryToWriteControlData": [],
        "writeHeaders": ["version", "code", "reason", "headers", "streamID"],
        "writeDataToStream": ["streamID", "data"],
        "endRequest": ["streamID"],
        "connectionLost": ["reason", "_cancelTimeouts"],
        "timeoutConnection": [],
    }
    for name, parameters in signatures.items():
        assert list(inspect.signature(getattr(H2Connection, name)).parameters)[1:] == parameters, (
            name
        )

    # the send loop ends a stream on the h2 connection, which is where the trailers are sent
    assert "self.conn.end_stream(" in inspect.getsource(H2Connection._sendPrioritisedData)
    connection = H2Connection(Clock())
    assert isinstance(connection.conn, h2.connection.H2Connection)
    assert {"priority", "streams", "_reactor", "timeOut"} <= set(dir(connection))
    stream_init = inspect.getsource(H2Stream.__init__)
    assert "self._request = " in stream_init and "self._conn = " in stream_init
