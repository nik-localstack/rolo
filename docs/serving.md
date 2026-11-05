Serving
=======

This guide shows you how to serve Rolo components through different Python web server technologies.

WSGI
----

### Serving a Router as WSGI app

If you only need a `Router` instance to serve your application, you can convert to a WSGI app using the `Router.wsgi()` method.

```python
from rolo import Router, route
from rolo.routing import handler_dispatcher

@route("/")
def index(request):
    return "hello world"

router = Router(dispatcher=handler_dispatcher())
router.add(index)

app = router.wsgi()
```

Now you can use any old WSGI compliant server to serve the application.
For example, if this file is stored in `myapp.py`, using gunicorn, you can:

```sh
pip install gunicorn
gunicorn -w 4 myapp:app
```

### Serving a Gateway as WSGI app

Unless you need Websockets, the Rolo Request object is fully WSGI compliant, so you can also use any WSGI server to serve a `Gateway`.
Simply use the `WSGIGateway` adapter.

```python
from rolo.gateway import Gateway
from rolo.gateway.wsgi import WsgiGateway

gateway: Gateway = ...

app = WsgiGateway(gateway)
```

Similar to the previous example, you can serve the `app` object through any WSGI compliant server.

ASGI
----

ASGI servers like Hypercorn allow asynchronous server communication, for example for Websockets.
Gateways can be served through the `AsgiGateway` adapter, which exposes a `Gateway` as an ASGI3 application.
Under the hood, it uses our own ASGI/WSGI bridge (`AsgiAdapter`), and converts ASGI calls to WSGI calls for regular HTTP requests, and uses ASGI websockets for serving rolo websockets.
File `myapp.py`:

```python
from rolo.gateway import Gateway
from rolo.gateway.asgi import AsgiGateway

gateway: Gateway = ...

app = AsgiGateway(gateway)
```

Now you can use Hypercorn or other ASGI servers to serve the `app` object.

```sh
pip install hypercorn
hypercorn myapp:app
```

Twisted
-------

Rolo can be served through [Twisted](https://twisted.org/), which supports both WSGI and Websockets.
You will need twisted, and wsproto installed `pip install twisted wsproto`.

```python
from rolo.gateway import Gateway
from rolo.serving.twisted import TwistedGateway
from twisted.internet import endpoints, reactor

gateway: Gateway = ...

# Rolo/Twisted adapter, that exposes a Rolo Gateway as a twisted.web.server.Site object
site = TwistedGateway(gateway)

endpoint = endpoints.TCP4ServerEndpoint(reactor, 8000)
endpoint.listen(site)

reactor.run()
```

### HTTP/2 streaming and trailers

With HTTP/2 support installed (`pip install twisted[http2]`), the Twisted server dispatches an HTTP/2 request as soon as its headers arrive.
The handler reads the request body from `request.stream` while the client is still sending it, and the response body is sent as it is produced, so a single request can stream in both directions.
A handler that reads slowly slows the client down through HTTP/2 flow control.
Each open stream holds a thread of the server's thread pool while its handler runs.
A read of the request body raises a `TimeoutError` once the stream was silent in both directions for the site's `timeOut`, so a stream waiting on a silent client releases its thread.

A response can also end with trailers, set on `Response.trailers`.
They are sent once the response body is exhausted, so they can still be added while the body is produced:

```python
from rolo import Request, Response, route


@route("/echo", methods=["POST"])
def echo(request: Request):
    response = Response(mimetype="application/octet-stream")

    def body():
        total = 0
        while chunk := request.stream.read(65536):
            total += len(chunk)
            yield chunk
        response.trailers["x-echo-bytes"] = str(total)

    response.response = body()
    return response
```

Trailers are only sent by the Twisted server over HTTP/2; other servers and HTTP/1.1 responses ignore them.
