import datetime

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from twisted.internet import ssl
from twisted.protocols.tls import TLSMemoryBIOFactory

from rolo.testing.pytest import _ServerInfo, get_random_tcp_port, wait_server_is_up


@pytest.fixture(scope="module")
def self_signed_cert() -> tuple[bytes, bytes]:
    """A self-signed certificate and private key for ``localhost``, both PEM encoded."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
        .sign(key, hashes.SHA256())
    )
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return cert.public_bytes(serialization.Encoding.PEM), key_pem


@pytest.fixture
def serve_twisted_tls_server(twisted_reactor, self_signed_cert):
    """Like ``serve_twisted_tcp_server``, but serves the protocol factory over TLS. The server accepts ``h2`` and
    ``http/1.1`` via ALPN."""
    ports = []

    def _create(protocol_factory, scheme: str = "https"):
        cert_pem, key_pem = self_signed_cert
        certificate = ssl.PrivateCertificate.loadPEM(cert_pem + key_pem)
        options = ssl.CertificateOptions(
            privateKey=certificate.privateKey.original,
            certificate=certificate.original,
            acceptableProtocols=[b"h2", b"http/1.1"],
        )
        factory = TLSMemoryBIOFactory(options, False, protocol_factory)

        port = get_random_tcp_port()
        ports.append(twisted_reactor.listenTCP(port, factory))
        srv = _ServerInfo("localhost", port, f"{scheme}://localhost:{port}")
        assert wait_server_is_up(srv), f"gave up waiting for {srv}"
        return srv

    yield _create

    for _port in ports:
        _port.stopListening()
