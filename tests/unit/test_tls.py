"""Unit tests for the shared TLS context helper."""

import ssl

from elt_mcp_server.utils.tls import build_tls_context


class TestBuildTlsContext:
    def test_returns_ssl_context(self):
        ctx = build_tls_context()
        assert isinstance(ctx, ssl.SSLContext)

    def test_enforces_tls_1_2_minimum(self):
        ctx = build_tls_context()
        assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2

    def test_certificate_verification_required(self):
        ctx = build_tls_context()
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.check_hostname is True

    def test_returns_fresh_context_each_call(self):
        assert build_tls_context() is not build_tls_context()
