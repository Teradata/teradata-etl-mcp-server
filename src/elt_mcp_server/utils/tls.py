"""Shared TLS configuration for outbound HTTPS clients.

Single source of truth for how the server talks TLS to external services
(Airflow, Airbyte, etc.). Certificate verification is always enabled and the
negotiated protocol is floored at TLS 1.2 — there is no option to disable it.
"""

import ssl


def build_tls_context() -> ssl.SSLContext:
    """Return an SSLContext that verifies certificates and enforces TLS 1.2+.

    Use this for every ``httpx.AsyncClient``/``httpx.Client`` ``verify`` argument
    so TLS handling is consistent across the project. The returned context:

    - performs full certificate verification (``CERT_REQUIRED`` + hostname check),
      inherited from :func:`ssl.create_default_context`; and
    - refuses to negotiate any protocol older than TLS 1.2.
    """
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    return context
