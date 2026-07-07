"""The :class:`TeradataAuth` value class and its per-consumer renderers.

One frozen dataclass encodes a resolved Teradata login identity. Each
consumer of that identity (tdload via CLIv2 argv; bteq via CLIv2 script
directives; teradatasql via Python-driver kwargs; dbt via env vars that
``profiles.yml``'s Jinja substitutes) has a dedicated ``render_for_*``
method. Future Airflow/Airbyte work adds more renderer methods here — no
changes to clients, tools, or generators.

Invariants (enforced in ``__post_init__``):

* TD2 / LDAP: ``username`` and ``password`` required.
* JWT: ``logdata`` required; normalized to ``token=<jwt>`` if not prefixed.
* SECRET: ``oidc_clientid`` and ``logdata`` (the client secret) required.
* BEARER: ``oidc_clientid`` and ``jws_private_key`` required.

BEARER on tdload raises :class:`AuthUnsupportedError` because CLIv2 reads
``jws_private_key`` from a config file (clispb.dat), not from argv. All
other consumer × mechanism pairs are supported.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

LogonMech = Literal["TD2", "LDAP", "JWT", "SECRET", "BEARER"]

_ALL_MECHANISMS: tuple[str, ...] = ("TD2", "LDAP", "JWT", "SECRET", "BEARER")


class AuthUnsupportedError(ValueError):
    """A consumer cannot render a :class:`TeradataAuth` for its wire format.

    Raised by renderer methods when a mechanism isn't supported on that
    transport — today, only BEARER on tdload.
    """


@dataclass(frozen=True)
class TdloadRendering:
    """The two outputs :meth:`TeradataAuth.render_for_tdload` produces."""

    job_var_entries: dict[str, str]
    env_vars: dict[str, str]


@dataclass(frozen=True)
class TeradataAuth:
    """A resolved Teradata authentication identity.

    Constructed by the resolver (either from wizard-populated
    :class:`TeradataSettings` or from a ``connections.yaml`` profile) and
    passed into clients for each operation. Immutable — call sites that
    need a different identity pass a different instance.
    """

    host: str
    port: int
    database: str
    mechanism: LogonMech

    username: str = ""
    password: str = ""
    logdata: str = ""
    oidc_clientid: str = ""
    jws_private_key: str = ""
    jws_cert: str = ""
    sslca: str = ""

    def __post_init__(self) -> None:
        mech = self.mechanism
        if mech not in _ALL_MECHANISMS:
            raise ValueError(
                f"Unknown mechanism {mech!r}. Expected one of "
                f"{', '.join(_ALL_MECHANISMS)}."
            )
        # Normalise ``database``: pydantic env-loading (and connections.yaml
        # profiles) occasionally surface the literal string "None" when the
        # user leaves a default-database field blank in the wizard or .env.
        # The pre-refactor TeradataClient silently mapped that to empty; keep
        # the behaviour centralised here so every consumer (teradatasql,
        # tdload env, dbt profile) gets the same treatment. Case-insensitive
        # match and strip surrounding whitespace.
        stripped_db = self.database.strip()
        if stripped_db.lower() == "none":
            object.__setattr__(self, "database", "")
        elif stripped_db != self.database:
            object.__setattr__(self, "database", stripped_db)
        if mech in ("TD2", "LDAP"):
            if not self.username:
                raise ValueError(f"{mech} requires username.")
            if not self.password:
                raise ValueError(f"{mech} requires password.")
        elif mech == "JWT":
            if not self.logdata:
                raise ValueError("JWT requires logdata (the JWT token).")
            # Normalise JWT logdata to CLIv2's expected "token=<jwt>" form.
            if not self.logdata.startswith("token="):
                object.__setattr__(self, "logdata", f"token={self.logdata}")
        elif mech == "SECRET":
            if not self.oidc_clientid:
                raise ValueError("SECRET requires oidc_clientid.")
            if not self.logdata:
                raise ValueError("SECRET requires logdata (the client secret).")
        elif mech == "BEARER":
            if not self.oidc_clientid:
                raise ValueError("BEARER requires oidc_clientid.")
            if not self.jws_private_key:
                raise ValueError("BEARER requires jws_private_key.")

    # ---------------------------------------------------------------
    # Identity comparison (ignores database for login-equivalence)
    # ---------------------------------------------------------------

    def same_identity_as(self, other: "TeradataAuth") -> bool:
        """Return True if *other* authenticates as the same user on the same host.

        Compares only login-relevant fields (host, port, mechanism,
        username, password, logdata, oidc_clientid, jws_private_key,
        jws_cert, sslca).  The ``database`` field is intentionally
        excluded — two auth objects that differ only in default database
        still authenticate as the same identity.
        """
        return (
            self.host == other.host
            and self.port == other.port
            and self.mechanism == other.mechanism
            and self.username == other.username
            and self.password == other.password
            and self.logdata == other.logdata
            and self.oidc_clientid == other.oidc_clientid
            and self.jws_private_key == other.jws_private_key
            and self.jws_cert == other.jws_cert
            and self.sslca == other.sslca
        )

    # ---------------------------------------------------------------
    # Renderer: tdload (CLIv2 argv + env vars)
    # ---------------------------------------------------------------

    def render_for_tdload(self, prefix: str = "Target") -> TdloadRendering:
        """Produce job-variable entries and env vars for a tdload invocation.

        ``prefix`` is ``"Target"`` for a single-instance load, or can be
        ``"Source"``/``"Target"`` in cross-instance transfers.

        Raises:
            AuthUnsupportedError: If ``mechanism`` is BEARER. tdload inherits
                CLIv2's rule that BEARER requires ``jws_private_key`` in a
                config file (clispb.dat), not on argv.
        """
        if self.mechanism == "BEARER":
            raise AuthUnsupportedError(
                "tdload cannot accept BEARER auth on the command line. "
                "Configure CLIv2 on the MCP host (clispb.dat with "
                "jws_private_key/jws_cert), or use TD2/LDAP/JWT/SECRET — "
                "a connections.yaml profile override can force a different "
                "mechanism per call."
            )

        job_vars: dict[str, str] = {}
        env: dict[str, str] = {"TdpId": self.host}

        if self.mechanism in ("TD2", "LDAP"):
            job_vars[f"{prefix}UserName"] = self.username
            job_vars[f"{prefix}UserPassword"] = self.password
            if self.mechanism == "LDAP":
                job_vars[f"{prefix}LogonMech"] = "LDAP"
            env["UserName"] = self.username
            env["UserPassword"] = self.password
            # LogonMech env var feeds the TPT DDL operator's ``@LogonMech``
            # attribute reference (see :meth:`TTUClient._prepare_tpt_ddl_script`).
            # For TD2 we still set it so the TPT script's
            # ``LogonMech = '@LogonMech'`` resolves to the literal "TD2"
            # rather than an empty string.
            env["LogonMech"] = self.mechanism
            env["LogonMechData"] = ""
        elif self.mechanism == "JWT":
            # logdata is already normalized to "token=<jwt>" in __post_init__.
            # CLIv2 syntactically requires TargetUserName even for JWT (the
            # subject the token was issued for) — tdload aborts with
            # "Value must be specified for variable 'TARGETUSERNAME'" when
            # it's empty. ``__post_init__`` does NOT require ``username`` for
            # JWT (BTEQ's ``.LOGON host/`` and the teradatasql driver both
            # work without it), so the check is render-scoped to tdload.
            if not self.username:
                raise AuthUnsupportedError(
                    "tdload with JWT requires a username (the user the JWT "
                    "was issued for, sent as TargetUserName by CLIv2). The "
                    "wizard or profile did not provide one — set "
                    "TERADATA_USERNAME in the wizard/.env or the profile's "
                    "'username' field, then retry."
                )
            job_vars[f"{prefix}UserName"] = self.username
            job_vars[f"{prefix}LogonMech"] = "JWT"
            job_vars[f"{prefix}LogonMechData"] = self.logdata
            env["UserName"] = self.username
            # UserPassword deliberately absent — JWT requires LOGON WITH NULL
            # PASSWORD on the DBS; if we set it tdload may prompt and hang.
            env["LogonMech"] = "JWT"
            env["LogonMechData"] = self.logdata
        elif self.mechanism == "SECRET":
            # CLI argv form: -u <client_id> --LogonMech SECRET --LogonMechData <bare-secret>.
            # (Distinct from BTEQ's OIDC-grant form; see render_for_bteq.)
            # ``oidc_clientid`` is required by ``__post_init__`` so this is a
            # defensive check — guards future field-emptying bugs and gives a
            # clearer error than tdload's cryptic TARGETUSERNAME complaint.
            if not self.oidc_clientid:
                raise AuthUnsupportedError(
                    "tdload with SECRET requires oidc_clientid (sent as "
                    "TargetUserName by CLIv2). The wizard or profile did not "
                    "provide one — set TERADATA_OIDC_CLIENTID in the wizard/"
                    ".env or the profile's 'oidc_clientid' field."
                )
            job_vars[f"{prefix}UserName"] = self.oidc_clientid
            job_vars[f"{prefix}LogonMech"] = "SECRET"
            job_vars[f"{prefix}LogonMechData"] = self.logdata
            env["UserName"] = self.oidc_clientid
            # UserPassword absent — SECRET doesn't take one.
            env["LogonMech"] = "SECRET"
            env["LogonMechData"] = self.logdata

        return TdloadRendering(job_var_entries=job_vars, env_vars=env)

    # ---------------------------------------------------------------
    # Renderer: bteq (CLIv2 script directives)
    # ---------------------------------------------------------------

    def render_for_bteq(self) -> list[str]:
        """Produce BTEQ header lines (``.SET LOGMECH``, ``.LOGDATA``,
        ``.CONNECTSTRING``, ``.LOGON``) for this identity.

        Called at the top of the BTEQ script before the user's statements.
        All five mechanisms supported, including BEARER.
        """
        lines: list[str] = []
        if self.mechanism == "TD2":
            lines.append(f".LOGON {self.host}/{self.username},{self.password}")
        elif self.mechanism == "LDAP":
            lines.append(".SET LOGMECH LDAP")
            lines.append(f".LOGON {self.host}/{self.username},{self.password}")
        elif self.mechanism == "JWT":
            lines.append(".SET LOGMECH JWT")
            # logdata is "token=<jwt>" after __post_init__ normalization.
            lines.append(f".LOGDATA {self.logdata}")
            lines.append(f".LOGON {self.host}/")
        elif self.mechanism == "SECRET":
            # BTEQ uses the OIDC client_credentials grant form via .LOGDATA —
            # distinct from the bare client secret that tdload's
            # LogonMechData expects.
            lines.append(".SET LOGMECH CRED")
            if self.sslca:
                lines.append(f".CONNECTSTRING SSLCA={self.sslca}")
            logdata_parts = [
                "grant_type=client_credentials",
                "scope=openid",
                f"client_id={self.oidc_clientid}",
                f"client_secret={self.logdata}",
            ]
            lines.append(f".LOGDATA {'&'.join(logdata_parts)}")
            lines.append(f".LOGON {self.host}/")
        elif self.mechanism == "BEARER":
            lines.append(".SET LOGMECH BEARER")
            connect_parts = [f"oidc_clientid={self.oidc_clientid}"]
            if self.jws_private_key:
                connect_parts.append(f"jws_private_key={self.jws_private_key}")
            if self.jws_cert:
                connect_parts.append(f"jws_cert={self.jws_cert}")
            if self.sslca:
                connect_parts.append(f"SSLCA={self.sslca}")
            lines.append(f".CONNECTSTRING {';'.join(connect_parts)}")
            lines.append(f".LOGON {self.host}/")
        return lines

    # ---------------------------------------------------------------
    # Renderer: teradatasql (Python driver kwargs)
    # ---------------------------------------------------------------

    def render_for_teradatasql(self) -> dict[str, Any]:
        """Produce kwargs suitable for ``teradatasql.connect(**kwargs)``.

        Caller is responsible for adding connection-level defaults
        (encryptdata, timeouts, etc.). This renderer covers only the
        auth-identity portion of the kwargs dict.
        """
        params: dict[str, Any] = {
            "host": self.host,
            "dbs_port": str(self.port),
            "logmech": self.mechanism,
        }
        if self.database:
            params["database"] = self.database

        if self.mechanism in ("TD2", "LDAP"):
            params["user"] = self.username
            params["password"] = self.password
        elif self.mechanism == "JWT":
            # logdata is normalized to "token=<jwt>".
            params["logdata"] = self.logdata
        elif self.mechanism == "SECRET":
            params["oidc_clientid"] = self.oidc_clientid
            params["logdata"] = self.logdata
        elif self.mechanism == "BEARER":
            params["oidc_clientid"] = self.oidc_clientid
            if self.jws_private_key:
                params["jws_private_key"] = self.jws_private_key
            if self.jws_cert:
                params["jws_cert"] = self.jws_cert

        if self.mechanism in ("BEARER", "SECRET") and self.sslca:
            params["sslca"] = self.sslca

        return params

    # ---------------------------------------------------------------
    # Renderer: dbt env vars (profiles.yml's Jinja env_var() calls)
    # ---------------------------------------------------------------

    def render_for_dbt_env(self) -> dict[str, str]:
        """Produce the full set of ``TERADATA_*`` env vars for a dbt subprocess.

        Returns **every** TERADATA_* var the profile template references,
        with empty strings for fields the active mechanism does not use.
        The caller is expected to merge this into the subprocess env *after*
        stripping any existing ``TERADATA_*`` from the parent shell — see
        :func:`sanitize_dbt_env` in the client layer.

        dbt-teradata wraps the Python driver, so all five mechanisms are
        supported.
        """
        env: dict[str, str] = {
            "TERADATA_HOST": self.host,
            "TERADATA_PORT": str(self.port),
            "TERADATA_DATABASE": self.database,
            "TERADATA_LOGMECH": self.mechanism,
            # All mechanism-specific fields default to empty so values from
            # the parent shell cannot shadow the resolved identity.
            "TERADATA_USERNAME": "",
            "TERADATA_PASSWORD": "",
            "TERADATA_LOGDATA": "",
            "TERADATA_OIDC_CLIENTID": "",
            "TERADATA_JWS_PRIVATE_KEY": "",
            "TERADATA_JWS_CERT": "",
            "TERADATA_SSLCA": "",
        }

        if self.mechanism in ("TD2", "LDAP"):
            env["TERADATA_USERNAME"] = self.username
            env["TERADATA_PASSWORD"] = self.password
        elif self.mechanism == "JWT":
            env["TERADATA_USERNAME"] = self.username
            env["TERADATA_LOGDATA"] = self.logdata
        elif self.mechanism == "SECRET":
            env["TERADATA_OIDC_CLIENTID"] = self.oidc_clientid
            env["TERADATA_LOGDATA"] = self.logdata
            env["TERADATA_SSLCA"] = self.sslca
        elif self.mechanism == "BEARER":
            env["TERADATA_OIDC_CLIENTID"] = self.oidc_clientid
            env["TERADATA_JWS_PRIVATE_KEY"] = self.jws_private_key
            env["TERADATA_JWS_CERT"] = self.jws_cert
            env["TERADATA_SSLCA"] = self.sslca

        return env

    # ---------------------------------------------------------------
    # Renderer: dbt profile YAML body
    # ---------------------------------------------------------------

    def render_for_dbt_profile_yaml(self) -> dict[str, Any]:
        """Produce the YAML body for a single target entry in ``profiles.yml``.

        Returns the dict that goes directly under
        ``<profile_name>: outputs: <target>:`` — the caller wraps it in the
        profiles.yml envelope.

        ``port`` is emitted as a string literal (``'1025'``); dbt-teradata's
        profile schema rejects ints and Jinja ``as_number``-rendered floats.
        """
        body: dict[str, Any] = {
            "type": "teradata",
            "host": "{{ env_var('TERADATA_HOST') }}",
            "port": str(self.port),
            "schema": "{{ env_var('TERADATA_DATABASE', '') }}",
            "tmode": "ANSI",
            "logmech": "{{ env_var('TERADATA_LOGMECH', 'TD2') }}",
            # Fields below are referenced unconditionally; dbt-teradata
            # ignores keys that aren't relevant to the active logmech, and
            # the Jinja default '' keeps YAML valid when they're unused.
            "user": "{{ env_var('TERADATA_USERNAME', '') }}",
            "password": "{{ env_var('TERADATA_PASSWORD', '') }}",
            "logdata": "{{ env_var('TERADATA_LOGDATA', '') }}",
            "oidc_clientid": "{{ env_var('TERADATA_OIDC_CLIENTID', '') }}",
            "jws_private_key": "{{ env_var('TERADATA_JWS_PRIVATE_KEY', '') }}",
            "jws_cert": "{{ env_var('TERADATA_JWS_CERT', '') }}",
            "sslca": "{{ env_var('TERADATA_SSLCA', '') }}",
        }
        return body
