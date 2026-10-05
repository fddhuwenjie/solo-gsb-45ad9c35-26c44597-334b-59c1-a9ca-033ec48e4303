from __future__ import annotations

import base64
import binascii
import hashlib
import json
from decimal import Decimal

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from .config import Settings, get_settings
from .db import Database
from .decoder import decode_stream
from .encoder import encode_frame
from .errors import (
    APIError,
    InvalidBase64Error,
    PayloadTooLargeError,
    ProtocolNotFoundError,
    ProtocolVersionConflictError,
)
from .models import DecodeRequest, DecodeResponse, EncodeRequest, EncodeResponse, ProtocolDefinition


def _error_body(code: str, message: str, details=None) -> dict:
    body = {"error": {"code": code, "message": message}}
    if details is not None:
        body["error"]["details"] = details
    return body


def _validation_errors(errors: list[dict]) -> dict:
    details = []
    for err in errors:
        detail = {
            "code": err.get("type", "validation_error"),
            "loc": [str(x) for x in err.get("loc", [])],
            "msg": err.get("msg", ""),
        }
        ctx = err.get("ctx")
        if isinstance(ctx, dict) and isinstance(ctx.get("error"), APIError):
            inner: APIError = ctx["error"]
            detail["code"] = inner.code
            detail["msg"] = inner.message
            if inner.details is not None:
                detail["details"] = inner.details
        details.append(detail)
    return _error_body("VALIDATION_ERROR", "request validation failed", details)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    app = FastAPI(
        title="Industrial Sensor Binary Frame Re-sync API",
        version="1.0.0",
        description="Register binary frame protocols and decode/resynchronise "
        "base64 byte streams that may start in the middle of a frame.",
    )
    app.state.settings = settings
    app.state.db = Database(settings.db_path)

    # ---- structured error handlers ----------------------------------------

    @app.exception_handler(APIError)
    async def _api_error(_: Request, exc: APIError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=_error_body(exc.code, exc.message, exc.details),
        )

    @app.exception_handler(RequestValidationError)
    async def _request_validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content=_validation_errors(exc.errors()))

    @app.exception_handler(ValidationError)
    async def _pydantic_validation(_: Request, exc: ValidationError) -> JSONResponse:
        # Models validated explicitly inside a route (e.g. /encode parses JSON
        # with parse_float=Decimal) still report through the structured 422.
        return JSONResponse(status_code=422, content=_validation_errors(exc.errors()))

    @app.exception_handler(Exception)
    async def _unhandled(_: Request, exc: Exception) -> JSONResponse:  # pragma: no cover
        return JSONResponse(
            status_code=500,
            content=_error_body("INTERNAL_ERROR", str(exc)),
        )

    # ---- routes ------------------------------------------------------------

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.post("/protocols", status_code=201)
    async def register_protocol(request: Request) -> dict:
        try:
            raw_body = await request.json()
        except Exception as exc:  # JSONDecodeError etc.
            raise InvalidBase64Error(
                code="INVALID_JSON", message=f"request body must be valid JSON: {exc}"
            ) from exc
        # Explicit validation so ProtocolValidationError (structured 422) propagates.
        proto = ProtocolDefinition.model_validate(raw_body)
        db: Database = app.state.db
        definition_sha = hashlib.sha256(
            proto.model_dump_json(by_alias=True).encode()
        ).hexdigest()

        existing = db.get_protocol_row(proto.protocol_id, proto.version)
        if existing is not None and existing["definition_sha256"] != definition_sha:
            raise ProtocolVersionConflictError(
                message=(
                    f"protocol '{proto.protocol_id}' version {proto.version} already exists "
                    "with a different definition"
                ),
                details={
                    "protocol_id": proto.protocol_id,
                    "version": proto.version,
                    "existing_sha256": existing["definition_sha256"],
                    "submitted_sha256": definition_sha,
                },
            )

        db.upsert_protocol(proto, definition_sha)
        return {
            "protocol_id": proto.protocol_id,
            "version": proto.version,
            "definition_sha256": definition_sha,
            "replaced": existing is not None,
        }

    @app.get("/protocols")
    async def list_protocols() -> dict:
        return {"protocols": app.state.db.list_protocols()}

    @app.get("/protocols/{protocol_id}")
    async def get_protocol(protocol_id: str, version: int | None = None) -> dict:
        found = app.state.db.find_protocol(protocol_id, version)
        if found is None:
            raise ProtocolNotFoundError(
                message=f"protocol '{protocol_id}'"
                + (f" version {version}" if version else "")
                + " not found"
            )
        proto, resolved_version = found
        return {
            "version": resolved_version,
            "definition": json.loads(proto.model_dump_json(by_alias=True)),
        }

    @app.post("/decode", response_model=DecodeResponse)
    async def decode(req: DecodeRequest) -> dict:
        db: Database = app.state.db

        try:
            raw = base64.b64decode(req.payload_b64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise InvalidBase64Error(message=f"payload_b64 is not valid base64: {exc}") from exc

        if len(raw) > settings.max_decode_bytes:
            raise PayloadTooLargeError(
                message=(
                    f"decoded payload is {len(raw)} bytes; limit is {settings.max_decode_bytes} bytes"
                ),
                details={"size": len(raw), "limit": settings.max_decode_bytes},
            )

        found = db.find_protocol(req.protocol_id, req.version)
        if found is None:
            raise ProtocolNotFoundError(
                message=f"protocol '{req.protocol_id}'"
                + (f" version {req.version}" if req.version else "")
                + " not found"
            )
        proto, resolved_version = found

        input_digest = hashlib.sha256(raw).hexdigest()

        cached = db.get_cached(proto.protocol_id, resolved_version, input_digest)
        if cached is not None:
            cached["cached"] = True
            return cached

        result = decode_stream(raw, proto)
        response = {
            "protocol_id": proto.protocol_id,
            "version": resolved_version,
            "input_digest": input_digest,
            "frames": result["frames"],
            "discarded": result["discarded"],
            "tail": result["tail"],
            "frame_count": len(result["frames"]),
            "consumed": result["consumed"],
            "cached": False,
        }
        validated = DecodeResponse.model_validate(response)
        db.put_cached(proto.protocol_id, resolved_version, input_digest, validated)
        return response

    @app.post(
        "/encode",
        response_model=EncodeResponse,
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {
                        # Parsed manually (parse_float=Decimal); keep the docs schema explicit.
                        "schema": EncodeRequest.model_json_schema(ref_template="#/components/schemas/{model}"),
                    }
                },
            }
        },
    )
    async def encode(request: Request) -> dict:
        # Parse JSON with Decimal for all floating-point numbers so physical
        # values keep their exact decimal representation through the inversion.
        try:
            raw_body = json.loads(await request.body(), parse_float=Decimal)
        except Exception as exc:  # JSONDecodeError etc.
            raise InvalidBase64Error(
                code="INVALID_JSON", message=f"request body must be valid JSON: {exc}"
            ) from exc
        req = EncodeRequest.model_validate(raw_body)

        found = app.state.db.find_protocol(req.protocol_id, req.version)
        if found is None:
            raise ProtocolNotFoundError(
                message=f"protocol '{req.protocol_id}'"
                + (f" version {req.version}" if req.version else "")
                + " not found"
            )
        proto, resolved_version = found

        result = encode_frame(proto, req.logical_length, req.fields)
        response = {"protocol_id": proto.protocol_id, "version": resolved_version, **result}
        return EncodeResponse.model_validate(response).model_dump()

    return app


app = create_app()
