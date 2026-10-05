from __future__ import annotations

import base64
import binascii
import hashlib
import json
import uuid
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
    SessionNotFoundError,
    SessionOffsetConflictError,
)
from .models import (
    DecodeRequest,
    DecodeResponse,
    EncodeRequest,
    EncodeResponse,
    ProtocolDefinition,
    SessionChunkRequest,
    SessionChunkResponse,
    SessionCreateRequest,
    SessionStateResponse,
)


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


def _shift_decode_result(result: dict, base: int) -> dict:
    """Re-base buffer-relative decoder offsets to session-absolute positions."""
    frames = [dict(f, start=f["start"] + base, end=f["end"] + base) for f in result["frames"]]
    discarded = []
    for interval in result["discarded"]:
        attempts = []
        for attempt in interval.get("failed_attempts", []):
            shifted = dict(attempt)
            if "start" in shifted:
                shifted["start"] += base
            detail = shifted.get("detail")
            if isinstance(detail, dict) and "raw_offset" in detail:
                shifted["detail"] = dict(detail, raw_offset=detail["raw_offset"] + base)
            attempts.append(shifted)
        discarded.append(
            dict(
                interval,
                start=interval["start"] + base,
                end=interval["end"] + base,
                failed_attempts=attempts,
            )
        )
    tail = dict(
        result["tail"],
        start=result["tail"]["start"] + base,
        end=result["tail"]["end"] + base,
    )
    return {"frames": frames, "discarded": discarded, "tail": tail}


def _session_state_body(row) -> dict:
    tail_b64 = row["tail"] or ""
    tail_len = len(base64.b64decode(tail_b64)) if tail_b64 else 0
    next_offset = row["next_offset"]
    return {
        "session_id": row["session_id"],
        "protocol_id": row["protocol_id"],
        "version": row["version"],
        "next_offset": next_offset,
        "frames_total": row["frames_total"],
        "tail": {
            "start": next_offset - tail_len,
            "end": next_offset,
            "payload_b64": tail_b64,
        },
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


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

    # ---- acquisition sessions: chunked byte streams ------------------------

    @app.post("/sessions", status_code=201, response_model=SessionStateResponse)
    async def create_session(req: SessionCreateRequest) -> dict:
        db: Database = app.state.db
        found = db.find_protocol(req.protocol_id, req.version)
        if found is None:
            raise ProtocolNotFoundError(
                message=f"protocol '{req.protocol_id}' version {req.version} not found"
            )
        proto, resolved_version = found
        session_id = uuid.uuid4().hex
        db.create_session(session_id, proto.protocol_id, resolved_version)
        return _session_state_body(db.get_session(session_id))

    @app.get("/sessions/{session_id}", response_model=SessionStateResponse)
    async def session_progress(session_id: str) -> dict:
        row = app.state.db.get_session(session_id)
        if row is None:
            raise SessionNotFoundError(message=f"session '{session_id}' not found")
        return _session_state_body(row)

    @app.post("/sessions/{session_id}/chunks", response_model=SessionChunkResponse)
    async def append_chunk(session_id: str, req: SessionChunkRequest) -> dict:
        db: Database = app.state.db
        row = db.get_session(session_id)
        if row is None:
            raise SessionNotFoundError(message=f"session '{session_id}' not found")

        try:
            chunk = base64.b64decode(req.payload_b64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise InvalidBase64Error(message=f"payload_b64 is not valid base64: {exc}") from exc
        chunk_sha = hashlib.sha256(chunk).hexdigest()
        next_offset = row["next_offset"]

        # Idempotent replay: the most recent chunk resent with the same offset
        # and content returns the stored response without re-producing output.
        if row["last_chunk_offset"] is not None and req.offset == row["last_chunk_offset"]:
            if chunk_sha == row["last_chunk_sha256"]:
                return json.loads(row["last_response"])
            raise SessionOffsetConflictError(
                message=(
                    f"offset {req.offset} was already received for session "
                    f"'{session_id}' with different content"
                ),
                details={
                    "session_id": session_id,
                    "offset": req.offset,
                    "next_offset": next_offset,
                },
            )

        if req.offset != next_offset:
            raise SessionOffsetConflictError(
                message=(
                    f"chunk offset {req.offset} does not match the receive offset "
                    f"{next_offset} of session '{session_id}'"
                ),
                details={
                    "session_id": session_id,
                    "offset": req.offset,
                    "expected_offset": next_offset,
                },
            )

        # Prepend the pending raw tail from the previous chunk, then run the
        # same resynchronising decoder on the concatenated buffer.
        tail_bytes = base64.b64decode(row["tail"]) if row["tail"] else b""
        combined = tail_bytes + chunk
        if len(combined) > settings.max_decode_bytes:
            raise PayloadTooLargeError(
                message=(
                    f"pending tail + chunk is {len(combined)} bytes; limit is "
                    f"{settings.max_decode_bytes} bytes"
                ),
                details={"size": len(combined), "limit": settings.max_decode_bytes},
            )

        found = db.find_protocol(row["protocol_id"], row["version"])
        if found is None:  # pragma: no cover - registered versions are immutable
            raise ProtocolNotFoundError(
                message=f"protocol '{row['protocol_id']}' version {row['version']} not found"
            )
        proto, resolved_version = found

        # Decode errors (structured 422) propagate without advancing the offset.
        result = decode_stream(combined, proto)

        base = next_offset - len(tail_bytes)
        shifted = _shift_decode_result(result, base)
        new_next_offset = next_offset + len(chunk)
        response = {
            "session_id": session_id,
            "protocol_id": proto.protocol_id,
            "version": resolved_version,
            "frames": shifted["frames"],
            "discarded": shifted["discarded"],
            "tail": shifted["tail"],
            "frame_count": len(shifted["frames"]),
            "chunk_bytes": len(chunk),
            "next_offset": new_next_offset,
        }
        stored = SessionChunkResponse.model_validate(response).model_dump_json()
        applied = db.apply_session_chunk(
            session_id,
            expected_offset=next_offset,
            new_offset=new_next_offset,
            tail_b64=shifted["tail"]["payload_b64"],
            frames_added=len(shifted["frames"]),
            chunk_offset=req.offset,
            chunk_sha256=chunk_sha,
            response_json=stored,
        )
        if not applied:
            # A concurrent append won the race; only an identical retry replays.
            current = db.get_session(session_id)
            if (
                current is not None
                and current["last_chunk_offset"] == req.offset
                and current["last_chunk_sha256"] == chunk_sha
            ):
                return json.loads(current["last_response"])
            raise SessionOffsetConflictError(
                message=(
                    f"session '{session_id}' advanced while this chunk was being "
                    "processed; the chunk was not applied"
                ),
                details={"session_id": session_id, "offset": req.offset},
            )
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
