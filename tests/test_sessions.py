"""Streaming acquisition sessions: chunked append, idempotent replay, persistence."""
import threading

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from tests.conftest import b64, build_logical, standard_protocol

SYNC = b"\xaa\x55"


def make_client(db_path, max_decode_bytes=1 << 20):
    app = create_app(Settings(db_path=db_path, max_decode_bytes=max_decode_bytes))
    return TestClient(app)


def register(client, **overrides):
    r = client.post("/protocols", json=standard_protocol(**overrides))
    assert r.status_code == 201, r.text


def create_session(client, version=1):
    r = client.post("/sessions", json={"protocol_id": "sensor-line", "version": version})
    assert r.status_code == 201, r.text
    return r.json()


def append(client, sid, offset, chunk):
    return client.post(
        f"/sessions/{sid}/chunks", json={"offset": offset, "payload_b64": b64(chunk)}
    )


def test_create_session_pins_exact_version(client):
    register(client)

    # version is required (the session pins an exact registered version)
    r = client.post("/sessions", json={"protocol_id": "sensor-line"})
    assert r.status_code == 422

    r = client.post("/sessions", json={"protocol_id": "nope", "version": 1})
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "PROTOCOL_NOT_FOUND"

    r = client.post("/sessions", json={"protocol_id": "sensor-line", "version": 9})
    assert r.status_code == 404

    state = create_session(client)
    assert state["protocol_id"] == "sensor-line"
    assert state["version"] == 1
    assert state["next_offset"] == 0
    assert state["frames_total"] == 0
    assert state["tail"] == {"start": 0, "end": 0, "payload_b64": ""}
    assert state["session_id"]


def test_session_uses_pinned_version_after_newer_registration(client):
    register(client)
    state = create_session(client, version=1)
    sid = state["session_id"]

    # Register a diverging v2; the session must keep decoding with v1.
    register(
        client,
        version=2,
        fields=[{"name": "renamed", "type": "u8", "offset": 0}],
        sequence_field="renamed",
    )

    r = append(client, sid, 0, SYNC + build_logical(7, 3, 100))
    assert r.status_code == 200, r.text
    names = {f["name"] for f in r.json()["frames"][0]["fields"]}
    assert {"seq", "addr", "temp"} <= names
    assert "renamed" not in names
    assert client.get(f"/sessions/{sid}").json()["version"] == 1


def test_chunked_stream_matches_one_shot(client):
    register(client)
    sid = create_session(client)["session_id"]

    f1 = build_logical(seq=1, addr=3, temp_s16=2345, status=0b1100_1111)
    f2 = build_logical(seq=2, addr=3, temp_s16=-150, status=0)
    stream = (
        b"\x12\x34"          # noise
        + SYNC + f1
        + b"\x99"            # noise between frames
        + SYNC + f2
        + SYNC + f1[:4]      # incomplete trailing frame
    )
    assert len(stream) == 31

    # Awkward chunk boundaries: mid-frame, mid-noise, mid-sync-payload.
    cuts = [stream[0:5], stream[5:13], stream[13:20], stream[20:31]]
    responses = []
    offset = 0
    for chunk in cuts:
        r = append(client, sid, offset, chunk)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["next_offset"] == offset + len(chunk)
        offset = body["next_offset"]
        responses.append(body)

    r1, r2, r3, r4 = responses
    # chunk 1: noise discarded, partial frame kept as pending tail
    assert r1["frames"] == []
    assert [(d["start"], d["end"], d["reason"]) for d in r1["discarded"]] == [(0, 2, "noise")]
    assert (r1["tail"]["start"], r1["tail"]["end"]) == (2, 5)
    assert r1["tail"]["payload_b64"] == b64(stream[2:5])
    # chunk 2: tail + new bytes complete frame 1 at session-absolute offsets
    assert [(f["start"], f["end"]) for f in r2["frames"]] == [(2, 13)]
    assert r2["frames"][0]["sequence"] == 1
    assert r2["tail"] == {"start": 13, "end": 13, "payload_b64": ""}
    # chunk 3: inter-frame noise discarded, next frame pending
    assert r3["frames"] == []
    assert [(d["start"], d["end"]) for d in r3["discarded"]] == [(13, 14)]
    assert (r3["tail"]["start"], r3["tail"]["end"]) == (14, 20)
    # chunk 4: frame 2 completes, trailing half-frame stays pending
    assert [(f["start"], f["end"]) for f in r4["frames"]] == [(14, 25)]
    assert r4["frames"][0]["sequence"] == 2
    assert (r4["tail"]["start"], r4["tail"]["end"]) == (25, 31)
    assert r4["tail"]["payload_b64"] == b64(SYNC + f1[:4])

    # The union of per-chunk results equals the one-shot /decode of the stream.
    one_shot = client.post(
        "/decode", json={"protocol_id": "sensor-line", "payload_b64": b64(stream)}
    ).json()
    frames = [f for r in responses for f in r["frames"]]
    discarded = [d for r in responses for d in r["discarded"]]
    assert frames == one_shot["frames"]
    assert discarded == one_shot["discarded"]
    assert responses[-1]["tail"] == one_shot["tail"]

    progress = client.get(f"/sessions/{sid}").json()
    assert progress["next_offset"] == 31
    assert progress["frames_total"] == 2
    assert progress["tail"]["payload_b64"] == b64(SYNC + f1[:4])


def test_frame_split_across_chunks(client):
    register(client)
    sid = create_session(client)["session_id"]
    wire = SYNC + build_logical(3, 1, 100)

    r1 = append(client, sid, 0, wire[:5])
    assert r1.status_code == 200, r1.text
    assert r1.json()["frames"] == []
    assert r1.json()["tail"]["payload_b64"] == b64(wire[:5])

    r2 = append(client, sid, 5, wire[5:])
    assert r2.status_code == 200, r2.text
    frame = r2.json()["frames"][0]
    assert (frame["start"], frame["end"]) == (0, len(wire))
    assert frame["sequence"] == 3
    assert r2.json()["next_offset"] == len(wire)


def test_replay_last_chunk_returns_original_response(client):
    register(client)
    sid = create_session(client)["session_id"]
    chunk1 = SYNC + build_logical(1, 1, 0)[:4]
    chunk2 = build_logical(1, 1, 0)[4:]

    r1 = append(client, sid, 0, chunk1)
    assert r1.status_code == 200, r1.text
    # Same offset + same content: original response, no duplicate production.
    replay = append(client, sid, 0, chunk1)
    assert replay.status_code == 200
    assert replay.json() == r1.json()
    progress = client.get(f"/sessions/{sid}").json()
    assert progress["next_offset"] == len(chunk1)
    assert progress["frames_total"] == 0

    r2 = append(client, sid, len(chunk1), chunk2)
    assert r2.status_code == 200, r2.text
    assert r2.json()["frame_count"] == 1
    # The replayed chunk is only the most recent one: chunk1 is now too old.
    stale = append(client, sid, 0, chunk1)
    assert stale.status_code == 409
    # Replaying chunk2 still works and does not double-count the frame.
    replay2 = append(client, sid, len(chunk1), chunk2)
    assert replay2.json() == r2.json()
    assert client.get(f"/sessions/{sid}").json()["frames_total"] == 1


def test_same_offset_different_content_rejected(client):
    register(client)
    sid = create_session(client)["session_id"]
    chunk = SYNC + build_logical(1, 1, 0)
    r1 = append(client, sid, 0, chunk)
    assert r1.status_code == 200, r1.text

    other = SYNC + build_logical(9, 9, 9)
    r = append(client, sid, 0, other)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "SESSION_OFFSET_CONFLICT"

    # State untouched: the session still continues at the old offset.
    progress = client.get(f"/sessions/{sid}").json()
    assert progress["next_offset"] == len(chunk)
    nxt = append(client, sid, len(chunk), SYNC + build_logical(2, 1, 0))
    assert nxt.status_code == 200, nxt.text
    assert nxt.json()["frames"][0]["sequence"] == 2


def test_out_of_order_offsets_rejected(client):
    register(client)
    sid = create_session(client)["session_id"]
    chunk = SYNC + build_logical(1, 1, 0)

    # Gap: offset ahead of the receive offset.
    r = append(client, sid, 5, chunk)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "SESSION_OFFSET_CONFLICT"
    assert r.json()["error"]["details"]["expected_offset"] == 0

    assert append(client, sid, 0, chunk).status_code == 200

    # Rewound: offset behind the receive offset (and not the last chunk).
    r = append(client, sid, 0, SYNC + build_logical(2, 2, 2))
    assert r.status_code == 409

    progress = client.get(f"/sessions/{sid}").json()
    assert progress["next_offset"] == len(chunk)
    assert progress["frames_total"] == 1


def test_empty_and_invalid_chunks_rejected(client):
    register(client)
    sid = create_session(client)["session_id"]

    r = client.post(f"/sessions/{sid}/chunks", json={"offset": 0, "payload_b64": ""})
    assert r.status_code == 422

    r = client.post(f"/sessions/{sid}/chunks", json={"offset": 0, "payload_b64": "###"})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "INVALID_BASE64"

    r = client.post(f"/sessions/{sid}/chunks", json={"offset": -1, "payload_b64": b64(b"x")})
    assert r.status_code == 422

    assert client.get(f"/sessions/{sid}").json()["next_offset"] == 0


def test_unknown_session_404(client):
    register(client)
    assert client.get("/sessions/nope").status_code == 404
    r = client.get("/sessions/nope")
    assert r.json()["error"]["code"] == "SESSION_NOT_FOUND"
    r = append(client, "nope", 0, b"x")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "SESSION_NOT_FOUND"


def test_combined_over_limit_is_413_and_does_not_advance(db_path):
    client = make_client(db_path, max_decode_bytes=16)
    register(client)
    sid = create_session(client)["session_id"]
    wire = SYNC + build_logical(1, 1, 0)  # 11 bytes

    r1 = append(client, sid, 0, wire[:5])
    assert r1.status_code == 200, r1.text
    assert r1.json()["next_offset"] == 5

    # tail (5) + chunk (12) = 17 > 16: rejected, offset not advanced.
    r = append(client, sid, 5, b"\x00" * 12)
    assert r.status_code == 413
    assert r.json()["error"]["code"] == "PAYLOAD_TOO_LARGE"
    progress = client.get(f"/sessions/{sid}").json()
    assert progress["next_offset"] == 5
    assert progress["tail"]["payload_b64"] == b64(wire[:5])

    # The correctly sized continuation still succeeds afterwards.
    r2 = append(client, sid, 5, wire[5:])
    assert r2.status_code == 200, r2.text
    assert r2.json()["frames"][0]["sequence"] == 1


def test_decode_error_does_not_advance_offset(client):
    register(client)
    sid = create_session(client)["session_id"]

    # Ambiguous stream: two valid frames end at the same offset.
    ambiguous = bytes.fromhex("67aa55710aaa550c7845ca08aab4be")
    r = append(client, sid, 0, ambiguous)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "AMBIGUOUS_FRAME"
    progress = client.get(f"/sessions/{sid}").json()
    assert progress["next_offset"] == 0
    assert progress["tail"] == {"start": 0, "end": 0, "payload_b64": ""}

    # A valid chunk at the same offset is still accepted.
    ok = append(client, sid, 0, SYNC + build_logical(1, 1, 0))
    assert ok.status_code == 200, ok.text
    assert ok.json()["frame_count"] == 1


def test_session_survives_restart(db_path):
    client1 = make_client(db_path)
    register(client1)
    sid = create_session(client1)["session_id"]
    wire = SYNC + build_logical(4, 2, 600)

    r1 = append(client1, sid, 0, wire[:5])
    assert r1.status_code == 200, r1.text

    # Simulate a service restart: a fresh app instance on the same database.
    client2 = make_client(db_path)
    progress = client2.get(f"/sessions/{sid}").json()
    assert progress["next_offset"] == 5
    assert progress["tail"]["payload_b64"] == b64(wire[:5])

    # The resent last chunk returns the original response after the restart.
    replay = append(client2, sid, 0, wire[:5])
    assert replay.status_code == 200
    assert replay.json() == r1.json()

    # And the stream continues where it left off.
    r2 = append(client2, sid, 5, wire[5:])
    assert r2.status_code == 200, r2.text
    assert r2.json()["frames"][0]["sequence"] == 4
    assert r2.json()["next_offset"] == len(wire)


def test_concurrent_same_offset_only_one_wins(db_path):
    app = create_app(Settings(db_path=db_path, max_decode_bytes=1 << 20))
    with TestClient(app) as client:
        register(client)
        sid = create_session(client)["session_id"]

    barrier = threading.Barrier(2)
    outcomes = []

    def submit(seq):
        with TestClient(app) as c:
            barrier.wait()
            r = append(c, sid, 0, SYNC + build_logical(seq, 1, 0))
            outcomes.append((r.status_code, r.json()))

    threads = [threading.Thread(target=submit, args=(s,)) for s in (1, 2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    codes = sorted(code for code, _ in outcomes)
    assert codes == [200, 409]
    conflict = next(body for code, body in outcomes if code == 409)
    assert conflict["error"]["code"] == "SESSION_OFFSET_CONFLICT"

    with TestClient(app) as client:
        progress = client.get(f"/sessions/{sid}").json()
    assert progress["next_offset"] == 2 + 9
    assert progress["frames_total"] == 1


def test_concurrent_identical_retry_both_return_same_response(db_path):
    app = create_app(Settings(db_path=db_path, max_decode_bytes=1 << 20))
    with TestClient(app) as client:
        register(client)
        sid = create_session(client)["session_id"]

    chunk = SYNC + build_logical(1, 1, 0)
    barrier = threading.Barrier(2)
    outcomes = []

    def submit():
        with TestClient(app) as c:
            barrier.wait()
            r = append(c, sid, 0, chunk)
            outcomes.append((r.status_code, r.json()))

    threads = [threading.Thread(target=submit) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert [code for code, _ in outcomes] == [200, 200]
    assert outcomes[0][1] == outcomes[1][1]
    with TestClient(app) as client:
        progress = client.get(f"/sessions/{sid}").json()
    assert progress["next_offset"] == len(chunk)
    assert progress["frames_total"] == 1
