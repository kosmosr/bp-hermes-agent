"""Unit tests for _EnvelopeRingBuffer."""
import pytest
from gateway.platforms.desktop import _EnvelopeRingBuffer


def test_empty_buffer_since_zero():
    buf = _EnvelopeRingBuffer(capacity=10)
    events, gap = buf.since(0)
    assert events == []
    assert gap is False


def test_append_and_since():
    buf = _EnvelopeRingBuffer(capacity=10)
    buf.append({"kind": "message.delta", "text": "hello"})
    buf.append({"kind": "message.delta", "text": "world"})

    events, gap = buf.since(0)
    assert len(events) == 2
    assert events[0]["text"] == "hello"
    assert gap is False


def test_since_partial():
    buf = _EnvelopeRingBuffer(capacity=10)
    buf.append({"kind": "a"})  # seq=1
    buf.append({"kind": "b"})  # seq=2
    buf.append({"kind": "c"})  # seq=3

    events, gap = buf.since(1)  # everything after seq 1
    assert len(events) == 2
    assert events[0]["kind"] == "b"
    assert events[1]["kind"] == "c"
    assert gap is False


def test_overflow_causes_gap():
    buf = _EnvelopeRingBuffer(capacity=3)
    for i in range(10):
        buf.append({"kind": f"e{i}"})

    # Buffer has seq 8,9,10; asking for since=5 means we lost 6,7
    events, gap = buf.since(5)
    assert gap is True
    assert len(events) == 3  # seq 8,9,10


def test_max_seq_tracks_total():
    buf = _EnvelopeRingBuffer(capacity=3)
    for i in range(5):
        buf.append({"kind": f"e{i}"})
    assert buf.max_seq == 5


def test_since_exact_max_returns_empty():
    buf = _EnvelopeRingBuffer(capacity=10)
    buf.append({"kind": "a"})
    buf.append({"kind": "b"})
    events, gap = buf.since(2)
    assert events == []
    assert gap is False
