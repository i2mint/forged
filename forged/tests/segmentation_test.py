import pytest
from forged.segmentation import (
    timestamp_stream,
    segment_equal_lengths,
    segment_on_condition,
    segment_on_condition_with_timer,
)


def test_dummy():
    assert True


def test_segment_string():
    stream = "this is a   stream"
    result = list(timestamp_stream(stream, filt=" "))
    expected = [
        (0, "t"),
        (1, "h"),
        (2, "i"),
        (3, "s"),
        (5, "i"),
        (6, "s"),
        (8, "a"),
        (12, "s"),
        (13, "t"),
        (14, "r"),
        (15, "e"),
        (16, "a"),
        (17, "m"),
    ]
    assert result == expected


def test_segment_range():
    stream = [1, 2, 3, 4, 5, 6, 7, 8]
    result = list(timestamp_stream(stream, filt=lambda x: x % 2))
    expected = [(0, 1), (2, 3), (4, 5), (6, 7)]

    assert result == expected


def test_segment_equal():
    stream = [1, 2, 3, 4, 5, 6]
    length = 3
    substreams = segment_equal_lengths(stream, length)
    substreams_lists = [list(item) for item in substreams]
    assert substreams_lists == [[1, 2, 3], [4, 5, 6]]


def test_segment_on_condition():
    condition = lambda x, y: x > y
    result = list(segment_on_condition([1, 2, 3, 3, 2, 5, 2, 4, 2], condition))
    expected = [[1, 2, 3, 3], [2, 5], [2, 4], [2]]
    assert result == expected

def test_segment_on_condition_with_timer():
    condition = lambda x, y: x == 'x'
    stream = 'abdcxcccdeefxaaassaaxsss'
    timer = 3
    result = list(segment_on_condition_with_timer(stream, condition, timer))
    expected = [['a', 'b', 'd'], ['c', 'c', 'c'], ['a', 'a', 'a'], ['s', 's', 's']]
    assert result == expected