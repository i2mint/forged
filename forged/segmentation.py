from typing import Callable, Any, Tuple, TypeVar
from more_itertools import ichunked, split_when, take

Timestamp = Any
Item = TypeVar("Item")
Callable[[Item], Tuple[Timestamp, Item]]


def timestamp_stream(stream, filt, timestamper=enumerate):
    if not callable(filt):
        sentinel = filt
        filt = lambda x: x != sentinel
    return filter(lambda x: filt(x[1]), timestamper(stream))


def segment_equal_lengths(stream, length):
    substreams = ichunked(stream, length)
    return substreams


def segment_on_condition(stream, condition):
    return split_when(stream, condition)


def segment_on_condition_with_timer(stream, condition, timer):
    substreams = segment_on_condition(stream, condition)
    if isinstance(timer, int):
        return (take(stream, timer) for stream in substreams)
    else:
        return (filter(timer, stream) for stream in substreams)


def segment_with_lookback(stream, condition, n_lookback):
    substreams = segment_on_condition(stream, condition)
    pass
