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
    substreams = segment_on_condition(stream, condition)  # expect list of substreams
    if isinstance(timer, int):
        return (take(timer,substream) for substream in substreams)
    else:
        return (filter(timer, substream) for substream in substreams)


def segment_with_lookback(stream, condition, n_lookback):
    substreams = segment_on_condition(stream, condition)
    pass


if __name__ == "__main__":
    condition = lambda x,y : x == 'x'
    stream = 'abdcxcccdeefxaaassaaxsss'
    print(stream)
    timer = 3
    substreams = segment_on_condition(stream, condition)
    result = list(segment_on_condition_with_timer(stream, condition, timer))
    print(list(result))
