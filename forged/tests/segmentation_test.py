"""
Tools to label/annotate stream elements

The motivating example is the case of an incoming stream
that we need to segment, according to the detection of an event.

For example, take a stream of integers and detect the event "multiple of 5":

```
1->2->3->4->'multiple of 5'->6->7->...

```

When the stream is "live", we don't want to process it immediately, but instead
we prefer to annotate it on the fly, by adding some metadata to it.

The simplest addition of metadata information could look like:

```
3->4->('multiple of 5', 5) -> 6 -> ...

```

This module treats the more complicated case of "multilabelling": a LabelledElement x has an attribute **x.element**,
and a container of labels **x.labels** (list, set or dict).

Multilabels can be used to segments streams into overlapping segments.

```
(group0)->(group0)->(group0, group1)->(group0, group1)-> (group1)->(group1)->...

```

"""


import pytest
from forged.segmentation import (
    timestamp_stream,
    segment_equal_lengths,
    segment_on_condition,
    segment_on_condition_with_timer,
    label_stream_on_condition,
    enumerate_annotation
)
from functools import partial
from creek.labeling import (
    DictLabeledElement,
    ListLabeledElement,
    SetLabeledElement,
    LabeledElement,
    label_element,
)


def test_dummy():
    assert True


def test_label_element():
    my_label_element = partial(
        label_element, labeled_element_cls=SetLabeledElement
    )
    x = my_label_element(42, "divisible_by_seven")
    assert (x.element, x.labels) == (42, {"divisible_by_seven"})


# may be label everything
# the more general object would be dictlabelled
# write on Thoughts on Segmentation

# {'phase':0}: groupby
# {'start_marker':'start'}
def test_label_stream_on_condition():
    stream = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    cond = lambda x: x % 5 == 0
    annotator = partial(
        label_element,
        label="divisible by 5",
        labeled_element_cls=SetLabeledElement,
    )

    def view_label(item):
        if isinstance(item, LabeledElement):
            return item.labels
        else:
            return item

    result = list(
        map(view_label, label_stream_on_condition(stream, cond, annotator))
    )
    expected = [
        1,
        2,
        3,
        4,
        {"divisible by 5"},
        6,
        7,
        8,
        9,
        {"divisible by 5"},
        11,
    ]
    assert result == expected


def test_timestamp_stream():
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
    condition = lambda x, y: x == "x"
    stream = "abdcxcccdeefxaaassaaxsss"
    timer = 3
    result = list(segment_on_condition_with_timer(stream, condition, timer))
    expected = [
        ["a", "b", "d"],
        ["c", "c", "c"],
        ["a", "a", "a"],
        ["s", "s", "s"],
    ]
    assert result == expected


def test_enumerate_annotation():
    word = "abcdefg"
    vals = [i * i for i in range(7)]
    stream = [
        DictLabeledElement(val).add_label({"letter": letter})
        for val, letter in zip(vals, word)
    ]
    result = enumerate_annotation(stream)
    result = [item.labels for item in result]
    expected = [
        {"letter": "a", "timestamp": 0},
        {"letter": "b", "timestamp": 1},
        {"letter": "c", "timestamp": 2},
        {"letter": "d", "timestamp": 3},
        {"letter": "e", "timestamp": 4},
        {"letter": "f", "timestamp": 5},
        {"letter": "g", "timestamp": 6},
    ]
    assert result == expected