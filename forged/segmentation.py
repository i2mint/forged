from typing import Callable, Any, Tuple, TypeVar
from more_itertools import ichunked, split_when, take, peekable
from functools import partial
from lined.tools import BufferStats, Segmenter, return_buffer_on_stats_condition
from lined import Pipe
from creek.labeling import DictLabeledElement, ListLabeledElement, SetLabeledElement, label_element



Timestamp = Any
Item = TypeVar("Item")
Callable[[Item], Tuple[Timestamp, Item]]



def label_stream_on_condition(stream, cond, annotator):
    for elem in stream:
        if cond(elem):
            elem = annotator(elem)
        yield elem


def timestamp_stream(stream, filt, timestamper=enumerate):
    if not callable(filt):
        sentinel = filt
        filt = lambda x: x != sentinel
    return filter(lambda x: filt(x[1]), timestamper(stream))

def enumerate_annotation(stream, timestamp_name='timestamp'):
    stream = peekable(stream)
    first_item = stream.peek()
    if not isinstance(first_item, DictLabeledElement):
        raise NotImplemented('Stream must be of DictLabeledElements')
    for i, item in enumerate(stream):
        item = item.add_label({timestamp_name:i})
        yield item

def timestamp_annotator(stream, timestamper = enumerate_annotation):
    return timestamper(stream)


def segment_equal_lengths(stream, length):
    substreams = ichunked(stream, length)
    return substreams


def segment_on_condition(stream, condition):
    return split_when(stream, condition)

def filter_labelled_list_on_label(stream, label):
    pass

def filter_labelled_dict_on_label(stream, label, label_value):
    pass




def segment_on_condition_with_timer(stream, condition, timer):
    substreams = segment_on_condition(stream, condition)  # expect list of substreams
    if isinstance(timer, int):
        return (take(timer, substream) for substream in substreams)
    else:
        return (filter(timer, substream) for substream in substreams)


def segment_with_lookback(stream, condition, n_lookback):
    substreams = segment_on_condition(stream, condition)
    pass


if __name__ == "__main__":
    # add docs, notes
    # place it somewhere else: may be creek instead
    #gen = "abcxdefghxbbjgjxdddd"
    """
    gen = [1,2,3,4,5,6,3,8,9]
    bs = BufferStats(maxlen=3, func=list)
    return_if_stats_contains_x = partial(
        return_buffer_on_stats_condition, cond=lambda x: x!=3, else_val=0
    )

    def append_annot(annot):
        def res(item):
            return "".join(["".join(item), annot])

        return res

    seg = Segmenter(
        buffer=bs,
        stats_buffer_callback=lambda x:x,
    )
    print(list(map(bs, gen)))
    #print(list(map(seg, gen)))
    
    stream = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    cond = lambda x: x % 5 == 0
    annotator = partial(label_element, label='divisible by 5', labeled_element_cls=SetLabeledElement)
    result = label_stream_on_condition(stream, cond, annotator)
    print(list(result))
    """
    word = "abcdefg"
    vals = [i*i for i in range(7)]
    stream = [DictLabeledElement(val).add_label({'letter': letter}) for val,letter in zip(vals,word)]
    #result = list(enumerate_annotation(stream))
    result = timestamp_annotator(stream)
    result = [item.labels for item in result]

    print(result)
