"""
Misc utils
"""

from functools import partial
from sklearn.datasets import make_blobs
import numpy as np

from lined.tools import deiterize


def call_repeatedly(func, *args, **kwargs):
    """Call func(*args, **kwargs) ad infinitum

    :param func: A callable
    :param args: args to call func with
    :param kwargs: kwargs to call func with
    :return: infinite generator of func(*args, **kwargs) outputs

    >>> from itertools import islice
    >>> from random import randint
    >>> never_ending_int_generator = call_repeatedly(randint, 0, 9)
    >>> # then... consume for ever, or just ask for a slice
    >>> from itertools import islice
    >>> t = list(islice(never_ending_int_generator, 7))  # example: [7, 7, 6, 1, 2, 8, 1]
    >>> len(t)
    7
    >>> assert all(0 <= x <= 9 for x in t)

    >>> def my_context_sensitive_func(context):
    ...     return context['current_center'] + randint(-2, 2)
    >>> context = {'current_center': 0}
    >>> never_ending_int_generator = call_repeatedly(my_context_sensitive_func, context)
    >>>
    >>> t = list(islice(never_ending_int_generator, 7))  # example: [-1, -2, -1, -1, 2, -2, 0]
    >>> assert len(t) == 7 and all(-2 <= x <= 2 for x in t)
    >>> context['current_center'] = 10
    >>> t = list(islice(never_ending_int_generator, 9))  # example: [11, 10, 9, 9, 9, 8, 12, 10, 8]
    >>> assert len(t) == 9 and all(8 <= x <= 12 for x in t)


    """
    while True:
        yield func(*args, **kwargs)


def in_box(X, box):
    """Get a boolean array indicating whether points X are within a given box

    :param X: n_pts x n_dims array of points
    :param box: 2 x n_dims box specs (box[0, :] is the min point and box[1, :] is the max point)
    :return: n_pts boolean array r where r[idx] is True iff X[idx, :] is within the box

    >>> import numpy as np
    >>> X = np.arange(12).reshape((4, 3))
    >>> print(X)
    [[ 0  1  2]
     [ 3  4  5]
     [ 6  7  8]
     [ 9 10 11]]
    >>> in_box(X, [[1, 2, 3], [6, 7, 8]])
    array([False,  True,  True, False])

    >>> in_box(X, box=[[2] * 3, [7] * 3])
    array([False,  True, False, False])

    """
    MINS_ROW_IDX = 0
    MAXS_ROW_IDX = 1
    X, box = map(np.array, (X, box))
    n_rows_in_box_matrix, ndims = box.shape
    assert (
        n_rows_in_box_matrix == 2
    ), 'box must have 2 rows only: [0] the min point and [1] the max point of the box'
    assert (
        X.shape[1] == ndims
    ), f"ndims of X should be aligned with box's ({ndims}): Was {X.shape[1]}"
    return np.all((box[MINS_ROW_IDX, :] <= X) & (X <= box[MAXS_ROW_IDX, :]), axis=1)


point_in_box = deiterize(in_box)
point_in_box.__doc__ = '''
Single point version of in_box function.

>>> point_in_box([0, 1, 2], box=[[1, 2, 3], [6, 7, 8]])
False
>>> point_in_box([3, 4, 5], box=[[1, 2, 3], [6, 7, 8]])
True
'''


def random_points_not_in_box(
    npts=100,
    universe_box=((0,) * 3, (10,) * 3),
    forbidden_box=((0, 1, 2), (3, 4, 5)),
    max_search=int(1e6),
):
    """Get some random points that are contained in the universe_box but not in the forbidden_box

    >>> universe_box = ((0,) * 3, (10,) * 3)
    >>> forbidden_box = ((0, 1, 2), (3, 4, 5))
    >>> pts = random_points_not_in_box(
    ...             npts=10,
    ...             universe_box=universe_box,
    ...             forbidden_box=forbidden_box)
    >>> pts.shape
    (10, 3)
    >>> all(in_box(pts, universe_box))
    True
    >>> any(in_box(pts, forbidden_box))
    False
    """
    universe_box, forbidden_box = map(np.array, (universe_box, forbidden_box))
    _, n_dims = universe_box.shape
    universe_min, universe_max = universe_box[0, :].min(), universe_box[1, :].max()

    rand_pt = lambda: np.random.uniform(
        low=universe_min, high=universe_max, size=(1, n_dims)
    )[0]
    pt_iterator = (rand_pt() for _ in range(max_search))
    is_in_universe = partial(point_in_box, box=universe_box)
    is_forbidden = partial(point_in_box, box=forbidden_box)

    def gen():
        c = 0
        while c < npts:
            pt = next(
                filter(lambda x: is_in_universe(x) and not is_forbidden(x), pt_iterator)
            )
            yield pt
            c += 1

    return np.array(list(gen()))


def tag_data(data, data_to_tag):
    """Returns the (data, data_to_tag(data)) pair"""
    return data, data_to_tag(data)


# def _should_ask(asked, got, need, extra=0.0):
#     return int(np.ceil(asked * (need / got) * (1 + extra)))
#
#
# def tagged_data(data_gen, data_to_tag):
#     for data in data_gen():
#         yield data, data_to_tag(data)
#
# def filtered_random(npts=100,
#                     ndims=2,
#                     data_gen=lambda npts, ndims: np.random.uniform(size=(npts, ndims))):
#     forbidden = np.array(forbidden).reshape((2, -1))
#     dims_of_forbidden_box = forbidden[1, 0] - forbidden[0, 1]
#
#     gen_data = partial(data_gen, ndims=ndims)  # fix ndims; left is only npts
#
#     X = np.zeros((0, ndims))
#     previous_X_size = len(X)
#     ask = npts
#     while len(X) < npts:
#         new_X = filt(gen_data(ask))
#         X = np.vstack((X, new_X))
#         ask = _should_ask(asked=ask, got=len(new_X),
#                           need=npts - len(X), extra=0.2)
#
#     return X[:npts]


def mk_blobs(n_samples=100, n_features=3, centers=2, **blobs_kwargs):
    """
    wrapper for make_blobs
    """
    X, y = make_blobs(
        n_samples=n_samples, n_features=n_features, centers=centers, **blobs_kwargs
    )
    y = np.array(list(map(str, y)))
    return X, y


def test_on_blobs(
    model_cls,
    deserialize_model_cls=None,
    min_accuracy=1.0,
    verbose=False,
    n_samples=100,
    n_features=3,
    centers=2,
    **blobs_kwargs,
):
    """randomly select data, fit a model, and assert a minimum accuracy.
    Also make sure jdict serialization and deserialization works (if deserialize_model_cls is given)

    Note: It sometimes happens that you get unlucky with with the random data, and get lower accuracies!
    TODO: Force easy data to be certain that a model should be able to handle it.

    Args:
        model_cls: model to use to fit
        deserialize_model_cls:
        min_accuracy:
        verbose:
        n_samples:
        n_features:
        centers:
        **blobs_kwargs:

    Returns:

    >>> from sklearn.linear_model import LogisticRegressionCV
    >>> test_on_blobs(LogisticRegressionCV)
    >>> test_on_blobs(LogisticRegressionCV(cv=5), min_accuracy=.9, n_samples=1000, n_features=6, centers=5)
    """
    import os

    try:
        import dill
    except ImportError:
        dill = type('fake_dill', (object,), {'dump': lambda x, y: None})

    if verbose:
        print(
            f'{model_cls} - {deserialize_model_cls} - {dict(min_accuracy=min_accuracy, **blobs_kwargs)}'
        )

    X, y = mk_blobs(
        n_samples=n_samples, n_features=n_features, centers=centers, **blobs_kwargs
    )

    if isinstance(model_cls, type):
        m = model_cls()
    else:  # it's an already parametrized model
        m = model_cls
        model_cls = m.__class__
    m.fit(X, y)
    accuracy = np.sum(m.predict(X) == y) / len(y)
    try:
        assert accuracy >= min_accuracy, f'Accuracy: {accuracy} < {min_accuracy}'
        if deserialize_model_cls is not None:
            if deserialize_model_cls is True:  # just take the model_cls
                deserialize_model_cls = model_cls
            m_jdict = m.to_jdict()
            m_jdict_m = deserialize_model_cls.from_jdict(m_jdict)
            assert np.all(
                m.predict(X) == m_jdict_m.predict(X)
            ), "some predictions weren't the same after deserialization"

    except:
        save_filepath = os.path.abspath('test_on_blobs.p')
        print(
            f"--> To see the data, do: import dill; m, X, y = dill.load(open('{save_filepath}', 'rb'))"
        )
        dill.dump((m, X, y), open('test_on_blobs.p', 'bw'))
        raise
