from sklearn.datasets import make_blobs
import numpy as np


def mk_blobs(n_samples=100, n_features=3, centers=2, **blobs_kwargs):
    X, y = make_blobs(n_samples=n_samples, n_features=n_features, centers=centers, **blobs_kwargs)
    y = np.array(list(map(str, y)))
    return X, y


def test_on_blobs(model_cls, deserialize_model_cls=None, min_accuracy=1.0, verbose=False,
                  n_samples=100, n_features=3, centers=2, **blobs_kwargs):
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
        print(f"{model_cls} - {deserialize_model_cls} - {dict(min_accuracy=min_accuracy, **blobs_kwargs)}")

    X, y = mk_blobs(n_samples=n_samples, n_features=n_features, centers=centers, **blobs_kwargs)

    if isinstance(model_cls, type):
        m = model_cls()
    else:  # it's an already parametrized model
        m = model_cls
        model_cls = m.__class__
    m.fit(X, y)
    accuracy = np.sum(m.predict(X) == y) / len(y)
    try:
        assert accuracy >= min_accuracy, f"Accuracy: {accuracy} < {min_accuracy}"
        if deserialize_model_cls is not None:
            if deserialize_model_cls is True:  # just take the model_cls
                deserialize_model_cls = model_cls
            m_jdict = m.to_jdict()
            m_jdict_m = deserialize_model_cls.from_jdict(m_jdict)
            assert np.all(m.predict(X) == m_jdict_m.predict(X)), \
                "some predictions weren't the same after deserialization"

    except:
        save_filepath = os.path.abspath('test_on_blobs.p')
        print(f"--> To see the data, do: import dill; m, X, y = dill.load(open('{save_filepath}', 'rb'))")
        dill.dump((m, X, y), open('test_on_blobs.p', 'bw'))
        raise
