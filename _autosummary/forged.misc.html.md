# forged.misc

Misc utils

### Functions

| [`call_repeatedly`](#forged.misc.call_repeatedly)(func, \*args, \*\*kwargs)   | Call func(\*args, \*\*kwargs) ad infinitum                                                 |
|----------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| [`in_box`](#forged.misc.in_box)(X, box)                              | Get a boolean array indicating whether points X are within a given box                     |
| [`mk_blobs`](#forged.misc.mk_blobs)([n_samples, n_features, centers])  | wrapper for make_blobs                                                                     |
| [`random_points_not_in_box`](#forged.misc.random_points_not_in_box)([npts, ...])       | Get some random points that are contained in the universe_box but not in the forbidden_box |
| [`tag_data`](#forged.misc.tag_data)(data, data_to_tag)                 | Returns the (data, data_to_tag(data)) pair                                                 |
| [`test_on_blobs`](#forged.misc.test_on_blobs)(model_cls[, ...])             | randomly select data, fit a model, and assert a minimum accuracy.                          |

### forged.misc.call_repeatedly(func, \*args, \*\*kwargs)

Call func(\*args, \*\*kwargs) ad infinitum

* **Parameters:**
  * **func** – A callable
  * **args** – args to call func with
  * **kwargs** – kwargs to call func with
* **Returns:**
  infinite generator of func(\*args, \*\*kwargs) outputs

```pycon
>>> from itertools import islice
>>> from random import randint
>>> never_ending_int_generator = call_repeatedly(randint, 0, 9)
>>> # then... consume for ever, or just ask for a slice
>>> from itertools import islice
>>> t = list(islice(never_ending_int_generator, 7))  # example: [7, 7, 6, 1, 2, 8, 1]
>>> len(t)
7
>>> assert all(0 <= x <= 9 for x in t)
```

```pycon
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
```

### forged.misc.in_box(X, box)

Get a boolean array indicating whether points X are within a given box

* **Parameters:**
  * **X** – n_pts x n_dims array of points
  * **box** – 2 x n_dims box specs (box[0, :] is the min point and box[1, :] is the max point)
* **Returns:**
  n_pts boolean array r where r[idx] is True iff X[idx, :] is within the box

```pycon
>>> import numpy as np
>>> X = np.arange(12).reshape((4, 3))
>>> print(X)
[[ 0  1  2]
 [ 3  4  5]
 [ 6  7  8]
 [ 9 10 11]]
>>> in_box(X, [[1, 2, 3], [6, 7, 8]])
array([False,  True,  True, False])
```

```pycon
>>> in_box(X, box=[[2] * 3, [7] * 3])
array([False,  True, False, False])
```

### forged.misc.mk_blobs(n_samples=100, n_features=3, centers=2, \*\*blobs_kwargs)

wrapper for make_blobs

### forged.misc.random_points_not_in_box(npts=100, universe_box=((0, 0, 0), (10, 10, 10)), forbidden_box=((0, 1, 2), (3, 4, 5)), max_search=1000000)

Get some random points that are contained in the universe_box but not in the forbidden_box

```pycon
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
```

### forged.misc.tag_data(data, data_to_tag)

Returns the (data, data_to_tag(data)) pair

### forged.misc.test_on_blobs(model_cls, deserialize_model_cls=None, min_accuracy=1.0, verbose=False, n_samples=100, n_features=3, centers=2, \*\*blobs_kwargs)

randomly select data, fit a model, and assert a minimum accuracy.
Also make sure jdict serialization and deserialization works (if deserialize_model_cls is given)

#### NOTE
It sometimes happens that you get unlucky with with the random data, and get lower accuracies!

* **Parameters:**
  * **model_cls** – model to use to fit
  * **deserialize_model_cls**
  * **min_accuracy**
  * **verbose**
  * **n_samples**
  * **n_features**
  * **centers**
  * **\*\*blobs_kwargs**

### Returns

```pycon
>>> from sklearn.linear_model import LogisticRegressionCV
>>> test_on_blobs(LogisticRegressionCV)
>>> test_on_blobs(LogisticRegressionCV(cv=5), min_accuracy=.9, n_samples=1000, n_features=6, centers=5)
```
