# forged.strings

Functions to generate strings

### Functions

| [`args_and_kwargs_indices`](#forged.strings.args_and_kwargs_indices)(format_string)          | Get the sets of indices and names used in manual specification of format strings, or None, None if auto spec.                                                                                                                                                                                                             |
|--------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dict_of_tuple`(d, fields)                                                                       |                                                                                                                                                                                                                                                                                                                           |
| `empty_arg_and_kwargs_for_format`(format_string)                                                 |                                                                                                                                                                                                                                                                                                                           |
| [`format_params_in_str_format`](#forged.strings.format_params_in_str_format)(format_string)      | Get the "parameter" indices/names of the format_string                                                                                                                                                                                                                                                                    |
| [`is_automatic_format_params`](#forged.strings.is_automatic_format_params)(format_params)       | Says if the format_params is from an automatic specification                                                                                                                                                                                                                                                              |
| [`is_hybrid_format_params`](#forged.strings.is_hybrid_format_params)(format_params)          | Says if the format_params is from a hybrid of auto and manual.                                                                                                                                                                                                                                                            |
| [`is_hybrid_format_string`](#forged.strings.is_hybrid_format_string)(format_string)          | Says if the format_params is from a hybrid of auto and manual.                                                                                                                                                                                                                                                            |
| [`is_manual_format_params`](#forged.strings.is_manual_format_params)(format_params)          | Says if the format_params is from a manual specification                                                                                                                                                                                                                                                                  |
| [`is_manual_format_string`](#forged.strings.is_manual_format_string)(format_string)          | Says if the format_string uses a manual specification                                                                                                                                                                                                                                                                     |
| [`random_dict_gen`](#forged.strings.random_dict_gen)([fields, word_size_range, ...]) | Random dict (of strings) generator                                                                                                                                                                                                                                                                                        |
| [`random_formatted_str_gen`](#forged.strings.random_formatted_str_gen)([format_string, ...])  | Random formatted string generator                                                                                                                                                                                                                                                                                         |
| [`random_string`](#forged.strings.random_string)([length, alphabet])               | Same as random_word, but it optimized for strings (5-10% faster for words of length 7, 25-30% faster for words of size 1000)                                                                                                                                                                                              |
| [`random_tuple_gen`](#forged.strings.random_tuple_gen)([tuple_length, ...])           | Random tuple (of strings) generator                                                                                                                                                                                                                                                                                       |
| [`random_word`](#forged.strings.random_word)(length, alphabet[, concat_func])    | Make a random word by concatenating randomly drawn elements from alphabet together :type length:  :param length: Length of the word :type alphabet:  :param alphabet: Alphabet to draw from :type concat_func:  :param concat_func: The concatenation function (e.g. + for strings and lists).                            |
| [`random_word_gen`](#forged.strings.random_word_gen)([word_size_range, alphabet, n]) | Random string generator :type word_size_range:  :param word_size_range: An int, 2-tuple of ints, or list-like object that defines the choices of word sizes :type alphabet:  :param alphabet: A string or iterable defining the alphabet to draw from :type n:  :param n: The number of elements the generator will yield |
| `tuple_of_dict`(d, fields)                                                                       |                                                                                                                                                                                                                                                                                                                           |

### Exceptions

| [`KeyValidationError`](#forged.strings.KeyValidationError)   | Error to raise when a key is not valid                            |
|-----------------------------------------------------------------------|-------------------------------------------------------------------|
| [`NotValid`](#forged.strings.NotValid)             | To use to indicate when an object doesn't fit expected properties |

### *exception* forged.strings.KeyValidationError

Bases: [`NotValid`](#forged.strings.NotValid)

Error to raise when a key is not valid

### *exception* forged.strings.NotValid

Bases: [`ValueError`](https://docs.python.org/3/builtins/exceptions.html#ValueError), [`TypeError`](https://docs.python.org/3/builtins/exceptions.html#TypeError)

To use to indicate when an object doesn’t fit expected properties

### forged.strings.args_and_kwargs_indices(format_string)

Get the sets of indices and names used in manual specification of format strings, or None, None if auto spec.
:type format_string: 
:param format_string: A format string (i.e. a string with {…} to mark parameter placement and formatting

* **Returns:**
  None, None if format_string is an automatic specification
  set_of_indices_used, set_of_fields_used if it is a manual specification

```pycon
>>> format_string = '{0} (no 1) {2}, {see} this, {0} is a duplicate (appeared before) and {name} is string-named'
>>> assert args_and_kwargs_indices(format_string) == ({0, 2}, {'name', 'see'})
>>> format_string = 'This is a format string with only automatic field specification: {}, {}, {} etc.'
>>> assert args_and_kwargs_indices(format_string) == (set(), set())
```

### forged.strings.format_params_in_str_format(format_string)

Get the “parameter” indices/names of the format_string

* **Parameters:**
  **format_string** – A format string (i.e. a string with {…} to mark parameter placement and formatting
* **Returns:**
  A list of parameter indices used in the format string, in the order they appear, with repetition.
  Parameter indices could be integers, strings, or None (to denote “automatic field numbering”.

```pycon
>>> format_string = '{0} (no 1) {2}, and {0} is a duplicate, {} is unnamed and {name} is string-named'
>>> format_params_in_str_format(format_string)
[0, 2, 0, None, 'name']
```

### forged.strings.is_automatic_format_params(format_params)

Says if the format_params is from an automatic specification

#### SEE ALSO
is_manual_format_params and is_hybrid_format_params

### forged.strings.is_hybrid_format_params(format_params)

Says if the format_params is from a hybrid of auto and manual.

#### NOTE
Hybrid specifications are considered non-valid and can’t be formatted with format_string.format(…).
Yet, it can be useful for flexibility of expression (but will need to be resolved to be used).

#### SEE ALSO
is_manual_format_params and is_automatic_format_params

### forged.strings.is_hybrid_format_string(format_string)

Says if the format_params is from a hybrid of auto and manual.

#### NOTE
Hybrid specifications are considered non-valid and can’t be formatted with format_string.format(…).
Yet, it can be useful for flexibility of expression (but will need to be resolved to be used).

```pycon
>>> is_hybrid_format_string('Manual: indices: {1} {2}, named: {named} {fields}')
False
>>> is_hybrid_format_string('Auto: only un-indexed and un-named: {} {}...')
False
>>> is_hybrid_format_string('Hybrid: at least a {}, and a {0} or a {name}')
True
>>> is_manual_format_string('No formatting is both manual and automatic formatting (so hybrid is both)!')
True
```

### forged.strings.is_manual_format_params(format_params)

Says if the format_params is from a manual specification

#### SEE ALSO
is_automatic_format_params

### forged.strings.is_manual_format_string(format_string)

Says if the format_string uses a manual specification

#### SEE ALSO
is_automatic_format_string and

```pycon
>>> is_manual_format_string('Manual: indices: {1} {2}, named: {named} {fields}')
True
>>> is_manual_format_string('Auto: only un-indexed and un-named: {} {}...')
False
>>> is_manual_format_string('Hybrid: at least a {}, and a {0} or a {name}')
False
>>> is_manual_format_string('No formatting is both manual and automatic formatting!')
True
```

### forged.strings.random_dict_gen(fields=('a', 'b', 'c'), word_size_range=(1, 10), alphabet='abcdefghijklmnopqrstuvwxyz', n=100)

Random dict (of strings) generator

* **Parameters:**
  * **fields** – Field names for the random dicts
  * **word_size_range** – An int, 2-tuple of ints, or list-like object that defines the choices of word sizes
  * **alphabet** – A string or iterable defining the alphabet to draw from
  * **n** ([`int`](https://docs.python.org/3/builtins/functions.html#int)) – The number of elements the generator will yield
* **Returns:**
  Random dict (of strings) generator

### forged.strings.random_formatted_str_gen(format_string='root/{}/{}_{}.test', word_size_range=(1, 10), alphabet='abcdefghijklmnopqrstuvwxyz', n=100)

Random formatted string generator

* **Parameters:**
  * **format_string** – A format string
  * **word_size_range** – An int, 2-tuple of ints, or list-like object that defines the choices of word sizes
  * **alphabet** – A string or iterable defining the alphabet to draw from
  * **n** – The number of elements the generator will yield
* **Returns:**
  Yields random strings of the format defined by format_string

### Examples

# >>> list(random_formatted_str_gen(‘root/{}/{}_{}.test’, (2, 5), ‘abc’, n=5))
[(‘root/acba/bb_abc.test’,),

> (‘root/abcb/cbbc_ca.test’,),
> (‘root/ac/ac_cc.test’,),
> (‘root/aacc/ccbb_ab.test’,),
> (‘root/aab/abb_cbab.test’,)]
```pycon
>>> # The following will be made not random (by restricting the constraints to "no choice"
>>> # ... this is so that we get consistent outputs to assert for the doc test.
>>>
>>> # Example with automatic specification
>>> list(random_formatted_str_gen('root/{}/{}_{}.test', (3, 4), 'a', n=2))
[('root/aaa/aaa_aaa.test',), ('root/aaa/aaa_aaa.test',)]
>>>
>>> # Example with manual specification
>>> list(random_formatted_str_gen('indexed field: {0}: named field: {name}', (2, 3), 'z', n=1))
[('indexed field: zz: named field: zz',)]
```

### forged.strings.random_string(length=7, alphabet='abcdefghijklmnopqrstuvwxyz')

Same as random_word, but it optimized for strings
(5-10% faster for words of length 7, 25-30% faster for words of size 1000)

### forged.strings.random_tuple_gen(tuple_length=3, word_size_range=(1, 10), alphabet='abcdefghijklmnopqrstuvwxyz', n=100)

Random tuple (of strings) generator

* **Parameters:**
  * **tuple_length** – The length of the tuples generated
  * **word_size_range** – An int, 2-tuple of ints, or list-like object that defines the choices of word sizes
  * **alphabet** – A string or iterable defining the alphabet to draw from
  * **n** ([`int`](https://docs.python.org/3/builtins/functions.html#int)) – The number of elements the generator will yield
* **Returns:**
  Random tuple (of strings) generator

### forged.strings.random_word(length, alphabet, concat_func=<built-in function add>)

Make a random word by concatenating randomly drawn elements from alphabet together
:type length: 
:param length: Length of the word
:type alphabet: 
:param alphabet: Alphabet to draw from
:type concat_func: 
:param concat_func: The concatenation function (e.g. + for strings and lists)

#### NOTE
Repeated elements in alphabet will have more chances of being drawn.

* **Returns:**
  A word (whose type depends on what concatenating elements from alphabet produces).

Not making this a proper doctest because I don’t know how to seed the global random temporarily

```pycon
>>> t = random_word(4, 'abcde');  # e.g. 'acae'
>>> t = random_word(5, ['a', 'b', 'c']);  # e.g. 'cabba'
>>> t = random_word(4, [[1, 2, 3], [40, 50], [600], [7000]]);  # e.g. [40, 50, 7000, 7000, 1, 2, 3]
>>> t = random_word(4, [1, 2, 3, 4]);  # e.g. 13 (because adding numbers...)
>>> # ... sometimes it's what you want:
>>> t = random_word(4, [2 ** x for x in range(8)]);  # e.g. 105 (binary combination)
>>> t = random_word(4, [1, 2, 3, 4], concat_func=lambda x, y: str(x) + str(y));  # e.g. '4213'
>>> t = random_word(4, [1, 2, 3, 4], concat_func=lambda x, y: int(str(x) + str(y)));  # e.g. 3432
```

### forged.strings.random_word_gen(word_size_range=(1, 10), alphabet='abcdefghijklmnopqrstuvwxyz', n=100)

Random string generator
:type word_size_range: 
:param word_size_range: An int, 2-tuple of ints, or list-like object that defines the choices of word sizes
:type alphabet: 
:param alphabet: A string or iterable defining the alphabet to draw from
:type n: 
:param n: The number of elements the generator will yield

* **Returns:**
  Random string generator
