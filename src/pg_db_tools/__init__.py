

def iter_join(separator, iterable):
    """
    Like str.join but with iterables in general.

    :param separator:
    :param iterable:
    :return:
    """
    it = iter(iterable)

    yield next(it)

    for item in it:
        yield separator
        yield item
