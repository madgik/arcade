import math
import numpy


def local(val1, val2):
    result = {}
    X = val1
    Y = val2
    result["sx"] = X.sum(axis=0)
    result["sxx"] = (X ** 2).sum(axis=0)
    result["sxy"] = (X * Y).sum(axis=0)
    result["sy"] = Y.sum(axis=0)
    result["syy"] = (Y ** 2).sum(axis=0)
    result["n"] = X.size
    return result


def merge(sx, sxx, sxy, sy, syy, n):
    n = numpy.sum(n)
    sx = numpy.sum(sx)
    sxx = numpy.sum(sxx)
    sxy = numpy.sum(sxy)
    sy = numpy.sum(sy)
    syy = numpy.sum(syy)
    d = math.sqrt(n * sxx - sx * sx) * math.sqrt(n * syy - sy * sy)
    return float((n * sxy - sx * sy) / d)
