udf_list = []

udf_list.append('''
CREATE or replace AGGREGATE pearson_global(sx FLOAT, sxx FLOAT, sxy FLOAT, sy FLOAT, syy FLOAT, n INT)
RETURNS FLOAT
LANGUAGE PYTHON {
    import math
    n = numpy.sum(n)
    sx = numpy.sum(sx)
    sxx = numpy.sum(sxx)
    sxy = numpy.sum(sxy)
    sy = numpy.sum(sy)
    syy = numpy.sum(syy)
    d = math.sqrt(n * sxx - sx * sx) * math.sqrt(n * syy - sy * sy)
    return float((n * sxy - sx * sy) / d)
};
''')

udf_list.append('''
CREATE or replace FUNCTION pearson_local(val1 FLOAT, val2 FLOAT)
RETURNS TABLE(sx FLOAT, sxx FLOAT, sxy FLOAT, sy FLOAT, syy FLOAT, n INT)
LANGUAGE PYTHON {
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
};
''')


class Algorithm:
    def algorithm(self, data_table, merged_local_results, attributes, result_table, parameters):
        yield {"set_schema": {"local": "sx FLOAT, sxx FLOAT, sxy FLOAT, sy FLOAT, syy FLOAT, n INT",
                              "global": "result FLOAT"}}
        yield self._local(data_table, attributes)
        yield self._global(merged_local_results)

    def _local(self,data_table, attributes):
        #### todo convert schema to a list and not string
        schema = "sx FLOAT, sxx FLOAT, sxy FLOAT, sy FLOAT, syy FLOAT, n INT"
        sqlscript = "select * from pearson_local((select * from %s))" % data_table
        return {'run_local': {'schema': schema, 'sqlscript': sqlscript}}

    def _global(self, merged_local_results):
        #### todo convert schema to a list and not string
        schema = "result FLOAT"
        sqlscript  = "select pearson_global(sx, sxx, sxy, sy, syy, n) from %s" % merged_local_results
        return {'run_global': {'schema': schema, 'sqlscript': sqlscript}}


## select pearson_global(sum(sx),sum(sxx),sum(sxy),sum(sy),sum(syy),sum(n)) from merged_local_results;
