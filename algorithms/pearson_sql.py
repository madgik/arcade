## 5% faster than with numpy UDFs in a dataset which contains 5 rows (3 in one local and 2 in the other).
class Algorithm:

    def algorithm(self, data_table, merged_local_results, attributes, result_table, parameters):
        iternum = 0

        yield self._local(iternum, data_table, parameters, attributes, result_table)
        yield self._global(iternum, merged_local_results, parameters, attributes)

    def _local(self, iternum, data_table, parameters, attributes, result_table):
        #### todo convert schema to a list and not string
        schema = "sx FLOAT, sxx FLOAT, sxy FLOAT, sy FLOAT, syy FLOAT, n INT"
        sqlscript = f'''
        SELECT
            SUM(x) as sx, 
            SUM(x*x) as sxx, 
            SUM(x*y) as sxy, 
            SUM(y) as sy, SUM(y*y) as syy, 
            COUNT(x) as n 
        FROM (
                SELECT 
                    {attributes[0]} as x,
                    {attributes[1]} as y 
                FROM {data_table}
             )  pearson_data
        '''
        return {'run_local': {'schema': schema, 'sqlscript': sqlscript}}


    def _global(self, iternum, merged_local_results, parameters, attributes):
        #### todo convert schema to a list and not string
        schema = "result FLOAT"
        sqlscript  = f'''
        SELECT  
            CAST((n * sxy - sx * sy) AS float)/
            (SQRT(n * sxx - sx * sx) * SQRT(n * syy - sy * sy))
            AS result
        FROM (
                SELECT SUM(n) as n,
                       SUM(sx) as sx,
                       SUM(sxx) as sxx,
                       SUM(sxy) as sxy,
                       SUM(sy) as sy,
                       SUM(syy) as syy 
                FROM {merged_local_results} 
             )  pearson_sums
        '''
        return {'run_global': {'schema': schema, 'sqlscript': sqlscript}}

## select pearson_global(SUM(sx),SUM(sxx),SUM(sxy),SUM(sy),SUM(syy),SUM(n)) from merged_local_results;
