udf_list = []
udf_list.append('''
CREATE OR REPLACE FUNCTION expectation(d_x float, d_y float, c_x float, c_y float)
RETURNS FLOAT LANGUAGE PYTHON {
return numpy.sqrt(numpy.power(d_x-c_x,2) + numpy.power(d_y-c_y,2))
};
''')

udf_list.append('''
CREATE OR REPLACE FUNCTION expectation_sql(d_x float, d_y float, c_x float, c_y float)
RETURNS FLOAT
BEGIN
 RETURN SQRT(POWER(d_x-c_x,2) + POWER(d_y-c_y,2));
END;
''')

class Algorithm: # iteration condition in python

    def algorithm(self, data_table, merged_local_results, attributes, result_table, num_of_centroids = 3):
        # init schemata yield
        yield {"set_schema":{"local" : "N INT, centx FLOAT, centy FLOAT, datax FLOAT, datay FLOAT", "global" : "centx FLOAT, centy FLOAT"}}

        centroids = []
        for iternum in range(1000):
            new_centroids = yield self.global_aggregation(merged_local_results, num_of_centroids)
            if new_centroids != centroids: centroids = new_centroids
            else: break
            yield self.local_expectation(data_table, attributes, result_table)

    def local_expectation(self, data_table, attr, result_table):
        sqlscript = f'''
            select count(*) as N, centx, centy, sum(datax) as datax, sum(datay) as datay from
                (
                select row_number() over (
                                          partition by datax, datay 
                                          order by expectation(datax, datay ,centx, centy)
                                         ) as id, datax, datay, centx, centy
                from (select {attr[0]} as datax, {attr[1]} as datay from {data_table}) as data_points, 
                     {result_table} as centroids
                ) expectations where id=1 group by centx, centy
        '''
        return {'run_local': {'sqlscript': sqlscript}}

    def global_aggregation(self, merged_local_results, centroids_n):
        sqlscript = f'''
        select cent_x, cent_y from (
            select sum(n) as points, sum(datax)/sum(n) as cent_x, sum(datay)/sum(n) as cent_y from {merged_local_results} 
            group by centx, centy
        	union all
        	select 0, rand()%15, rand()%15 from generate_series(0, {centroids_n})
            order by points desc limit {centroids_n}
            ) global_centroids
        	'''
        return {'run_global': {'sqlscript': sqlscript}}