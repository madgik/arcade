udf_list = []
udf_list.append('''
CREATE OR REPLACE FUNCTION EUCLIDEAN_DISTANCE(*)
RETURNS FLOAT LANGUAGE PYTHON {
   sums = 0.0
   for i in range(1,int(len(_columns)/2+1)):
       sums += numpy.power(_columns['arg'+str(i)]-_columns['arg'+str(int(i+len(_columns)/2))],2)
   return numpy.sqrt(sums)
};
''')

class Algorithm: # iteration condition in sql

    def algorithm(self, data_table, merged_local_results, attributes, result_table, centroids_n = 3):
        # init local and global schemata
        yield {"set_schema": {"local": "N INT, centx FLOAT, centy FLOAT, datax FLOAT, datay FLOAT",
                                 "global": "termination BOOL, iternum INT, centx FLOAT, centy FLOAT"}}
        for iternum in range(70):
            yield self.global_aggregation(iternum, merged_local_results, result_table, centroids_n)
            yield self.local_expectation(iternum, data_table, attributes, result_table)

    def local_expectation(self, iternum, data_table, attr, result_table):
        sqlscript= f'''
        select count(*) as N, centx, centy, sum(datax) as datax, sum(datay) as datay from (
            select row_number() over (
                                      partition by datax, datay 
                                      order by SQRT(POWER(datax-centx,2) + POWER(datay-centy,2))
                                     ) as id, datax, datay, centx, centy
            from (select {attr[0]} as datax, {attr[1]} as datay from {data_table}) as data_points, {result_table}
        ) expectations where id=1 group by centx, centy
        '''
        return {'run_local': {'sqlscript': sqlscript}}

    def global_aggregation(self, iternum, merged_local_results, result_table, centroids_n):
        sqlscript = f'''
        select exists (select cent_x, cent_y intersect select centx, centy from {result_table}) as termination,  
            {iternum}, cent_x, cent_y from (
            select sum(n) as points, sum(datax)/sum(n) as cent_x, sum(datay)/sum(n) as cent_y from {merged_local_results} 
            group by centx, centy
        	union all
        	select 0, rand()%15, rand()%15 from generate_series(0, {centroids_n})
            order by points desc limit {centroids_n}
            ) global_centroids order by termination desc;
        	'''
        return {'run_global': {'sqlscript': sqlscript}}