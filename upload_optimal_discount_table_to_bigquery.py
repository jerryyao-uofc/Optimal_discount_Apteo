from utils.src.main.code.io.bigquery_client import BigqueryClient
bigquery_client = BigqueryClient(environment = "production")
import pandas as pd

class UploadOptimalDiscountTableToBigQuery: 
    def __init__(self, table, CUSTOMER_QUERY, UPLOAD_PATH):
        self.table = table
        self.CUSTOMER_QUERY = CUSTOMER_QUERY
        self.UPLOAD_PATH = UPLOAD_PATH 
        
    def get_customer_id_and_email(self):
        res_customer = bigquery_client.execute_query_and_return_results(self.CUSTOMER_QUERY, return_as_records=True)
        df_customer = pd.DataFrame.from_dict(res_customer)
        return df_customer 
    
    def find_the_optimal_discount_for_each(self): 
        df_customer = self.get_customer_id_and_email()
        df_customer['date'] = 0
        df_customer['purchase_liklihood'] = 0.0
        df_customer['recommanded_discount']= 0.0
        
        for index, row in df_customer.iterrows():
            cur_email = row['email_address']
            all_possible= (self.table[self.table['email']== cur_email]).sort_values(by=['sigmoid'], ascending= False)
            if (len(all_possible)>0):
                df_customer['date'].iloc[index]= all_possible['date'].iloc[0]
                df_customer['purchase_liklihood'].iloc[index] = all_possible['sigmoid'].iloc[0]
                df_customer['recommanded_discount'].iloc[index] = all_possible['discount'].iloc[0]
        return df_customer  
    
    # use before sending dataframe into bigquery clienet for upload 
    @staticmethod
    def convert_dict_to_res(dataframe1): 
        res_list = []
        for index, row in dataframe1.iterrows():
            dict_temp = {}
            for (columnName, columnData) in row.iteritems():
                dict_temp[columnName] = columnData
            res_list.append(dict_temp)
        return res_list
    
    def process_and_upload_to_big_query(self): 
        
        df_customer = self.find_the_optimal_discount_for_each()
        final_optimal_table = self.convert_dict_to_res(df_customer)
        try:
            status = bigquery_client.create_or_update_table(self.UPLOAD_PATH, final_optimal_table, if_exists = 'replace')
            print("upload to big query sucessfull")
        except: 
            print("upload to big query failed")
        