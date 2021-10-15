import pandas as pd
from datetime import date
from datetime import datetime
from generate_optimal_discount_initial_training_dataset import GenerateOptimalDiscountInitialTrainingDataset


class GeneratePredictionDatasetForExistingCustomer:
    
    
    def __init__(self, existing_customer_list, previous_discount_dataset, previous_transaction_record, discount_ceiling, discount_step, days_countas_conversion):
        self.existing_customer_list = existing_customer_list
        self.previous_discount_dataset = previous_discount_dataset
        self.previous_transaction_record = previous_transaction_record
        self.discount_ceiling = discount_ceiling
        self.discount_step = discount_step 
        self.days_countas_conversion = days_countas_conversion
        
        self.dict_max_prev_artificial ={}
        self.dict_most_recent_artificial = {}
        self.dict_count_discount_rec_artificial = {}
        self.dict_count_average_dis_artificial = {}
        
    def populate_dict(self):
        # """
        # populate the dictionary above for later use: 

        # :Param list_given (list): a list of customers 
        # :Param previous_discount_his: the dataframe of previous discount history 

        # Returns:
        # 	Nothing
        # """
        # list_given = self.customers
        # previous_discount_his = self.previous_discount_dataset
        for cus_email in self.existing_customer_list:
            # print(cus_email)
            prev_existence =  self.previous_discount_dataset[ self.previous_discount_dataset["email"] == cus_email]

            if len(prev_existence) > 0: 
                sorted_prev = prev_existence.sort_values(by = ['count_discount_recieved'], ascending = False)
                max_int = (sorted_prev['max_previous_discount'].iloc[0])
                self.dict_max_prev_artificial[cus_email] = max_int
                self.dict_most_recent_artificial[cus_email] = (sorted_prev['discount'].iloc[0])
                self.dict_count_discount_rec_artificial[cus_email]= sorted_prev['count_discount_recieved'].iloc[0]
                self.dict_count_average_dis_artificial[cus_email] = sorted_prev['average_previous_discount_recieved'].iloc[0]
            else: 
                self.dict_max_prev_artificial[cus_email] = 0
                self.dict_most_recent_artificial[cus_email] = 0
                self.dict_count_discount_rec_artificial[cus_email]= 0
                self.dict_count_average_dis_artificial[cus_email] = 0
        return True
    
        # idea for future expansion: can make customer date customizable 
        # right now the discount rate is treated as a sudo email sent out today 
        # but we can make "today" as the future date to automate the scheduling of the optimal discount
    def populate_df(self): 
        today = date.today()
        d3 = datetime.strptime(today.strftime("%Y-%m-%d %H:%M:%S"), '%Y-%m-%d %H:%M:%S')
        dataframe1 = pd.DataFrame(columns = ['email', 'campagin_ID', 'discount', 'date', 
                                              'customer_ID_klaviyo', 'max_previous_discount',
                                     'count_discount_recieved', 'average_previous_discount_recieved'])
        
        for i in range(0, len(self.existing_customer_list)): 
            # print(i)
            cus_email = self.existing_customer_list[i]
            for m in range(10, self.discount_ceiling, self.discount_step):
                
                #max_previous_discount_generated
                mp = self.dict_max_prev_artificial[cus_email]
                # count_discount_recieved_generated
                mr = self.dict_count_discount_rec_artificial[cus_email]

                #average discount previouse recieved 
                adp = self.dict_count_average_dis_artificial[cus_email] 

                # most recent discount 
                mrd = self.dict_most_recent_artificial[cus_email]

                dataframe1.loc[len(dataframe1)] = [cus_email, 'AI_predict', m, d3, '', max(mp, m), mr+1, (mrd+ adp*mr)/(mr+1) ]
                
        return dataframe1
    
    def generate_prediction_data_set(self): 
        # all steps together generate the prediction datasets
        self.populate_dict()
        res = self.populate_df()
        
        generate_prediction_data_set_class_use = GenerateOptimalDiscountInitialTrainingDataset(res,
                                                                                              self.previous_transaction_record,
                                                                                              self.days_countas_conversion)
        
        generate_prediction_data_set_class_use.discount_initialization()
        generate_prediction_data_set_class_use.process()
        
        return res.drop(columns = ['label'])