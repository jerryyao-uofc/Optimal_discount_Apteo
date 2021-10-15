import pandas as pd
import numpy as np

class ImproveOptimalDiscountTrainingDataset: 
    def __init__(self, initial_training_dataset): 
        self.initial_training_dataset = initial_training_dataset 
    
    @staticmethod
    def delete_repeat(dataframe1):
        """
        negative label downsampling - delete negative label's repeated discount offer 
        Returns:
            Nothing
        """
        for index, row in dataframe1.iterrows():
            email_string = row['email']
            discount_match = row['discount']
            discount_captured = (dataframe1[dataframe1['email']==email_string])
            sub_captured = (discount_captured[(discount_captured['discount'] == discount_match) & (discount_captured['label']==0)])
            if (len(sub_captured) > 1 and row['label'] == 0): 
                dataframe1.drop(index, inplace=True)
        return dataframe1
    
    @staticmethod
    def delete_order_zero(dataframe1):
        """
        negative and positive label downsampling - delete the follwing 
        1) those who have never purchased a single thing in the store and they still didn't
            despite giving them a discount 
        2) those who we know thing about and just show up and make the first purchase 
            without using any discount (essentialy all the features are zero)
        Returns:
            Nothing
        """
        for index, row in dataframe1.iterrows():
            if (row['customer_lifetime_orders']==0) & (row['label'] == 0 ): 
                dataframe1.drop(index, inplace=True)
            if (row['customer_lifetime_orders']==0) & (row['label'] == 1) &(row['discount'] == 0):
                dataframe1.drop(index, inplace=True)
        return dataframe1
    
    @staticmethod
    def delete_positive_repeat(dataframe1):
        """
        remove positive label's repeating label. 
        For example a puchase can be made after recieing three discount emails: 20%, 30%, 33% off
            we are only keeping the email that is the most recent. In this case is 33%
            here we delete the rest 
        Returns:
            Nothing
        """
        positive_case = dataframe1[dataframe1['label']==1]

        for index, row in positive_case.iterrows():
            order_ref = row['order_reference']
            all_repeat = dataframe1[(dataframe1['label']==1) & (dataframe1['order_reference'] == order_ref)]
            if len(all_repeat)>1: 
                sorted_repeat = all_repeat.sort_values(by=['date'], ascending=False)
                for index_i, row_i in sorted_repeat.iterrows(): 
                    if row_i['date'] != sorted_repeat['date'].iloc[0]:
                        dataframe1.drop(index_i, inplace=True)
        return dataframe1
    
    @staticmethod
    def show_sample_info(dataframe1):
        """
        just to show the basic spread of positive and negative labels 

        :param sorted_dis (dataframe): it's the same discount dataframe we have been using along the way

        Returns:
            Nothing
        """       
        a = len(dataframe1)
        b = len(dataframe1[dataframe1['label']==1])
        c = len(dataframe1[dataframe1['label']==0])
        # print("total {}\n positive: {}, about {:0.2f} \n negative: {}, about {:0.2f}".format(a,b, b/a , c, c/a))
        return (b,c)
    
    
    def optimize_initial_training_dataset(self): 
        post_delete_repeat = self.delete_repeat(self.initial_training_dataset.sort_values(by=['label'], ascending=True))
        self.show_sample_info(post_delete_repeat)
        
        post_delete_order_zero = self.delete_order_zero(post_delete_repeat)
        self.show_sample_info(post_delete_order_zero)
        
        post_delete_positive_repeat = self.delete_positive_repeat(post_delete_order_zero)
        self.show_sample_info(post_delete_positive_repeat)
        
        return post_delete_positive_repeat