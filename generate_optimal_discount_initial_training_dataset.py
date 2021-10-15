import json
import glob, os
from matplotlib import image
import numpy as np
import pandas as pd

import holidays
from datetime import datetime, timedelta
from time import strftime
from datetime import datetime
import json
from collections import Counter

class GenerateOptimalDiscountInitialTrainingDataset: 
    
    def __init__(self, previous_discount_dataset, previous_transaction_record, days_countas_conversion): 
        self.previous_discount_dataset = previous_discount_dataset
        self.previous_transaction_record = previous_transaction_record 
        self.days_countas_conversion = days_countas_conversion
         
    @staticmethod
    def time_formating_ft(timestamp):
        # only need to and should run only once 
        # converting two time formats into the same kind 
        date_time_str = timestamp.to_pydatetime().strftime("%Y-%m-%d %H:%M")
        date_time_object = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M')
        return date_time_object

    def processing_time_format(self):
        for i in (self.previous_discount_dataset).index: 
            (self.previous_discount_dataset).at[i, "date"] = datetime.strptime((self.previous_discount_dataset).at[i, "date"], '%Y-%m-%d %H:%M:%S')
        
        for i in (self.previous_transaction_record).index: 
            (self.previous_transaction_record).at[i , "created_at"] = self.time_formating_ft((self.previous_transaction_record).at[i , "created_at"])
  
    def add_zero_discount(self): 

        dataframe1 = self.previous_discount_dataset
        dataframe2 = self.previous_transaction_record
        
        zero_discount_transaction = dataframe2[dataframe2['discount_amount'] < 0.01]
        for index, row in zero_discount_transaction.iterrows():
            email = row['email_address']
            # creates a proxy email with discount of 0 percent sent to his customer 3 days before purchase
            update = row['created_at'] - timedelta(days=3)
            # this actuall makes sense in the business narrative. 
            # when a customer made a purchase with zero percent, we can say that they basically forgot
            # there were previous discounts being sent to them. So all the historical discount data is 0 
            dataframe1.loc[len(dataframe1.index)] = [email, '', 0 , update, '',0,0,0]
    
    def discount_initialization(self):
        """
        initilize the d dataframe with the features we will create

        :param d (dataframe): the discount table 

        Returns:
            Nothing
        """
        self.previous_discount_dataset['shipping_address_zip_code'] = ""
        self.previous_discount_dataset['shipping_address_city'] = ""
        self.previous_discount_dataset['shipping_address_state'] = ""
        self.previous_discount_dataset['shipping_address_country'] = ""
        self.previous_discount_dataset['highest_discount']= 0.0
        self.previous_discount_dataset['lowest_discount'] = 0.0
        self.previous_discount_dataset['average_discount'] = 0.0
        self.previous_discount_dataset['first_purchase_discount'] = 0.0
        self.previous_discount_dataset['days_since_last_purchase'] = 0 
        self.previous_discount_dataset['days_since_first_purchase'] = 0 

        # dollars
        self.previous_discount_dataset['customer_lifetime_orders'] = 0.0
        self.previous_discount_dataset['customer_average_order'] = 0.0
        self.previous_discount_dataset['total_spent_last_30']  = 0.0
        self.previous_discount_dataset['total_spent_last_60']  = 0.0
        self.previous_discount_dataset['total_spent_last_90']  = 0.0
        self.previous_discount_dataset['total_spent_last_180']  = 0.0
        self.previous_discount_dataset['total_spent_last_365']  = 0.0
        self.previous_discount_dataset['total_spent_last_730']  = 0.0
        self.previous_discount_dataset['average_between'] = 0.0 

        # 0 or 1  (means yes or no)
        self.previous_discount_dataset['last_purchase_around_thanksgiving'] = 0 
        self.previous_discount_dataset['last_purchase_before_chirstmas'] = 0

        # 0 or quantities 
        self.previous_discount_dataset['top_prodct_1'] = 0 
        self.previous_discount_dataset['top_prodct_2'] = 0 
        self.previous_discount_dataset['top_prodct_3'] = 0 
        self.previous_discount_dataset['top_prodct_4'] = 0 
        self.previous_discount_dataset['top_prodct_5'] = 0 
        self.previous_discount_dataset['top_prodct_6'] = 0 
        self.previous_discount_dataset['top_prodct_7'] = 0 
        self.previous_discount_dataset['top_prodct_8'] = 0 

        self.previous_discount_dataset['top_eight_spent'] = 0.0 

        # label
        self.previous_discount_dataset['label'] = 0

        # record
        self.previous_discount_dataset['order_reference'] = ''
        self.previous_discount_dataset['order_reference_pf'] = 0 
            
    def datasets_init(self): 
        """
        initialize the data sets

        : param dataframe1 (dataframe): df, aka "orders table"
        : param dataframe2 (dataframe): discount, aka "discount information"

        Returns:
            Nothing
        """
        # process the time formats
        self.processing_time_format()
        # add zero discount to 
        self.add_zero_discount()
        # initilize some features 
        self.discount_initialization()
        
    
    # return a list of top products 
    @staticmethod
    def collect_top_product(column_data):
        res = []
        dict_record = {}

        for i in range(0,len(column_data)):
            if i in column_data.index: 
                list_purchase = json.loads(column_data[i])
                for j in range(0,len(list_purchase)): 
                    product_bouhgt = list_purchase[j]['product_name']
                    if product_bouhgt in dict_record: 
                        quant = dict_record[product_bouhgt] + list_purchase[j]['quantity']
                        dict_record[product_bouhgt] = quant
                    else: 
                        dict_record[product_bouhgt] = list_purchase[j]['quantity']

        k = Counter(dict_record)
        return [p[0] for p in k.most_common(8) ] 

    # collect price of all the products
    # return a dictionary 
    @staticmethod
    def price_collector(column_data):
        dict_price = {}
        for i in range(0,len(column_data)):
            # check if this row is still in the dataframe 
            if i in column_data.index:
                list_purchase = json.loads(column_data[i])
                for j in range(0,len(list_purchase)): 
                    product_bouhgt = list_purchase[j]['product_name']
                    if product_bouhgt not in dict_price: 
                        dict_price[product_bouhgt] = list_purchase[j]['product_cost']
        return dict_price 

    # return a dictionary of product bought
    # bought what -> how many 
    @staticmethod
    def collect_top_product_dict(column_data):
        dict_record = {}
        for i in range(0,len(column_data)):
            if i in column_data.index:
                list_purchase = json.loads(column_data.iloc[i])
                for j in range(0,len(list_purchase)): 
                    product_bouhgt = list_purchase[j]['product_name']
                    if product_bouhgt in dict_record: 
                        quant = dict_record[product_bouhgt] + list_purchase[j]['quantity']
                        dict_record[product_bouhgt] = quant
                    else: 
                        dict_record[product_bouhgt] = list_purchase[j]['quantity']
        return dict_record
    
    def process(self):
        """
        create all the features we want 

        :param new_df (dataframe): the updated orders table, same as df 
        :param discount (dataframe): the discount table 
        """
        discount = self.previous_discount_dataset
        new_df = self.previous_transaction_record

        # before going row by row, need to record the top 8 products: 
        top_eight_list = self.collect_top_product(new_df['line_items'])
        # and record the price for each product: 
        price_line = self.price_collector(new_df['line_items'])

        for i in discount.index:  
            print(i)
            current_email = discount['email'][i]
            current_time = discount['date'][i]

            # works for hydronize
            search_transaction = new_df[new_df['email_address'] == current_email]

            if len(search_transaction) != 0: 
                # looking at all the transactions that have done by (before) this time stamp (discount email)
                relavant_transaction = search_transaction[search_transaction['created_at'] <= current_time]

                # looking at all the transactions that have done after 30 days this time stampe (discount email)
                possible_conversion = search_transaction[(search_transaction['created_at'] > current_time)
                                                         & (search_transaction['created_at'] < current_time + timedelta(days=self.days_countas_conversion))]

                # Life time value in store (ignoring CANDADIAN DOLLAR for now, treat it as US dollar)
                discount['customer_lifetime_orders'][i] = relavant_transaction['total_amount'].sum()

                # discount recieved on first purchase 
                first_purchase_df = relavant_transaction[relavant_transaction.created_at == relavant_transaction.created_at.min()]
                if len(first_purchase_df)>0:   

                    s = first_purchase_df['discount_percentage'].iloc[0]
                    discount['first_purchase_discount'][i] = s

                    # days since first purchase 
                    days = (first_purchase_df['created_at'].iloc[0] - current_time).days

                    discount['days_since_first_purchase'][i] = days

                # average time (days) between pruchases for this customer 
                if len(relavant_transaction) > 1: 
                    total_between = 0
                    for t in range(0, len(relavant_transaction)-1):
                        incre = abs((relavant_transaction['created_at'].iloc[t+1] - relavant_transaction['created_at'].iloc[t]).days)
                        total_between = total_between+ incre
                    discount['average_between'][i] = total_between/len(relavant_transaction)

                if len(relavant_transaction) > 0: 
                    # purchased contexts analysis : 
                    top_purchase = self.collect_top_product_dict(relavant_transaction['line_items'])
                    sum_spent_top = 0.0
                    for a in range(0,len(top_eight_list)): 
                        # top 8 most purchased items (unit of purchases)
                        if top_eight_list[a] in top_purchase: 
                            discount['top_prodct_'+str(a+1)][i] = top_purchase[top_eight_list[a]]
                            sum_spent_top = sum_spent_top + (price_line[top_eight_list[a]]) * (top_purchase[top_eight_list[a]])

                    # total spent on the top 8 products 
                    discount['top_eight_spent'][i] = sum_spent_top

                    # preivous discount usage situation 
                    discount_purchased = relavant_transaction['discount_percentage']
                    discount['highest_discount'][i] = discount_purchased.max()
                    discount['lowest_discount'][i] = discount_purchased.min()
                    discount['average_discount'][i] = discount_purchased.mean() 

                    # Customer average order size
                    discount['customer_average_order'][i] = (discount['customer_lifetime_orders'][i])/len(relavant_transaction)

                    # total spnet in the past 30, 60, 90, 180, 365, 730
                    last_30_transaction = relavant_transaction[relavant_transaction['created_at'] > (current_time - timedelta(days=30))]
                    last_60_transaction = relavant_transaction[relavant_transaction['created_at'] > (current_time - timedelta(days=60))]
                    last_90_transaction = relavant_transaction[relavant_transaction['created_at'] > (current_time - timedelta(days=90))]
                    last_180_transaction = relavant_transaction[relavant_transaction['created_at'] > (current_time - timedelta(days=180))]
                    last_365_transaction = relavant_transaction[relavant_transaction['created_at'] > (current_time - timedelta(days=365))]
                    last_730_transaction = relavant_transaction[relavant_transaction['created_at'] > (current_time - timedelta(days=730))]

                    discount['total_spent_last_30'][i]  = last_30_transaction['total_amount'].sum()
                    discount['total_spent_last_60'][i]  = last_60_transaction['total_amount'].sum()
                    discount['total_spent_last_90'][i]  = last_90_transaction['total_amount'].sum()
                    discount['total_spent_last_180'][i]  = last_180_transaction['total_amount'].sum()
                    discount['total_spent_last_365'][i]  = last_365_transaction['total_amount'].sum()
                    discount['total_spent_last_730'][i]  = last_730_transaction['total_amount'].sum()

                else: 
                    discount['highest_discount'][i] = 0
                    discount['lowest_discount'][i] = 0
                    discount['average_discount'][i] = 0

                # days since last purchase 
                last_purchase_df = relavant_transaction[relavant_transaction.created_at == relavant_transaction.created_at.max()]
                if len(last_purchase_df)>0:

                    # days since last purchase 
                    discount['days_since_last_purchase'][i] = (last_purchase_df['created_at'].iloc[0] - current_time).days

                    # check if last purcahse made near thanksgiving (10 days before or after thanks giving)
                    last_purchase_date = last_purchase_df['created_at'].iloc[0]

                    thanksgiving = list(holidays.UnitedStates(years=last_purchase_date.year))[-2]
                    diff_thanks = abs((last_purchase_date.date() - thanksgiving).days)

                    if diff_thanks < 10: 
                        discount['last_purchase_around_thanksgiving'][i] = 1

                    # check if last purhcase is bought before christmas
                    christmas = list(holidays.UnitedStates(years=last_purchase_date.year))[-1]
                    diff_christmas = (christmas -last_purchase_date.date() ).days
                    if diff_christmas < 15: 
                        discount['last_purchase_before_chirstmas'][i] = 1

                    #address information we know by the timestamp of the discount 
                    #(note) if a purchase is placed after recieving the email, then we don't know the address
                    discount['shipping_address_zip_code'][i] = last_purchase_df['shipping_address_zip_code'].iloc[0]
                    discount['shipping_address_city'][i] = last_purchase_df['shipping_address_city'].iloc[0]
                    discount['shipping_address_state'][i] = last_purchase_df['shipping_address_state'].iloc[0]
                    discount['shipping_address_country'][i] = last_purchase_df['shipping_address_country'].iloc[0]

                #creating labels (Most important part)
                for q in range(0,len(possible_conversion)): 
                    discount_purchased = (possible_conversion['discount_percentage'].iloc[q])*100
                    discount_offered = discount['discount'][i]
                    discount["label"][i] = 1
                    discount['order_reference'][i] = possible_conversion['order_id'].iloc[q]
                    discount['order_reference_pf'][i] = possible_conversion['discount_percentage'].iloc[q]
        

