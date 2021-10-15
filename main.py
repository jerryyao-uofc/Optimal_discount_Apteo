from klaviyo_client_previous_discount_extraction import KlaviyoClientPreviousDiscountExtraction
from generate_optimal_discount_initial_training_dataset import GenerateOptimalDiscountInitialTrainingDataset
from improve_optimal_discount_training_dataset import ImproveOptimalDiscountTrainingDataset
from generate_prediction_dataset_for_exisitng_customer import GeneratePredictionDatasetForExistingCustomer
from optimal_discount_training_and_prediction import OptimalDiscountTrainingAndPrediction
from upload_optimal_discount_table_to_bigquery import UploadOptimalDiscountTableToBigQuery

# all of the above imports are the files from this same directoy. So this can be imported pretty 


from hydronize_optimal_discount_query import SQL_TRANSACTIONS_AND_CUSTOMER_EMAIL_MACTCHING, SQL_CUSTOMER_QUERY

# importing the bigquery functions from the code base, might need to fix the path because the directory has been changed
from utils.src.main.code.io.bigquery_client import BigqueryClient
bigquery_client = BigqueryClient(environment = "production")

import pandas as pd
import numpy as np


"""
main.py is designated to run the project from start to finish but it may not be the best way 
 to understand what the intermediate dataframe look like.

So in order to understand what they look like, it's better to take a look at the jupyter notebook called
"intermediate steps and dataframe". The code are the exactly the same. 
""" 

def main():
    campaign_url = "https://a.klaviyo.com/api/v1/campaigns"
    canpaign_info = "https://a.klaviyo.com/api/v1/campaign"
    hydro_key =  '00000'

    regexes = ['discount', 'DISCOUNT' , '[0-9][0-9]%.discount', 
               '[0-9][0-9]%.DISCOUNT'
               'save.[0-9][0-9]%', 'SAVE.[0-9][0-9]%',
               '[0-9][0-9]%.off', '[0-9][0-9]%.OFF', 
               'code.[a-zA-Z0-9]+', 
               'code:.[a-zA-Z0-9]+','CODE:.[a-zA-Z0-9]+']

    ADD_PATH = '00000'

    klaviyo_client_interaction = KlaviyoClientPreviousDiscountExtraction(campaign_url = campaign_url, 
                                                                         campaign_info = canpaign_info, 
                                                                         API_key = hydro_key, 
                                                                         regex_usage = regexes)
    discount_history_all_customers = klaviyo_client_interaction.extract_discount_all(image_on = False)

    res = bigquery_client.execute_query_and_return_results(SQL_TRANSACTIONS_AND_CUSTOMER_EMAIL_MACTCHING, return_as_records=True)
    previous_transaction_all = pd.DataFrame.from_dict(res)

    generate_initial_training_dataset= GenerateOptimalDiscountInitialTrainingDataset(previous_discount_dataset = discount_history_all_customers, 
                                                                                     previous_transaction_record = previous_transaction_all, 
                                                                                     days_countas_conversion = 30)
    generate_initial_training_dataset.datasets_init()
    generate_initial_training_dataset.process()

    discount_history = discount_history_all_customers.copy()


    improve_training_dataset = ImproveOptimalDiscountTrainingDataset(initial_training_dataset = discount_history_all_customers)
    final_training_dataset_optimized = improve_training_dataset.optimize_initial_training_dataset()


    res_customer = bigquery_client.execute_query_and_return_results(SQL_CUSTOMER_QUERY, return_as_records=True)
    df_customer = pd.DataFrame.from_dict(res_customer)
    customer_list = df_customer['email_address'].values.tolist()


    prediction_set = GeneratePredictionDatasetForExistingCustomer(existing_customer_list = customer_list, 
                                                                  previous_discount_dataset = discount_history_all_customers, 
                                                                  previous_transaction_record = previous_transaction_all, 
                                                                  discount_ceiling = 31, 
                                                                  discount_step = 9 , 
                                                                  days_countas_conversion = 30 )

    final_prediction_set = prediction_set.generate_prediction_data_set()
    predict_customer_info = final_prediction_set[['email', 'campagin_ID', 'discount', 'date']].copy()


    features_to_delete = ['email','campagin_ID', 'date', 'customer_ID_klaviyo',
          'shipping_address_city','shipping_address_state', 'shipping_address_country',
           'shipping_address_zip_code','order_reference', 'order_reference_pf']

    training_ready = final_training_dataset_optimized.drop(columns = features_to_delete)
    prediction_ready = final_prediction_set.drop(columns = features_to_delete)


    training_and_prediction = OptimalDiscountTrainingAndPrediction(training_data_set = training_ready, 
                                                                    prediction_data_set = prediction_ready)
    purchase_probability = training_and_prediction.train_and_predict()



    upload_to_big_query = UploadOptimalDiscountTableToBigQuery(table = predict_customer_info.join(purchase_probability),
                                                               CUSTOMER_QUERY = SQL_CUSTOMER_QUERY, 
                                                               UPLOAD_PATH = ADD_PATH)
    upload_to_big_query.process_and_upload_to_big_query()

if __name__ == "__main__":
    main()