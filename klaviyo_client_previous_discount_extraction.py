import sys
import os
import requests
import json
import re
from bs4 import BeautifulSoup
from IPython.core.display import display, HTML
# image necesseities 
import cv2
import pytesseract 
from skimage import io
import numpy as np
import urllib.request
import cv2
import operator
import pandas as pd




class KlaviyoClientPreviousDiscountExtraction:
    def __init__(self, campaign_url, campaign_info, API_key, regex_usage):
        
        self.campaign_url = campaign_url
        self.campaign_info = campaign_info
        self.API_key = API_key
        self.regex_usage = regex_usage
        
    def list_campaign_id(self, page):
        """
         fetch all the campagins and returns a list of campagisn 

         :param url (string): the url of the campaign url 
         :param api_key (string): the api key for a specific store 
         :param Page (int): the specific page of the capmagisn trying to obtain 

         Returns:
             a list of campagin IDs 
        """
        res = []
        # count WARNING!
        querystring = {"api_key":self.API_key, "page": str(page), "count": "100"}
        headers = {"Accept": "application/json"}
        response = requests.request("GET", self.campaign_url, headers=headers, params=querystring)
        
        all_campaigns = json.loads(response.text)   
        for each in all_campaigns['data']:
            res.append(each['id'])
        return res 
    
    def get_all_list_ids(self): 
    # get the list of campaign IDs
        res = []
        k = 0 
        first_iter = self.list_campaign_id(k)
        len_now = len(first_iter)
        res.extend(first_iter)
        while len_now == 100:
            k = k+1
            iterr = self.list_campaign_id(k)
            res.extend(iterr)
            len_now = len(iterr)
        return res

    # extract the html file of the provided list of campaign id 
    def generate_html_list(self, ids):
        sub = []
        res = []
        for each in ids: 
            try:
                url_combine = self.campaign_info + "/" + each
                querystring = {"api_key":self.API_key}
                headers = {"Accept": "application/json"}
                response = requests.request("GET", url_combine, headers=headers, params=querystring)
                json_object = json.loads(response.text)
                subject = json_object['subject']
                each_html = json_object['template']['html']
                res.append(each_html)
                sub.append(subject)

            except Exception as e: 
                sub.append("error")
                res.append("")
        return sub, res
    
    
    # OpenCV, NumPy, and urllib
    def url_to_image(url):
        # download the image, convert it to a NumPy array, and then read
        # it into OpenCV format
        resp = urllib.request.urlopen(url)
        image = np.asarray(bytearray(resp.read()), dtype="uint8")
        image = cv2.imdecode(image, cv2.IMREAD_COLOR)
        # return the image
        return image

    # input a image url, return the string 
    def image_to_string(img):
        image = url_to_image(img)
        text = pytesseract.image_to_string(image)
        return text 

    # switch = true -> turn on image-text extraction 
    # switch = False -> turn off image-text extraction 
    @staticmethod
    def generate_text(sjct_ls, html_ls, switch):
        res = []
        for title, each in zip(sjct_ls, html_ls):
            soup = BeautifulSoup(each, 'html.parser')
            str_html = " ".join(soup.get_text().split())

            # initializaing a string 
            string_init = ''
            if switch == True: 
                all_images = soup.findAll('img')
                for image in all_images: 
                    try: 
                        string_init = string_init + ' ' + image_to_string(image['src'])
                    except Exception as e: 
                        string_init = string_init

            res.append(title + ' ' + string_init + ' ' + str_html)
        return res

    # genetrate discount amount for each of all the emails
    # print the index of email and discount amount
    # campagin_IDs : a list of campaigns - a list of strings
    # dict_camp a dictionary: storing campagin ID to a discount rate
    def generat_discount(self, all_emails_text, campign_IDs, dict_camp):
        result_list = []
        i=0
        discount_count = 0
        for each in all_emails_text: 
            temp_res = []
            for reg in self.regex_usage: 
                all_res_reg= re.findall(reg, each)
                for item in all_res_reg: 
                    temp_res.append(item)

            all_num = [0]
            for item_temp in temp_res:
                x = re.findall('[0-9]+', item_temp)

                for item in x:
                    temp = int(item)
                    if temp < 100: 
                        all_num.append(temp)

            disc = max(all_num)
            if disc != 0: 
                discount_count = discount_count+ 1
                result_list.append(campign_IDs[i])
                dict_camp[campign_IDs[i]] = disc

            i= i+1

        return result_list
    
    # request all the recipients 
    # campagisn is a list of campagin IDs:
    def read_campaign(self, campains, dict_customer, dict_campagin_To_recipients):
        total_len = 0
        for campaign in campains: 
            url = "https://a.klaviyo.com/api/v1/campaign/" + campaign + "/recipients"
            querystring = {"api_key":self.API_key , "count":"25000","sort":"asc"}
            headers = {"Accept": "application/json"}
            response = requests.request("GET", url, headers=headers, params=querystring)

            object_recipients = json.loads(response.text)       
            list_recipients_this_email = []

            for recipient in object_recipients['data']: 
                customer_email = recipient['email']
                list_recipients_this_email.append(customer_email)

                if customer_email not in dict_customer:
                    dict_customer[customer_email] = recipient['customer_id']

            total_len = len(list_recipients_this_email) + total_len
            dict_campagin_To_recipients[campaign] = list_recipients_this_email
        return total_len
    
    def get_campaign_date(self, discount_campaigns, dict_date): 
        # page COUNT WARNINGS
        # api_key_unique (API_key), discount_campaigns, dict_date
        # url = "https://a.klaviyo.com/api/v1/campaigns"
        
        # might need to come back to here
        # currently this only work with the first page of campagin ID
        # might need to write an addition function to get the page of other
        querystring = {"api_key":self.API_key,"page":"0","count":"100"}
        headers = {"Accept": "application/json"}
        response = requests.request("GET", self.campaign_url, headers=headers, params=querystring)
        campaigns_load = json.loads(response.text) 

        for each in campaigns_load['data']:
            campagin_str = each['id']
            if campagin_str in discount_campaigns: 
                dict_date[campagin_str] = each['sent_at']
        return
    
    @staticmethod
    def build_df(dict_recipt2Campagin, dict_discount, dict_klaviyoID, dict_date ,
            dict_max_prev, dict_prev_seen, dict_prev_avera):
        # working with all these stored dictionary and produce the final dataframe 
        df = pd.DataFrame(columns = ('email', 'campagin_ID', 'discount', 'date', 'customer_ID_klaviyo',
                                    'max_previous_discount', 'count_discount_recieved', 
                                     'average_previous_discount_recieved'))
        for email in dict_recipt2Campagin: 
            one_user_campaigns = dict_recipt2Campagin[email]
            for each_campagin in one_user_campaigns: 
                try:
                    new_row = {'email': email,
                               'campagin_ID': each_campagin, 
                               'discount': dict_discount[each_campagin],
                               'date': dict_date[each_campagin],
                               'customer_ID_klaviyo':dict_klaviyoID[email], 
                               'max_previous_discount': dict_max_prev[(email, each_campagin)],
                               'count_discount_recieved': dict_prev_seen[(email, each_campagin)], 
                               'average_previous_discount_recieved': dict_prev_avera[(email, each_campagin)]}
                    df= df.append(new_row,ignore_index = True)
                except Exception as e: 
                    df= df
        return df

    @staticmethod
    # populate the dictionary 
    def reverse_dict(dict_campagin_to_Recipient, dict_customer, dict_recipient_to_campagin):

        for each in dict_customer: 
            dict_recipient_to_campagin[each] = []

        for each_ID in dict_campagin_to_Recipient: 
            campaign_email_list = dict_campagin_to_Recipient[each_ID]


            for each_email in campaign_email_list: 
                oldlist = dict_recipient_to_campagin[each_email]
                oldlist.append(each_ID)
                dict_recipient_to_campagin[each_email] = oldlist

        return

    @staticmethod
    def sort_campagin_ID(list_IDs, dict_date):
        temp_list = []

        for each in list_IDs: 
            try:
                # because date of some campaigns are only found in the first page 
                # when running for stores not hydronize, this will not be able to find the date for 
                # campagins that is not in the first page 
                if dict_date[each] != None:
                    temp_list.append((dict_date[each], each))

            except Exception as e: 
                temp_list = temp_list 
        temp_list.sort(key = operator.itemgetter(0))
        return [x[1] for x in temp_list]

    
    def rank(self, dict_all_customer, dict_date, dict_recip_to_camp, dict_disc,
             dict_max_prev, dict_prev_seen, dict_prev_avera): 
        for each in dict_all_customer: 
            list_camps = self.sort_campagin_ID(dict_recip_to_camp[each] , dict_date)

            list_disco = [dict_disc[x] for x in list_camps]

            for i in range(len(list_disco)): 
                tuple_temp = (each, list_camps[i])
                if i ==0 :
                    dict_max_prev[tuple_temp] = 0
                    dict_prev_seen[tuple_temp] = 0
                    dict_prev_avera[tuple_temp] = 0 
                else: 
                    dict_max_prev[tuple_temp] = max(list_disco[0:i])
                    dict_prev_seen[tuple_temp] = i 
                    dict_prev_avera[tuple_temp] = (sum(list_disco[0:i])/i)
        return 

    # MAIN FUNCTION
    #image_on: True-> extract information from images 
    #image_on: False-> only extract information from text and subjects
    # name as string: the name of the company 
    def extract_discount_all(self, image_on): 

        # PHASE 1: request all emails
        # print("PHASE 1 processing ... ")
        all_email_ID = self.get_all_list_ids()
        subject_array, html_array = self.generate_html_list(all_email_ID)
        # print("PHASE 1 DONE: Recieved total number of emails: "+str(len(all_email_ID)))

        # PHASE 2: extract ext
        # print("PHASE 2 processing ... (this may take a while if you required image extraction)")
        emails_extracted = self.generate_text(subject_array, html_array, image_on)
        # print("PHASE 2 DONE: All emails texts, subjects, processed ")

        # PHASE 3 : ectracted discount
        # dictionary storing: Discount Campaign ID --> Discount percentage off
        # print("PHASE 3 processing ...")
        dict_discount = {}
        discount_campaign_ids = self.generat_discount(emails_extracted, all_email_ID, dict_discount)
        # print("PHASE 3 DONE: Extracted total emails w discount: " + str(len(discount_campaign_ids)))


        # PHASE 4: Get recipients and detailed campaign infos 
        # print("PHASE 4 processing ...")
        # dictionary storing: customer email --> Klaviyo customer ID 
        dict_customer = {}
        # dictionary storing: campagin IDs --> list of email recipients 
        dict_campagin_to_Recipient = {}
        # dictionary storing: campagin IDs --> Date
        dict_campagin_date = {}
        # dictionary storing: email address --> list of campaigns this email recieves
        dict_recipient_to_campagin= {}

        expected_entrees = self.read_campaign(discount_campaign_ids, dict_customer, dict_campagin_to_Recipient)
        # print("PHASE 4 DONE: Final data set total expected rows "+ str(expected_entrees))


        self.get_campaign_date(discount_campaign_ids, dict_campagin_date)
        # print(dict_campagin_date)
        
        self.reverse_dict(dict_campagin_to_Recipient, dict_customer, dict_recipient_to_campagin)

        dict_max_recieved = {}
        dict_previous_seen = {}
        dict_previous_average = {}


        # PHASE additional 
        self.rank(dict_customer, dict_campagin_date, dict_recipient_to_campagin, dict_discount,
             dict_max_recieved, dict_previous_seen, dict_previous_average)


        # PHASE 5: generate dataframe
        # print("PHASE 5 processing ...")
        all_data = self.build_df(dict_recipient_to_campagin,
                             dict_discount,
                             dict_customer,
                             dict_campagin_date, 
                             dict_max_recieved, dict_previous_seen, dict_previous_average)
        # print("PHASE 5 DONE: Dataframe generated")

        # PHASE 6 : save as CSV
        # all_data.to_csv(r'/Users/jerry/Desktop/Apteo/'+ name +'.csv', index = False)
        # print("DONE")

        return all_data