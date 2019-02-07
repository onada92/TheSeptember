import pandas as pd
import numpy as np

# """
# DATE: OCT,26

# FILE DESCRIBTION: 
# IN THIS FILE, THE TABLES ARE BEING UPLOADED, THE TABLES ARE:
# 	1) PERSON TABLE
# 	2) PRODUCT TABLE 
# 	3) SESSION TABLE
# 	4) ECOMMERCE TABLE 
# 	5) EVENT PAGE TABLE
# """



# #----------------------------------------------IMPORTING PERSON TABLE--------------------------------------------------------------
def getPerson():	
	# with open("D:/TheSeptember/Data/New/FINAL/personsTableL.csv", 'rb') as f:
	#     result = chardet.detect(f.read())
	personTableF = pd.read_csv("D:/TheSeptember/Data/New/FINAL/personTable.csv",low_memory=False)
	personTable = pd.DataFrame(data = personTableF)
	personTable.columns = ['user_id','last_fc_update','email_address','first_name','last_name','full_name','age','age_range','gender','company','title','country','city','region','postal','address','longitude','latitude','avatar','accept_language','prefer_language','facebook_id','facebook_username','facebook_url','linked_in_id','linked_in_username','linked_in_url','linked_in_bio','twitter_username','twitter_followers','first_seen','first_location','first_location_host','first_location_path','first_location_query','first_location_fragment','first_referrer','first_referrer_host','first_referrer_path','first_referrer_query','first_referrer_fragment','first_utm_campaign','first_utm_content','first_utm_medium','first_utm_source','first_utm_term','last_seen','last_ip_address','last_geo_ip_country','last_geo_ip_city','last_geo_ip_region','last_geo_ip_postal','last_geo_ip_longitude','last_geo_ip_latitude','last_session_id','date_of_birth','phone','orders_placed','orders_total','last_order_time','last_order_total','multi_device','last_user_agent','last_operating_system','last_device_type','last_browser','last_browser_major','first_order_time','first_order_total','session_count','first_order_session_number','last_utm_campaign','last_utm_content','last_utm_medium','last_utm_source','last_utm_term','refunds_given','refunds_total','first_location_secure','first_referrer_secure','person_id','custom_traits','first_order_revenue','last_order_revenue','revenue_avg','revenue_avg_bucket','revenue_sum','revenue_sum_bucket','session_count_bucket','source_category','first_order_days_to','first_utm_combined','has_email','has_purchased','last_utm_combined','total_avg','total_avg_bucket','total_sum_bucket']
	last_ip = personTable['last_ip_address']
	personTable['last_ip'] = last_ip.str[:15]
	#personTable[personTable.orders_total.apply(lambda x: x.isnumeric())]
	personTable_feat = personTable[['person_id', 'age', 'gender', 'has_purchased', 'last_ip', 'first_order_session_number', 'last_session_id', 'first_order_total', 'last_order_total', 'orders_placed', 'orders_total', 'first_order_revenue', 'last_order_revenue', 'revenue_sum_bucket','revenue_avg_bucket', 'session_count', 'session_count_bucket', 'total_sum_bucket','total_avg', 'total_avg_bucket']]
	#print ("The person table is: \n",personTable_feat.dtypes)
	return (personTable_feat)
	
# #----------------------------------------------IMPORTING PRODUCT TABLE------------------------------------------------------------
def getProduct():
	# with open("D:/TheSeptember/Data/New/FINAL/ProductTable.csv", 'rb') as f:
	#     result = chardet.detect(f.read())

	productTableF = pd.read_csv("D:/TheSeptember/Data/New/FINAL/productTable.csv", low_memory=False)
	productTable = pd.DataFrame(data = productTableF)
	productTableColumn = ['session_id','id','sku','price','quantity','price_bucket','person_id','event_id']

	productTable.columns = productTableColumn
	#productTable_feat = productTableF[['event_id','person_id','session_id','sku', 'name', 'price', 'quantity', 'price_bucket', 'category_id', 'category_name']]
	# print("The product table is : ",productTable)
	#print("The product tabe id is: \n",  productTable.event_id)
	#print("-----------------------------------------------------------")
	return (productTable)
# #---------------------------------------------IMPORTING SESSION TABLE------------------------------------------------------------
def getSession():

# with open("D:/TheSeptember/Data/New/FINAL/sessionsTable.csv", 'rb') as f:
#     result = chardet.detect(f.read())
	sessions = pd.read_csv("D:/TheSeptember/Data/New/FINAL/sesionTable11.csv", low_memory = False)
	sessionTable = pd.DataFrame(data = sessions)
	sessions.columns = ['session_id','first_seen','first_ip_address','first_geo_ip_country','first_geo_ip_city','first_geo_ip_region','first_geo_ip_postal','first_geo_ip_longitude','first_geo_ip_latitude','first_user_agent','first_operating_system','first_device_type','first_browser','first_browser_major','first_location','first_location_host','first_location_path','first_location_query','first_location_fragment','first_referrer','first_referrer_host','first_referrer_path','first_referrer_query','first_referrer_fragment','first_utm_campaign','first_utm_content','first_utm_medium','first_utm_source','first_utm_term','last_seen','last_location','last_location_host','last_location_path','last_location_query','last_location_fragment','click_count','input_count','view_count','watch_count','track_count','ecommerce_count','page_count','orders_total','orders_placed','person_alias','source_category','first_utm_combined','session_duration','session_duration_interval','page_count_interval','orders_interval','cf_first_utm_campaign','cf_first_utm_medium','cf_first_utm_source','cf_first_utm_term','cf_first_utm_content','cf_first_utm_combined','visit_type','refunds_given','refunds_total','first_location_secure','first_referrer_secure','last_location_secure','identify_count','id','person_id','cf_source_category','revenue_sum','revenue_sum_bucket']
	sessions = sessions [['session_id','click_count','input_count','view_count','watch_count','track_count','ecommerce_count','page_count','orders_total','orders_placed','session_duration','session_duration_interval','page_count_interval','orders_interval','visit_type','refunds_given','refunds_total','identify_count','id','person_id','revenue_sum','revenue_sum_bucket']]
	sub_sessions = sessions[sessions.person_id == '213aa6fe-69fb-11e8-ad03-0242ac110003']
	# print("The session id is: \n", sub_sessions)
	# print("-----------------------------------------------------------")
	return (sessions)
# #----------------------------------------------IMPORTING ECOMMERCE TABLE----------------------------------------------------
def getEcommerce():
	#FIX
	#UnicodeEncodeError: 'charmap' codec can't encode character '\u2013' in position 10159: character maps to <undefined>
	ecommerceTables = pd.read_csv("D:/TheSeptember/Data/New/FINAL/ecommerce1.csv", low_memory = False)
	ecommerceTable = ecommerceTables.iloc[:,0:46]
	ecommerceTable.columns = ['event_name','timestamp_','person_alias','visitor_id','session_id','page_title','location_host','location_path','location_query','location_fragment','referrer_host','referrer_path','referrer_query','referrer_fragment','event_sub_type','category_id','category_name','quote_id','order_id','total','revenue','shipping','shipping_method','tax','discount','coupon','quote_total_quantity','quote_line_item_count','currency','fx_rate','source','subtotal','note','order_channel','tags','discount_bucket','revenue_bucket','shipping_bucket','subtotal_bucket','tax_bucket','total_bucket','location_secure','referrer_secure','id','person_id','custom_properties']
	ecommerceTable = ecommerceTable[['event_name','visitor_id','session_id','page_title','category_id','category_name','quote_id','order_id','total','revenue','discount','tags','discount_bucket','revenue_bucket','subtotal_bucket','total_bucket','id','person_id']]
	#print("E-commerce table id is: \n",ecommerceTable.id)
	#print("-----------------------------------------------------------") 
	return (ecommerceTable)
# #----------------------------------------------IMPORTING EVENT_PAGE TABLE------------------------------------------------------------
def getEpage():
	# with open("D:/TheSeptember/Data/New/FINAL/eventPage.csv", 'rb') as f:
	#     result = chardet.detect(f.read())
	epageF = pd.read_csv("D:/TheSeptember/Data/New/FINAL/eventPage.csv", low_memory = False)
	epage = pd.DataFrame(data = epageF)
	epage.columns = ['timestamp_','person_alias','visitor_id','session_id','page_title','location_host','location_path','location_query','location_fragment','referrer_host','referrer_path','referrer_query','referrer_fragment','duration','location_secure','referrer_secure','id','person_id','custom_properties','bla1','bla2','ba3','bla4']
	epages = epage.iloc[:,0:19]
	epages.columns = ['timestamp_','person_alias','visitor_id','session_id','page_title','location_host','location_path','location_query','location_fragment','referrer_host','referrer_path','referrer_query','referrer_fragment','duration','location_secure','referrer_secure','id','person_id','custom_properties']
	#print("epage table is: \n", epages.id)
	return(epages)
# #--------------------------------------------------------------------------------------------------------------------------------------------------
def mergedSessions():
	sessions = getSession()
	products = getProduct()
	sessions_df = sessions.merge(products, how = 'left', left_on = 'session_id', right_on = 'session_id')
	#sessions_df.columns = sessions_df[['']]
	#print(sessions_df)
	return(sessions)

#getPerson()
#getProduct()
#getSession()
#getEcommerce()
#getEpage()
#mergedSessions()