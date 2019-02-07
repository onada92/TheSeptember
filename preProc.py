# Importing the TS file where it imports all the data as pandas dataframe 
import TS

#Importing necessary libraries/packages
import pandas as pd
import numpy as np

from scipy.sparse import *
from scipy import *

from sklearn import preprocessing

#______________________________________________________________________________________________________________________________________

def person_processing():
	print("----------------Manipulating and cleaning the Person Table!: ------")
	
	# Choosing the columns needed 
	print("----------------Importing the table and dropping unnecessary columns------")
	filtered_person_data = TS.getPerson()
	filtered_person_data.dropna(how='all')
	filtered_person_data = filtered_person_data.dropna(subset=["person_id"])
	
	# Editing the the data types of all the columns that must be float and categorical
	print("----------------Editing the data type of the columns------")
	filtered_person_data['orders_total'] = filtered_person_data['orders_total'].apply(pd.to_numeric, downcast = 'float', errors='coerce')
	filtered_person_data['first_order_revenue'] = filtered_person_data['first_order_revenue'].apply(pd.to_numeric, downcast = 'float', errors='coerce')
	filtered_person_data['session_count'] = filtered_person_data['session_count'].apply(pd.to_numeric, downcast = 'integer', errors='coerce')
	filtered_person_data['last_order_total'] = filtered_person_data['last_order_total'].apply(pd.to_numeric, downcast = 'float', errors='coerce')
	filtered_person_data['age'] = filtered_person_data['age'].apply(pd.to_numeric, downcast = 'integer', errors = 'coerce')
	for col in ['gender', 'has_purchased']:
		filtered_person_data[col] = filtered_person_data[col].astype('category')
	
	#Setting new columns and making them strings of of 0
	print("----------------Creating the new columns")
	filtered_person_data['revenue_avg_bucket'] = filtered_person_data['revenue_avg_bucket'].fillna('0 -')
	filtered_person_data['revenue_avg_lbound'] = '0'
	filtered_person_data['revenue_sum_bucket'] = filtered_person_data['revenue_sum_bucket'].fillna('0 -')
	filtered_person_data['revenue_sum_lbound'] = '0'
	filtered_person_data['session_count_bucket'] = filtered_person_data['session_count_bucket'].fillna('0 -')
	filtered_person_data['session_count_lbound'] = '0'
	filtered_person_data['total_sum_bucket'] = filtered_person_data['total_sum_bucket'].fillna('0 -')
	filtered_person_data['total_sum_lbound'] = '0'
	filtered_person_data['total_avg_bucket'] = filtered_person_data['total_avg_bucket'].fillna('0 -')
	filtered_person_data['total_avg_lbound'] = '0'
	
	# #Obtaining the lower bound of revenue_avg_bucket
	print("----------------Getting the lower bound of the revenue average bucket and creating a new column")
	res = []
	for val in filtered_person_data['revenue_avg_bucket']:
		new_val = val.rpartition(' -')[0]
		if new_val == '':
			new_val = '0'
		#print("The new_val before any replacement is: ", new_val)
		new_num = new_val.replace(',','')
		#print("The new_num after replacement is: ",new_num)
		new_number = int(new_num)
		#print(new_number)
		res.append(new_number)
	# #print(res)
	filtered_person_data.revenue_avg_lbound = res
	
	#Obtaining the lower bound of revenue_sum_bucket
	print("----------------Getting the lower bound of the revenue sum bucket and creating a new column")
	res = []
	for val in filtered_person_data['revenue_sum_bucket']:
		new_val = val.rpartition(' -')[0]
		if new_val == '':
			new_val = '0'
		#print("The new_val before any replacement is: ", new_val)
		new_num = new_val.replace(',','')
		#print("The new_num after replacement is: ",new_num)
		new_number = int(new_num)
		#print(new_number)
		res.append(new_number)
	#print(res)
	filtered_person_data.revenue_sum_lbound = res

	#Obtaining the lower bound of the session_count_bucket
	print("----------------Getting the lower bound of the session count bucket and creating a new column")
	res = []
	for val in filtered_person_data['session_count_bucket']:
		new_val = val.rpartition('-')[0]
		if new_val == '':
			new_val = '0'
		#print("The new_val before any replacement is: ", new_val)
		new_num = new_val.replace(',','')
		#print("The new_num after replacement is: ",new_num)
		new_number = int(new_num)
		#print(new_number)
		res.append(new_number)
	filtered_person_data.session_count_lbound = res
	

	#Obtaining the lower bound of the total_sum_bucket
	print("----------------Getting the lower bound of the total sum bucket and creating a new column")
	res = []
	for val in filtered_person_data['total_sum_bucket']:
		new_val = val.rpartition(' -')[0]
		if new_val == '':
			new_val = '0'
		#print("The new_val before any replacement is: ", new_val)
		new_num = new_val.replace(',','')
		#print("The new_num after replacement is: ",new_num)
		new_number = int(new_num)
		#print(new_number)
		res.append(new_number)
	#print(res)
	filtered_person_data.total_sum_lbound = res
	

	#Obtaining the lower bound of the total_sum_bucket
	print("----------------Getting the lower bound of the total avg bucket and creating a new column")
	res = []
	for val in filtered_person_data['total_avg_bucket']:
		new_val = val.rpartition(' -')[0]
		if new_val == '':
			new_val = '0'
		#print("The new_val before any replacement is: ", new_val)
		new_num = new_val.replace(',','')
		#print("The new_num after replacement is: ",new_num)
		new_number = int(new_num)
		#print(new_number)
		res.append(new_number)
	#print (res)
	filtered_person_data.total_avg_lbound = res


	# Changing the data types of the lower bound columns to float
	print('----------------Changing the data type of the lower bound columns to float----------------')
	filtered_person_data['session_count_lbound'] = filtered_person_data.session_count_lbound.astype(float)
	filtered_person_data['revenue_avg_lbound'] = filtered_person_data.revenue_avg_lbound.astype(float)
	filtered_person_data['revenue_sum_lbound'] = filtered_person_data['revenue_sum_lbound'].astype('float')
	filtered_person_data['total_sum_lbound'] = filtered_person_data['total_sum_lbound'].astype('float')
	filtered_person_data['total_avg_lbound'] = filtered_person_data['total_avg_lbound'].astype('float')

	# Dropping the columns that were manipulated to create the lbound (lower bound) columns
	filtered_person_data = filtered_person_data.drop(columns=['revenue_sum_bucket', 'revenue_avg_bucket', 'session_count_bucket', 'total_sum_bucket', 'total_avg_bucket'])

	#Printing the data and info about the data
	# print("The description of the data is: ", filtered_person_data.describe(include='all'))
	# print("The data type of the filtered_person_data is: ",filtered_person_data.dtypes)
	#print("The full data is :", filtered_person_data.revenue_sum_lbound)
	#print('******************'*5)
	#print(filtered_person_data.person_id.describe())
	return (filtered_person_data)


def product_processing():

	# Loading the table to productTable
	print("Starting the product table pre-processing")
	productTable = TS.getProduct()

	# Editing the price bucket column and replacing it with price_lbound
	print("----------------Initiating all the new columns----------------")
	productTable['price_bucket'] = productTable['price_bucket'].fillna('0 -')
	productTable['price_lbound'] = '0'
	productTable['brand'] = '0'
	productTable['color'] = '***'
	productTable['styles'] = '****'
	productTable['brand_and_style'] = '********'

	# Changing the price bucket and getting the lower bound of it
	print("----------------Getting the lower bound of the price bucket and creating a new column----------------")
	res = []
	for val in productTable['price_bucket']:
		new_val = val.rpartition(' -')[0]
		if new_val == '':
			new_val = '0'
		#print("The new_val before any replacement is: ", new_val)
		new_num = new_val.replace(',','')
		#print("The new_num after replacement is: ",new_num)
		if (new_num == '9.50'):
			new_num = '9'
		new_number = int(new_num)
		#print(new_number)
		res.append(new_num)
	productTable.price_lbound = res
	#print(res)

	#Obtaining the brand, style, color and size from the SKU column
	print("----------------Getting the brand, style and color out from the SKU----------------")
	brand_style = []
	color = []
	style_list = []
	for val in productTable['sku']:
	#	print(type(val))
		val = str(val)
		if '-' not in val:
			brand_style.append(val)
			color.append('XXX')
		else:
			new_val = val.split('-')
			if len(new_val) >1:
				brand_style.append(new_val[0])
				color.append(new_val[1])

	productTable.brand_and_style = brand_style
	productTable.color = color

	# for brands in productTable['brand']:
	# 	brands = str(brands)
	# 	style = brands[4:]
	# 	style_list.append(style)

	# productTable.style = style_list
	#print("The style is: ", style)

	brand_style = []
	for vals in productTable['brand_and_style']:
		vals = str(vals)
		if vals == '':
			new_style = 'XXXX'
			new_brand = 'XXXX'
			style_list.append(new_style)
			brand_style.append(new_brand)
		else:
			new_brand = vals[:4]
			new_style = vals[4:]
			brand_style.append(new_brand)
			style_list.append(new_style)

	for vals in productTable['id']:
		vals = str(val)


	productTable['styles'] = style_list
	productTable['brands'] = brand_style

	#Changing the types of each column to the one required 
	print("----------------Changing the columns of the table to the required types----------------")
	productTable.id = productTable.id.astype(str)
	productTable.price_lbound = productTable.price_lbound.astype(float)
	productTable = productTable.drop(columns = 'price_bucket')
	productTable['npid'] = productTable['brand_and_style']
	productTable = productTable[['npid','id', 'person_id', 'event_id', 'brands', 'styles', 'color','quantity', 'price', 'price_lbound']]

	#Printing statements of the product table after modifications and processing
	#print("The full data is",productTable.price_lbound)
	# print("The description of the data is: \n", productTable.describe(include='O'))
	# print("The data type of the product table columns are: ", productTable.dtypes)
	# print("The SKU, brand and color columns are: ",productTable.styles ) 
	#print("The full data is: \n", productTable)
	# filtered_product = productTable[productTable['id'] == '00005207-acc3-11e6-b9ef-0100ab1800b1']
	# print(fpd['npid'].shape)
	#print(productTable)
	print('******************************************************DONE PRODUCT TABLE**********************************************')
	return(productTable)

def session_preProc():
	session_table = TS.getSession()
	sessions = session_table[['person_id', 'id']]
	#print(sessions.describe())
	#print(sessions)
	return(sessions)
	
def mergingTables():
	'''
	Objective: What I need from this function is to merge the person and the product table to be able to obtain products purchased and their count 
	based on every person_id 
	'''
	print("Entering the merging table")
	# person = person_processing()
	# product = product_processing()
	new_table = pd.merge(person, product, on=('person_id'))
	print(new_table)

def datatrial():
	'''
	Trying to get the item-user matrix to perform martix factorization from the session table
	'''
	# product = product_processing()
	# person = person_processing()
	# session = session_preProc()
	# df = session[['id', 'person_id']]
	# sid = session.person_id
	# npid = product.npid
	# print(npid.nunique())
	

	# le = preprocessing.LabelEncoder()
	# df['person_id_encoded'] = le.fit_transform(df['person_id'])
	# df['npid_encoded'] = le.fit_transform(product['npid'])
	# df['ratings'] = 1

	# data =df['ratings']
	# row = df['person_id_encoded']
	# col = df['id_encoded']
	
	#mat = csr_matrix((data,(row,col)), shape=(257992,(len(df['id_encoded']-1))))
	#print("The dataframe is :", df)

	#org_pid = df[df.id_encoded == 3] 
	#print(df.id)
	#Adding those 2 new pid and uid columns
	#df['pid'] = pid
	#df['uid'] = uid 
	#print("The full dataframe is: ", df)
	#print (pid)
	#print(df.describe(include='O'))
	#mx = pd.DataFrame(sid,pid)

def merges():
	product = product_processing()
	#product = product[:1000]
	sess = TS.getSession()
	ecom = TS.getEcommerce()
	epage = TS.getEpage()
	product['mid'] = product['event_id']
	ecom['mid'] = ecom['id']
	pro_ecom = pd.merge(product, ecom, on=['mid','person_id'], how='inner')
	#pro_ecom['session_id'] = pro_ecom['session_id_x']
	df_merged = pd.merge(pro_ecom, sess, on=['session_id'], how= 'inner')

	df_merged['person_id'] = df_merged['person_id_y']
	df_merged['ratings'] = 0
	event_name = df_merged['event_name']
	ratings = [0]*774977
	for i in range(len(event_name)):
		if event_name[i] == 'Viewed Product':
			ratings[i] = 1
		elif event_name[i] == 'Removed from Cart':
			ratings[i] = 2
		elif event_name[i] == 'Added to Cart':
			ratings[i] = 3
		elif event_name[i] == 'Started Checkout':
			ratings[i] = 4
		elif event_name[i] == 'Completed Order':
			ratings[i] = 5

	df_merged['rating'] = ratings
	df_merged.drop(['total_bucket','view_count','watch_count', 'track_count','person_id_x', 'category_id','discount_bucket', 'revenue_bucket','person_id_y', 'subtotal_bucket','refunds_given','refunds_total', 'id_x', 'mid', 'identify_count', 'revenue_sum', 'revenue_sum_bucket'], axis=1, inplace = True)
	df_orders = df_merged[df_merged['orders_placed'] > 0]
	#print(df_merged.person_id)
	return(df_merged)


def matrix_form():
	#Creating a matrix
	df_mat = merges() 
	df_mat = df_mat[['person_id','npid', 'rating']]
	mat = df_mat.pivot_table(index=['person_id'],columns=['npid'],values='rating')
	mat = mat.fillna(0)
	#mat_b = mat['8f27487c-9e3d-11e7-adc5-0142ac110004',]
	mat_df = pd.DataFrame(mat)
	#mat_uid = mat_df[['8f27487c-9e3d-11e7-adc5-0142ac110004',]]
	#print(mat_df.shape)
	return(mat_df)

def matrix_form1():
	#Creating a matrix
	df_mat = merges() 
	df_mat = df_mat[['person_id','npid', 'rating']]
	df_mat = df_mat[df_mat.person_id == 'ab257738-8eab-11e7-b17d-0100ab18f924']
	#df_mat = df_mat[df_mat.npid == '10130003']
	#mat_df = pd.DataFrame(mat)
	#mat_uid = mat_df[['8f27487c-9e3d-11e7-adc5-0142ac110004',]]
	#print(df_mat)

def PT ():
	pt = merges()
	#df = pt[pt.npid == '10101038']
	pt = pt[['session_id', 'id', 'person_id', 'npid', 'rating', 'event_id','session_duration_interval', 'visit_type', 'brands', 'styles', 'color', 'quantity', 'price',
	   'price_lbound', 'event_name', 'page_title', 'order_id', 'total', 'revenue', 'discount', 'click_count', 'input_count', 
	   'ecommerce_count', 'page_count', 'page_count_interval', 'orders_total', 'orders_placed', 'orders_interval', 'session_duration']]
	print(pt)

PT()