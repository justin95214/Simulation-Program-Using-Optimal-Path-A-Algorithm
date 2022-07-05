import numpy as np
import pandas as pd

import io
# df-client info
df_client = pd.read_excel('PR.xlsx')
#print(df_client.head(10))

# DP address
df_DP = pd.read_excel('DP_raw.xlsx')
#print(df_DP.head(5))
df_client = pd.merge(left=df_client, right=df_DP, on="DP", how="right")
df_client = df_client
# DF truck & time info df
df_info = pd.read_excel('sheet_2.xlsx')
# merge the info
info_col=df_info.columns[1:]

#df_client[info_col] = df_info[info_col].iloc[0]
df_client = pd.merge(left=df_client, right=df_info, on="DP", how="right")

week_num =['월','화','수','목','금']


# change the day
for day in week_num:
	print("############################################################################################################\n")
	#Per DP, get the client info
	for DF_element in  df_DP['DP'].values.tolist():
		print("@@@@@@@@@@@@@@@@@@@@@@@@		["+ DF_element+"/"+day+"]		@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
		


		#지역 요일 별 df
		temp_df = df_client.loc[df_client['DP']==DF_element]
		filter_columns = list(set(temp_df.columns.values.tolist())- set(week_num))
		filter_columns.append(day)
		temp_df = temp_df[filter_columns]	
		temp_df['stopby'] = 0
		temp_df =temp_df.dropna(axis=0)
		print("지역 요일 별 df shape : ",temp_df.shape)
		#print(temp_df.head(5))
		#stopby check!!
		temp_df.to_csv("./result/test_"+DF_element+"_"+day+".csv",index=False ,encoding='utf-8-sig')
		# how many truck we need!
		# how many back to DP
		Point_list =['DP']
		Rest_time_list =[]
		Rest_Stock_list=[]
		Rest_Stock_list.append(temp_df.iloc[0]['대당 적재능력(box)'])
		Rest_time_list.append(temp_df.iloc[0]['대당 운행시간(분)'])
		current_point = 'DP'
		truck_list = []
		while(len(temp_df.loc[temp_df['stopby']==0]) >0):
			#print("Point_list : ", Point_list)
			#print("---------------------------")
			#print("Time list :", Rest_time_list)
			#print("---------------------------")
			#print("Stock list :", Rest_Stock_list)
			#print("---------------------------")
			if current_point =='DP':
				temp_df['DP_x곡률값']=temp_df['Longitude']*3600
				temp_df['DP_y곡률값']=temp_df['Latitude']*3600
	
				#client curvature value
				temp_df['x곡률값']=temp_df['경도(X좌표)']*3600
				temp_df['y곡률값']=temp_df['위도(Y좌표)']*3600

				temp_df['DP_위도거리']= abs(temp_df['x곡률값'] -temp_df['DP_x곡률값'] )  *0.0245
				temp_df['DP_경도거리']= abs(temp_df['y곡률값'] -temp_df['DP_y곡률값'] )  *0.0245
				temp_df['DP_운행거리'] = temp_df['DP_위도거리']+temp_df['DP_경도거리']

			
				Max_distance_row = temp_df[temp_df['stopby']==0].sort_values(by='DP_운행거리').iloc[0]
				#현재 위치 변경
				current_point = Max_distance_row['code']
				Point_list.append(current_point)
				#stopby =1 check
				temp_df.loc[(temp_df['code']==current_point),'stopby']=1
				#print(len(temp_df.loc[temp_df['stopby']==1]))
				current_point_stock = temp_df.loc[(temp_df['code']==current_point)][day].values.tolist()[0]
				Rest_Stock_list.append(Rest_Stock_list[-1]-current_point_stock)
				#print(" >> currrent_p : ",current_point,"	current_stock : ",current_point_stock)
				time =  60*(temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])
				Rest_time_list.append(Rest_time_list[-1] - time - temp_df.loc[temp_df['code']==current_point]['상차시간(분)'].values.tolist()[0] - temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0])
				next_check_time = 60*(temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])


			elif current_point !='DP':
				temp_df['x곡률값']=temp_df['경도(X좌표)']*3600
				temp_df['y곡률값']=temp_df['위도(Y좌표)']*3600

				temp_df[current_point+'_x곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['경도(X좌표)'].values.tolist()[0]*3600
				temp_df[current_point+'_y곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['위도(Y좌표)'].values.tolist()[0]*3600

				temp_df['위도거리']= abs(temp_df['x곡률값'] -temp_df[current_point+'_x곡률값'] )  *0.0245
				temp_df['경도거리']= abs(temp_df['y곡률값'] -temp_df[current_point+'_y곡률값'] )  *0.0245
				temp_df['운행거리'] = temp_df['위도거리']+temp_df['경도거리']

				temp_df['각도']= np.degrees(np.arctan(temp_df['위도거리']/temp_df['경도거리']))

				lowest_degree_row = temp_df[temp_df['stopby']==0].sort_values(by='각도').iloc[0]
				#현재 위치 변경
				current_point = lowest_degree_row['code']
				Point_list.append(current_point)

				temp_df.loc[(temp_df['code']==current_point),'stopby']=1
				current_point_stock = temp_df.loc[(temp_df['code']==current_point)][day].values.tolist()[0]
				
				time = 60*(temp_df.loc[temp_df['code']==current_point]['운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])
				Rest_time_list.append(Rest_time_list[-1] -time - temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0])
				

				Rest_Stock_list.append(Rest_Stock_list[-1]-current_point_stock)
				#print(" ++ currrent_p : ",current_point,"   current_stock : ",current_point_stock)

				next_check_time = 60*(temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])
			if  Rest_time_list[-1]- next_check_time>0 and Rest_Stock_list[-1] >0:
				continue
			elif Rest_time_list[-1]- next_check_time>0 and Rest_Stock_list[-1] <=0:
				# 여유시간이 충분한데 capa 가 없을 때 
				current_point ='DP'
				Point_list.append(current_point)
				# 회차 하고 다시 다음 회전을 위한 상차 
				Rest_time_list.append(Rest_time_list[-1]- next_check_time - temp_df.iloc[0]['상차시간(분)'])
				continue
				# 여유시간이 없을 때 
			elif Rest_time_list[-1]- next_check_time <0:
				if (len(temp_df[temp_df['stopby']==0]) > 0):
				# 여유시간이 없을 때 마지막 거래처는 갔다온다
					temp_df['x곡률값']=temp_df['경도(X좌표)']*3600
					temp_df['y곡률값']=temp_df['위도(Y좌표)']*3600

					temp_df[current_point+'_x곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['경도(X좌표)'].values.tolist()[0]*3600
					temp_df[current_point+'_y곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['위도(Y좌표)'].values.tolist()[0]*3600

					temp_df['위도거리']= abs(temp_df['x곡률값'] -temp_df[current_point+'_x곡률값'] )  *0.0245
					temp_df['경도거리']= abs(temp_df['y곡률값'] -temp_df[current_point+'_y곡률값'] )  *0.0245
					temp_df['운행거리'] = temp_df['위도거리']+temp_df['경도거리']

					temp_df['각도']= np.degrees(np.arctan(temp_df['위도거리']/temp_df['경도거리']))
					#print(temp_df[temp_df['stopby']==0])
					lowest_degree_row = temp_df[temp_df['stopby']==0].sort_values(by='각도').iloc[0]
					#현재 위치 변경
					current_point = lowest_degree_row['code']
					Point_list.append(current_point)

					temp_df.loc[(temp_df['code']==current_point),'stopby']=1
					current_point_stock = temp_df.loc[(temp_df['code']==current_point)][day].values.tolist()[0]

					time = 60*(temp_df.loc[temp_df['code']==current_point]['운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])
					Rest_time_list.append(Rest_time_list[-1] -time - temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0])


					Rest_Stock_list.append(Rest_Stock_list[-1]-current_point_stock)
					#print(" ++ currrent_p : ",current_point,"   current_stock : ",current_point_stock)

					next_check_time = 60*(temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])

				# 회차만 
				Rest_time_list.append(Rest_time_list[-1]- next_check_time)

				truck_list.append(Point_list)	
				current_point = 'DP'
				Point_list = ['DP']
				Rest_Stock_list.append(temp_df.iloc[0]['대당 적재능력(box)'])
				Rest_time_list.append(temp_df.iloc[0]['대당 운행시간(분)'])	

		
		result = pd.DataFrame(truck_list)
		total_list =[]
		counting_list = []
		No_truck =1
		for row in result.values.tolist():
			temp = 'DP'
			temp = temp +" >> "
			counting = 0
			client_count = 0
			for i in row[1:]:
				if i != 'DP' and i!='None':
					client_count = client_count+1
					temp = temp +str(i)
					temp = temp + " >> "
				else:
					temp = temp +"DP"
					counting = counting+1
					total_list.append(["No. "+str(No_truck),counting,temp, client_count])
					temp = 'DP >>'
					client_count = 0
			No_truck = No_truck+1 

		DF = pd.DataFrame(total_list,columns =['No_truck','back count','path','client_count'])
		DF.to_csv("./total_result/test_"+DF_element+"_"+day+".csv",index=False ,encoding='utf-8-sig')
		print(DF)
