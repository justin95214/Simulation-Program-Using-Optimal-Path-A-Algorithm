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
print(info_col)
#df_client[info_col] = df_info[info_col].iloc[0]
df_client = pd.merge(left=df_client, right=df_info, on="DP", how="right")

week_num =['월','화','수','목','금']


# change the day
for day in week_num:
	print("############################################################################################################\n")
	#Per DP, get the client info
	for DF_element in  df_DP['DP'].values.tolist():
		print("@@@@@@@@@@@@@@@@@@@@@@@@		["+ DF_element+"/"+day+"]		@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")

		#Rest_time_list
		Rest_time_list =[]
		Current_point = ['DP']
		Point_list =['DP']
		Rest_Stock_list =[]
		count_time =0

		temp_df = df_client.loc[df_client['DP']==DF_element]
		#order =null removed
		#stock ability check
		Rest_time_list.append(temp_df.iloc[0]['대당 운행시간(분)'])
		Rest_Stock_list.append(temp_df.iloc[0]['대당 적재능력(box)'])
		#print(temp_df)
		temp_df = temp_df.copy()
		#client curvature value
		temp_df['x곡률값']=temp_df['경도(X좌표)']*3600
		temp_df['y곡률값']=temp_df['위도(Y좌표)']*3600
		#DF curvature value
		temp_df['DP_x곡률값']=temp_df['Longitude']*3600
		temp_df['DP_y곡률값']=temp_df['Latitude']*3600

		#fileter_columns
		filter_columns = list(set(temp_df.columns.values.tolist())- set(week_num))
		#filter_columns =['DP', 'code', 'citydistrict', 'x곡률값', 'y곡률값','DT_x곡률값','DT_y곡률값','대당 적재능력(box)',  '대당 운행시간(분)',  '평균 운행 속도(km/h)', '상차시간(분)', '하차시간(분)']
		#update the daily weekend
		filter_columns.append(day)
		#filter_columns.append(info_col)
		#filter
		temp_df = temp_df[filter_columns]
		temp_df =temp_df.dropna(axis=0)
		#get latitude & longitude values
		temp_df['위도거리']= abs(temp_df['x곡률값'] -temp_df['DP_x곡률값'] )  *0.0245
		temp_df['경도거리']= abs(temp_df['y곡률값'] -temp_df['DP_y곡률값'] )  *0.0245
		temp_df['운행거리'] = temp_df['위도거리']+temp_df['경도거리']

		#get the degree
		temp_df['각도']= np.degrees(np.arctan(temp_df['위도거리']/temp_df['경도거리']))
		#print(temp_df.head(10))

		#apply far sweep algorithm
		# check the stopby the client
		temp_df['stopby'] = 0
		#print(temp_df.head(10))
		## 1. Get the Max Client Row Distance From DP to Client 
		### Max distance 
		Max_distance = np.max(temp_df['운행거리'].values)
		## consider just the duplicated Max Value
		Max_client = temp_df.loc[temp_df['운행거리']==Max_distance].iloc[0]
		#df.loc[(df['CAL_AGE']>=300),'CAL_AGE']=90
		# if the stopby the client, stopby is 1 
		temp_df.loc[(temp_df['운행거리']==Max_distance),'stopby']=1
		#print(temp_df.loc[temp_df['운행거리']==Max_distance].iloc[0])
		#change the current location
		current_point = Max_client['code']
		#Point order list
		Point_list.append(Max_client['code'])
		#Rest_time list
		drive_time =60*(Max_client['운행거리']/Max_client['평균 운행 속도(km/h)'])
		print("drive time: ",drive_time)

		time = Rest_time_list[0]-drive_time-Max_client['상차시간(분)']-Max_client['하차시간(분)']
                #print(60*(Max_client['운행거리']/Max_client['평균 운행 속도(km/h)']),Max_client['상차시간(분)'],Max_client['하차시간(분)'])
		Rest_time_list.append(time)
		Rest_Stock_list.append(Rest_Stock_list[0]-Max_client[day])

		print(">>",current_point,Point_list, drive_time ,time,Rest_time_list)
		#check the next drive
	
		while(True):
			again_temp_df = temp_df[temp_df['stopby']==0]
			print("label : ",temp_df[temp_df['stopby']==1]['code'].values.tolist())
			#print(temp_df[temp_df['stopby']==1])
			# time from current point to DP > 0 and Rest Stock ability > 0
			#print(time - drive_time,Rest_Stock_list[-1])
			if time - drive_time > 0 and  Rest_Stock_list[-1]>0 :

				#again_temp_df = temp_df[temp_df['stopby']==0]
				#print("label : ",temp_df[temp_df['stopby']==1]['code'].values.tolist())
				#client curvature value
				again_temp_df = again_temp_df.copy()
				again_temp_df['x곡률값']=again_temp_df['경도(X좌표)']*3600
				again_temp_df['y곡률값']=again_temp_df['위도(Y좌표)']*3600
				#current curvature value
				#print(temp_df.loc[temp_df['code']==Point_list[-1]],Point_list[-1])
				again_temp_df[current_point+'_x곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['경도(X좌표)'].values.tolist()[0]*3600
				again_temp_df[current_point+'_y곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['위도(Y좌표)'].values.tolist()[0]*3600
				#print(temp_df.loc[temp_df['code']==Point_list[-1]]['경도(X좌표)'].values,temp_df.loc[temp_df['code']==Point_list[-1]]['위도(Y좌표)'].values)
				again_temp_df['위도거리']= abs(again_temp_df['x곡률값'] -again_temp_df[current_point+'_x곡률값'] )  *0.0245
				again_temp_df['경도거리']= abs(again_temp_df['y곡률값'] -again_temp_df[current_point+'_y곡률값'] )  *0.0245
				again_temp_df['운행거리'] = again_temp_df['위도거리']+again_temp_df['경도거리']
				again_temp_df['각도']= np.degrees(np.arctan(again_temp_df['위도거리']/again_temp_df['경도거리']))
				#sorted the degree
				again_temp_df =  again_temp_df.sort_values(by='각도')
				#lowest degree
				lowest_degree_row = again_temp_df.iloc[0]
				#print(lowest_degree_row)
				#print(lowest_degree_row['각도'])
				temp_df.loc[(temp_df['code']==lowest_degree_row['code']),'stopby']=1
				#print("????? ",temp_df.loc[(temp_df['각도']==lowest_degree_row['각도'])])
				#change the current location
				current_point = lowest_degree_row['code']
				#Point order list
				Point_list.append(lowest_degree_row['code'])
				#Rest_time list
				drive_time =60*(lowest_degree_row['운행거리']/lowest_degree_row['평균 운행 속도(km/h)'])
				#print("drive time: ",drive_time,lowest_degree_row['운행거리'],lowest_degree_row['평균 운행 속도(km/h)'])
	
				time = Rest_time_list[-1] - drive_time - lowest_degree_row['하차시간(분)']
	
				Rest_time_list.append(time)
				Rest_Stock_list.append(Rest_Stock_list[-1]-lowest_degree_row[day])
	
				print(">>",current_point,Point_list, drive_time ,time,Rest_time_list)

			elif time - drive_time > 0 and  Rest_Stock_list[-1] <= 0:

				#again_temp_df = temp_df[temp_df['stopby'] == 0]
				Rest_Stock_list.append(temp_df.iloc[0]['대당 적재능력(box)'])
				Point_list.append('DP')
				
				# client curvature value
				again_temp_df = again_temp_df.copy()
				again_temp_df['x곡률값'] = again_temp_df['경도(X좌표)'] * 3600
				again_temp_df['y곡률값'] = again_temp_df['위도(Y좌표)'] * 3600
				# current curvature value
				if Point_list[-1] == 'DP':
					again_temp_df[current_point+'_x곡률값']=again_temp_df['Longitude']*3600
					again_temp_df[current_point+'_y곡률값']=again_temp_df['Latitude']*3600

				else:
					again_temp_df[current_point+'_x곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['경도(X좌표)'].values.tolist()[0]*3600
					again_temp_df[current_point+'_y곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['위도(Y좌표)'].values.tolist()[0]*3600

				again_temp_df['위도거리'] = abs(again_temp_df['x곡률값'] - again_temp_df[current_point+'_x곡률값']) * 0.0245
				again_temp_df['경도거리'] = abs(again_temp_df['y곡률값'] - again_temp_df[current_point+'_y곡률값']) * 0.0245
				again_temp_df['운행거리'] = again_temp_df['위도거리'] + again_temp_df['경도거리']
				again_temp_df['각도'] = np.degrees(np.arctan(again_temp_df['위도거리'] / again_temp_df['경도거리']))
				# sorted the degree 
				again_temp_df = again_temp_df.sort_values(by='각도')
				# lowest degree
				lowest_degree_row = again_temp_df.iloc[0]
				temp_df.loc[(temp_df['code'] == lowest_degree_row['code']), 'stopby'] = 1
				# change the current location
				current_point = lowest_degree_row['code']
				# Point order list
				Point_list.append(lowest_degree_row['code'])
				# Rest_time list
				drive_time = 60 * (lowest_degree_row['운행거리'] / lowest_degree_row['평균 운행 속도(km/h)'])
				#print("drive time: ", drive_time)
				time = Rest_time_list[-1] - drive_time - lowest_degree_row['하차시간(분)']
				Rest_time_list.append(time)
				Rest_Stock_list.append(Rest_Stock_list[-1]-lowest_degree_row[day])
				print("++",current_point,Point_list, drive_time ,time,Rest_time_list)

			elif time - drive_time < 0 :
				
				# client curvature value
				again_temp_df = again_temp_df.copy()
				again_temp_df['x곡률값'] = again_temp_df['경도(X좌표)'] * 3600
				again_temp_df['y곡률값'] = again_temp_df['위도(Y좌표)'] * 3600
				# current curvature value
				again_temp_df[current_point+'_x곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['경도(X좌표)'].values.tolist()[0]*3600
				again_temp_df[current_point+'_y곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['위도(Y좌표)'].values.tolist()[0]*3600
				again_temp_df['위도거리'] = abs(again_temp_df['x곡률값'] - again_temp_df[current_point+'_x곡률값']) * 0.0245
				again_temp_df['경도거리'] = abs(again_temp_df['y곡률값'] - again_temp_df[current_point+'_y곡률값']) * 0.0245
				again_temp_df['운행거리'] = again_temp_df['위도거리'] + again_temp_df['경도거리']
				again_temp_df['각도'] = np.degrees(np.arctan(again_temp_df['위도거리'] / again_temp_df['경도거리']))
				# sorted the degree 
				again_temp_df = again_temp_df.sort_values(by='각도')
				# lowest degree
				lowest_degree_row = again_temp_df.iloc[0]
				temp_df.loc[(temp_df['code'] == lowest_degree_row['code']), 'stopby'] = 1
				# change the current location
				current_point = lowest_degree_row['code']
				# Point order list
				Point_list.append(lowest_degree_row['code'])
				print("<<",current_point,Point_list, drive_time ,time,Rest_time_list)
				break;
			again_temp_df.to_csv("./result/test1_"+ DF_element+"_"+day+".csv", index =False, encoding='utf-8-sig')


