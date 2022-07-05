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
df_info = pd.read_excel('sheet_4.xlsx')
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
		distance = 0
		print(temp_df.iloc[0]['대당 적재능력(box)'])
		Rest_Stock_list.append(temp_df.iloc[0]['대당 적재능력(box)'])
		Rest_time_list.append(temp_df.iloc[0]['대당 운행시간(분)'])
		current_point = 'DP'
		truck_list = []
		check = 0
		up_time = 0
		d_list=[]
		while(len(temp_df.loc[temp_df['stopby']==0]) >0):
			print("Point_list : ", Point_list)
			#print("---------------------------")
			#print("Time list :", Rest_time_list)
			#print("---------------------------")
			#print("Stock list :", Rest_Stock_list)
			#print("---------------------------")
			#check = check+1
			if current_point =='DP':
				temp_df['DP_x곡률값']=temp_df['Latitude']*3600
				temp_df['DP_y곡률값']=temp_df['Longitude']*3600
	
				#client curvature value
				temp_df['x곡률값']=temp_df['경도(X좌표)']*3600
				temp_df['y곡률값']=temp_df['위도(Y좌표)']*3600

				temp_df['DP_위도거리']= abs(temp_df['x곡률값']-temp_df['DP_y곡률값'])*0.0245
				temp_df['DP_경도거리']= abs(temp_df['y곡률값']-temp_df['DP_x곡률값'])*0.0306
				temp_df['DP_운행거리'] = temp_df['DP_위도거리']+temp_df['DP_경도거리']

				#최초와 중간 회차 뒤에 경우 
				Max_distance_row = temp_df[temp_df['stopby']==0].sort_values(by='DP_운행거리', ascending=False).iloc[0]
				#print(">>운행거리 :", Max_distance_row['DP_운행거리'])


				

				#현재 위치 변경
				current_point = Max_distance_row['code']
				#print(check,"거리로 선정 Client: ",Max_distance_row['DP_운행거리'],current_point)
				#print(">>",current_point)
				#장소 리스트 업데이트 
				Point_list.append(current_point)
				#stopby =1 check
				temp_df.loc[(temp_df['code']==current_point),'stopby']=1
				#print(len(temp_df.loc[temp_df['stopby']==1]))
				current_point_stock = temp_df.loc[(temp_df['code']==current_point)][day].values.tolist()[0]
				Rest_Stock_list.append(Rest_Stock_list[-1]-current_point_stock)
				print(" >> currrent_p : ",current_point,"	current_stock : ",current_point_stock)
				time =  (temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])*60
				print("ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd:",time)
				Rest_time_list.append(Rest_time_list[-1] - time - temp_df.loc[temp_df['code']==current_point]['상차시간(분)'].values.tolist()[0] - temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0])
				up_time= up_time+time+temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0] +temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0]
				distance =distance+temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]
				#print("거리",temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0],"  / 속도 :",temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])
				#print("소요시간 :",(temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])*60)
				next_check_time = (temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])*60
				print("From :",Point_list[-2],"To :",Point_list[-1])


			elif current_point !='DP':
				#print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>y!!!!!!DP")
				#거래처 위치 
				temp_df['x곡률값']=temp_df['경도(X좌표)']*3600
				temp_df['y곡률값']=temp_df['위도(Y좌표)']*3600

				#현재 위치 (DP >> 현재 위치 
				temp_df[current_point+'_x곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['경도(X좌표)'].values.tolist()[0]*3600
				temp_df[current_point+'_y곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['위도(Y좌표)'].values.tolist()[0]*3600

				temp_df[current_point+'위도거리']= abs(temp_df['x곡률값'] -temp_df[current_point+'_x곡률값'] )  *0.0245
				temp_df[current_point+'경도거리']= abs(temp_df['y곡률값'] -temp_df[current_point+'_y곡률값'] )  *0.0306
				temp_df[current_point+'운행거리'] = temp_df[current_point+'위도거리']+temp_df[current_point+'경도거리']
				print(temp_df.head(1))			

				#print(temp_df.head(5))
				temp_df['각도']= np.degrees(np.arctan(temp_df[current_point+'위도거리']/temp_df[current_point+'경도거리']))

				print("주행거리 : ",lowest_degree_row[current_point+'운행거리'])
				distance =distance+ lowest_degree_row[current_point+'운행거리']

				#각도가 최적인 것을 선택
				lowest_degree_row = temp_df[temp_df['stopby']==0].sort_values(by='각도').iloc[0]
				#현재 위치 변경
				current_point = lowest_degree_row['code']

				#print("주행시간 : ",temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])
				temp_df.loc[(temp_df['code']==current_point),'stopby']=1
				current_point_stock = temp_df.loc[(temp_df['code']==current_point)][day].values.tolist()[0]
				#current_point = lowest_degree_row['code']

				time = (lowest_degree_row[current_point+'운행거리']/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])*60
				#print("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",time)
				Rest_time_list.append(Rest_time_list[-1] -time - temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0])

				#current_point = lowest_degree_row['code']
				#####Point_list.append(current_point)
				#print("운행거리 :", lowest_degree_row['DP_운행거리'])
				#current_point = lowest_degree_row['code']
				up_time= up_time+time+temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0]
				Rest_Stock_list.append(Rest_Stock_list[-1]-current_point_stock)
				#print(" ++ currrent_p : ",current_point,"   current_stock : ",current_point_stock)
				print("From :",Point_list[-2],"To :",Point_list[-1])


				next_check_time = (temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])*60
			if  Rest_time_list[-1]- next_check_time>0 and Rest_Stock_list[-1] >0 :
				print("조건 1",Rest_time_list[-1]- next_check_time,Rest_Stock_list[-1])
				#print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",up_time)
				d_list.append(distance)
				continue
			elif Rest_time_list[-1]- next_check_time>0 and Rest_Stock_list[-1] <=0:
				print("조건 2",Rest_time_list[-1]- next_check_time,Rest_Stock_list[-1])
				# 여유시간이 충분한데 capa 가 없을 때 
				current_point ='DP'
				Point_list.append(current_point)
				# 회차 하고 다시 다음 회전을 위한 상차 
				Rest_time_list.append(Rest_time_list[-1]- next_check_time - temp_df.iloc[0]['상차시간(분)'])
				continue
				up_time = up_time+next_check_time+temp_df.iloc[0]['상차시간(분)']


				#distance = distance + temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]
				d_list.append(distance)
				# 여유시간이 없을 때 
			elif Rest_time_list[-1]- next_check_time <0 or Rest_time_list[-1]>0: 
				print("조건 3",Rest_time_list[-1]- next_check_time,Rest_Stock_list[-1])
				if (len(temp_df[temp_df['stopby']==0]) > 0):
				# 여유시간이 없을 때 마지막 거래처는 갔다온다
					temp_df['x곡률값']=temp_df['경도(X좌표)']*3600
					temp_df['y곡률값']=temp_df['위도(Y좌표)']*3600

					temp_df[current_point+'_x곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['경도(X좌표)'].values.tolist()[0]*3600
					temp_df[current_point+'_y곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['위도(Y좌표)'].values.tolist()[0]*3600

					temp_df[current_point+'위도거리']= abs(temp_df['x곡률값'] -temp_df[current_point+'_x곡률값'] )  *0.0245
					temp_df[current_point+'경도거리']= abs(temp_df['y곡률값'] -temp_df[current_point+'_y곡률값'] )  *0.0306
					temp_df[current_point+'운행거리'] = temp_df[current_point+'위도거리']+temp_df[current_point+'경도거리']

					temp_df['각도']= np.degrees(np.arctan(temp_df[current_point+'위도거리']/temp_df[current_point+'경도거리']))
					#print(temp_df[temp_df['stopby']==0])
					lowest_degree_row = temp_df[temp_df['stopby']==0].sort_values(by='각도').iloc[0]
					#현재 위치 변경

					temp_df.loc[(temp_df['code']==current_point),'stopby']=1
					current_point_stock = temp_df.loc[(temp_df['code']==current_point)][day].values.tolist()[0]

					time = (temp_df.loc[temp_df['code']==current_point][current_point+'운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])*60
					Rest_time_list.append(Rest_time_list[-1] -time - temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0])


					current_point = lowest_degree_row['code']
					Point_list.append(current_point)

					Rest_Stock_list.append(Rest_Stock_list[-1]-current_point_stock)
					#print(" ++ currrent_p : ",current_point,"   current_stock : ",current_point_stock)

					next_check_time = (temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])*60
					up_time= up_time+time +time+temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0]
					print("From :",Point_list[-2],"To :",Point_list[-1])

				# 회차만 
				Rest_time_list.append(Rest_time_list[-1]- next_check_time)
				print("VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV",distance)
				d_list.append(distance)
				distance =0
				truck_list.append(Point_list)	
				current_point = 'DP'
				Point_list = ['DP']
				Rest_Stock_list = [temp_df.iloc[0]['대당 적재능력(box)']]
				Rest_time_list = [temp_df.iloc[0]['대당 운행시간(분)']]
				up_time =0

		temp_df.to_csv("temp1.csv", index=False, encoding='utf-8-sig')		
		result = pd.DataFrame(truck_list)
		#print(result)
		#result.to_csv(DF_element+day+"check.csv",encoding='utf-8-sig')
		#print(result)
		total_list =[]
		counting_list = []
		No_truck =1
		path_sum =[]
		
		for k,row in enumerate(result.values.tolist()):
			temp = 'DP'
			temp = temp +" >> "
			counting = 0
			client_count = 0
			path_value =0
			#print(row[1:])
			L = filter(None, row[1:])
			for idx, i in enumerate(L):

				if i != 'DP':
					#print("afjdlksjfkldsjlfkjasdlkjflkdjslfjdskjfjsdakfjlsdkf")
					client_count = client_count+1
					temp = temp +str(i)
					temp = temp + " >> "

				else:
					temp = temp +"DP"
					counting = counting+1
					#print(No_truck)
					#total_list.append(["No. "+str(No_truck),counting,temp, client_count])
					temp = 'DP >>'
					client_count = 0
				#total_list.append(["No. "+str(No_truck),counting,temp, client_count,path_value])
				if idx ==0:

					before = i
					print("누적거리 :ddddddddddddddddddddddsssssssssssssssssssssss 1: ",path_value)
					path_value =path_value+float(temp_df.loc[temp_df['code']==i]['DP_운행거리'].values.tolist()[0])
					#print("before :",before)
				elif idx ==1 or before =='DP':
					#print("before/ i :",before,i)

					path_value =path_value+float(temp_df.loc[temp_df['code']==i][before+'운행거리'].values.tolist()[0])
					before = i
					#print("누적거리 :ddddddddddddddddddddddsssssssssssssssssssssss 2: ",path_value)
					#print(path_value)
				elif idx >1 and i!='DP':
					#print("before/ i :",before,i)
					path_value =path_value+float(temp_df.loc[temp_df['code']==i][before+'운행거리'].values.tolist()[0])
					before = i
					#print("누적거리 :ddddddddddddddddddddddsssssssssssssssssssssss 3: ",path_value)
					#print(path_value)
				elif i =='DP':
					path_value =path_value+float(temp_df.loc[temp_df['code']==before]['DP_운행거리'].values.tolist()[0])
					before = i
					#print(path_value)
					#print("누적거리 :ddddddddddddddddddddddsssssssssssssssssssssss 4: ",path_value)
				elif i =='None'or i =='' or i == None:
					break;

				total_list.append(["No. "+str(No_truck),counting,temp, client_count,d_list[k]])
			#path_sum.append(path_value)
			#total_list.append(["No. "+str(No_truck),counting,temp, client_count,path_value])
			No_truck = No_truck+1 
		#print(total_list)	
		DF = pd.DataFrame(total_list,columns =['No_truck','back count','path','client_count','sum_path'])
		final_list =[]
		for truck in DF['No_truck'].unique().tolist():
			final_list.append(DF[DF['No_truck']==truck].sort_values(by='sum_path'))

		DF.to_csv("./total_R/atest_"+DF_element+"_"+day+".csv",index=False ,encoding='utf-8-sig')
		#final = pd.DataFrame(final_list, columns=['No_truck','back count','path','client_count','sum_path'], index =False)	
		#DF.to_csv("./total_R/test_"+DF_element+"_"+day+".csv",index=False ,encoding='utf-8-sig')
		#print(DF)
