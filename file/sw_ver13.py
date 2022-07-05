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

at_once = []
total_max = []
# change the day
for day in week_num:
	how_long_list=[]
	total1_list =[]
	print("############################################################################################################\n")
	#Per DP, get the client info
	for DF_element in  df_DP['DP'].values.tolist():
		print("@@@@@@@@@@@@@@@@@@@@@@@@		["+ DF_element+"/"+day+"]		@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
		#지역 요일 별 df
		temp_df = df_client.loc[df_client['DP']==DF_element]
		print("@@@@@@@@@@@@@@@@@@@@@@@@			shape :", temp_df.shape)
		print("@@@@@@@@@@@@@@@@@@@@@@@@               small shape :", temp_df.loc[df_client[day]==1].shape)
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
		time_list=[]
		one_truck_list=[]
		Max_row =0
		col_list=[]
		T_time_list = []
		DP_count = []
		car_num =0
		how_many_car_list = []

		while(len(temp_df.loc[temp_df['stopby']==0]) >0):
			print("Point_list : ", Point_list)
			print("@@@@@@@@@@@@@@@@@@@@@@@@               check the stopby :", temp_df.loc[temp_df['stopby']==0].shape)
			#print("---------------------------")
			#print("Time list :", Rest_time_list)
			#print("---------------------------")
			#print("Stock list :", Rest_Stock_list)
			#print("---------------------------")
			#check = check+1
			print("---------------------------------------------------------------------------------------------------------------------------------\n")
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
				print(">>",current_point)

				#장소 리스트 업데이트 
				Point_list.append(current_point)

				#stopby =1 check
				temp_df.loc[(temp_df['code']==current_point),'stopby']=1

				#적재량 리스트 업데이트
				current_point_stock = Max_distance_row[day].tolist()

				Rest_Stock_list.append(Rest_Stock_list[-1]-current_point_stock)

				print(" >> currrent_p : ",current_point,"	current_stock : ",current_point_stock)
				dist = temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]
				fast = temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0]
				time =  (dist / fast)*60
				print("dist :	",dist,"/ fast :	",fast,"/ time :	",time)
							

				
				#남은 시간 리스트  업데이트 
				#소요 시간 = 상차 + [DP-현재 위치]이동시간 + 하차  
				Rest_time_list.append(Rest_time_list[-1] - time - temp_df.loc[temp_df['code']==current_point]['상차시간(분)'].values.tolist()[0] - temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0])
				# 누적 시간 업데이트
				up_time= up_time+time+temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0] +temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0]
				# 누적 거리 업데이트
				
				# 기존 거리 = 기존 거리 + [DP-현재 위치]이동시간
				distance =distance+dist
				#print("거리",temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0],"  / 속도 :",temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])
				#print("소요시간 :",(temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])*60)
				next_check_time = (temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])*60
				# 현재 지점에서 DP까지 시간 여유
				print("누적거리 :   ",distance,"/ 누적시간  :        ",up_time,"/ time :        ",next_check_time , "possible stock :	",Rest_Stock_list[-1])
				print("Point_list : ", Point_list)
				print("From :",Point_list[-2],"To :",Point_list[-1])

			elif current_point !='DP':
				#print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>y!!!!!!DP")
				#거래처 위치 
				temp_df['x곡률값']=temp_df['경도(X좌표)']*3600
				temp_df['y곡률값']=temp_df['위도(Y좌표)']*3600

				#현재 위치 (DP >> 현재 위치 
				temp_df[current_point+'_x곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['경도(X좌표)'].values.tolist()[0]*3600
				temp_df[current_point+'_y곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['위도(Y좌표)'].values.tolist()[0]*3600

				#현재 위치와 거래처간의 계산
				temp_df[current_point+'위도거리']= abs(temp_df['x곡률값'] -temp_df[current_point+'_x곡률값'] )  *0.0245
				temp_df[current_point+'경도거리']= abs(temp_df['y곡률값'] -temp_df[current_point+'_y곡률값'] )  *0.0306
				temp_df[current_point+'운행거리'] = temp_df[current_point+'위도거리']+temp_df[current_point+'경도거리']
				#print(temp_df.head(1))			

				#print(temp_df.head(5))
				temp_df['각도']= np.degrees(np.arctan(temp_df[current_point+'위도거리']/temp_df[current_point+'경도거리']))

				#print("주행거리 : ",lowest_degree_row[current_point+'운행거리'])
				#distance =distance+ lowest_degree_row[current_point+'운행거리']


				#각도가 최적인 것을 선택
				#lowest_degree_row = temp_df[temp_df['stopby']==0].sort_values(by= '각도').iloc[0]
				tdf= temp_df[temp_df['stopby']==0]
				tdf = tdf.copy()

				lowest_degree_row = tdf.sort_values(by=current_point+'운행거리')
				print("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
				#print(lowest_degree_row.head(3))

				lowest_degree_row =  lowest_degree_row.iloc[0]

				dist = lowest_degree_row[current_point+'운행거리'].tolist()
				fast = lowest_degree_row['평균 운행 속도(km/h)'].tolist()
				time =  (dist / fast)*60
				print("dist :   ",dist,"/ fast :        ",fast,"/ time :        ",time)


				#현재f 위치 변경
				current_point = lowest_degree_row['code']
				Point_list.append(current_point)

				# stopby =1 check
				temp_df.loc[(temp_df['code']==current_point),'stopby']=1
				
				#current_point = lowest_degree_row['code']

				#남은시간 리스트 업데이트 
				#  마지막 남은시간- [ 기존거래처 - 갱신 거래처]이동거리 - 하차				
				Rest_time_list.append((Rest_time_list[-1] -time - temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0]))

				# 누적 시간 업데이트 
				get_ff_time = temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0]

				up_time= up_time + time + get_ff_time

				distance =distance+dist

				current_point_stock= lowest_degree_row[day].tolist()
				#남은 적재량 리스트 업데이트 
				Rest_Stock_list.append(Rest_Stock_list[-1]-current_point_stock)
				#print(" ++ currrent_p : ",current_point,"   current_stock : ",current_point_stock)
				print("누적거리 :   ",distance,"/ 누적시간  :        ",up_time, "/ Rest_time :	 ",Rest_time_list[-1],  " / possible stock :,",Rest_Stock_list[-1],"/ possible time :        ",next_check_time)
				print("From :",Point_list[-2],"To :",Point_list[-1])

				next_check_time = (temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])*60


			d_list.append(distance)

			if  Rest_time_list[-1]- next_check_time>0 and Rest_Stock_list[-1] >0  and up_time<360:
				print("조건 1",Rest_time_list[-1]- next_check_time,Rest_Stock_list[-1])
				#print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",up_time)
				continue
			elif Rest_time_list[-1]- next_check_time>0 and Rest_Stock_list[-1] <=0 and up_time <360:
				print("조건 2",Rest_time_list[-1]- next_check_time,Rest_Stock_list[-1])
				# 여유시간이 충분한데 capa 가 없을 때 
				current_point ='DP'
				# 회차 하고 다시 다음 회전을 위한 상차 
				Rest_time_list.append((Rest_time_list[-1]- next_check_time - temp_df.iloc[0]['상차시간(분)']))
				
				up_time = up_time+next_check_time+temp_df.iloc[0]['상차시간(분)']
				Point_list.append(current_point)
				Rest_Stock_list.append(temp_df.iloc[0]['대당 적재능력(box)'])
			   	#distance = distance + temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]
				d_list.append(distance)
				print("					<<<<<<<<<<<<<회차 현황 : >>>>>>>>>>>>>>>>>>\n")
				print("누적거리 :   ",distance,"/ 누적시간  :        ",up_time, "/ Rest_time :   ",Rest_time_list[-1],  " / possible stock :,",Rest_Stock_list[-1],"/ possible time :        ",next_check_time)
				if up_time <360 or Rest_time_list[-1] > 0:
					print("*************************************************************************************************")
					continue
				
				elif up_time > 360 or Rest_time_list[-1] < 0 :
					current_point = 'DP'
					Point_list = ['DP']
					Rest_Stock_list = [temp_df.iloc[0]['대당 적재능력(box)']]
					Rest_time_list = [temp_df.iloc[0]['대당 운행시간(분)']]
					up_time =0
					
					print(" >>>>>>>>>>>>>>>>>>>>>회차 시 누적 시간 이 360 초과 여서 운행 종료 <<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
			elif len(temp_df.loc[temp_df['stopby']==0]) <1:
				print("조건 4 :1개 남음 \n")
				continue


				# 여유시간이 없을 때 
			elif Rest_time_list[-1]- next_check_time <0 or Rest_time_list[-1]>0: 
				print("조건 3",Rest_time_list[-1]- next_check_time,Rest_Stock_list[-1])
				print("Left  0 count :",len(temp_df.loc[temp_df['stopby']==0]),"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
				if up_time < 360 or len(temp_df.loc[temp_df['stopby']==0]) >1:
				# 여유시간이 없을 때 마지막 거래처는 갔다온다

					#거래처 위치
					temp_df['x곡률값']=temp_df['경도(X좌표)']*3600
					temp_df['y곡률값']=temp_df['위도(Y좌표)']*3600

					#현재 위치 (Cient >> 마지막 복귀
					temp_df[current_point+'_x곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['경도(X좌표)'].values.tolist()[0]*3600
					temp_df[current_point+'_y곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['위도(Y좌표)'].values.tolist()[0]*3600

					#현재 위치와 거래처간의 계산
					temp_df[current_point+'위도거리']= abs(temp_df['x곡률값'] -temp_df[current_point+'_x곡률값'] )  *0.0245
					temp_df[current_point+'경도거리']= abs(temp_df['y곡률값'] -temp_df[current_point+'_y곡률값'] )  *0.0306
					temp_df[current_point+'운행거리'] = temp_df[current_point+'위도거리']+temp_df[current_point+'경도거리']

					#print(temp_df.head(5))
					temp_df['각도']= np.degrees(np.arctan(temp_df[current_point+'위도거리']/temp_df[current_point+'경도거리']))

					#print("주행거리 : ",lowest_degree_row[current_point+'운행거리'])
					#distance =distance+ lowest_degree_row[current_point+'운행거리']


					#각도가 최적인 것을 선택
					#lowest_degree_row = temp_df[temp_df['stopby']==0].sort_values(by='각도').iloc[0]

					tdf= temp_df[temp_df['stopby']==0]
					tdf = tdf.copy()
					lowest_degree_row = tdf.sort_values(by=current_point+'운행거리')
					lowest_degree_row =  lowest_degree_row.iloc[0]

	
					dist = lowest_degree_row[current_point+'운행거리'].tolist()
					fast = lowest_degree_row['평균 운행 속도(km/h)'].tolist()
					time =  (dist / fast)*60
					print("dist :   ",dist,"/ fast :        ",fast,"/ time :        ",time)


					#현재 위치 변경
					current_point = lowest_degree_row['code']
					Point_list.append(current_point)
					print("Point list : ", Point_list)
					# stopby =1 check
					temp_df.loc[(temp_df['code']==current_point),'stopby']=1

					#current_point = lowest_degree_row['code']

					#남은시간 리스트 업데이트
					#  마지막 남은시간- [ 기존거래처 - 갱신 거래처]이동거리 - 하차
					Rest_time_list.append(Rest_time_list[-1] -time - temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0])

					# 누적 시간 업데이트
					get_ff_time = temp_df.loc[temp_df['code']==current_point]['하차시간(분)'].values.tolist()[0]

					up_time= up_time + time + get_ff_time
					distance =distance+dist

					current_point_stock= lowest_degree_row[day].tolist()
					#남은 적재량 리스트 업데이트
					Rest_Stock_list.append(Rest_Stock_list[-1]-current_point_stock)
					#print(" ++ currrent_p : ",current_point,"   current_stock : ",current_point_stock)
					print("누적거리 :   ",distance,"/ 누적시간  :        ",up_time, "/ Rest_time :   ",Rest_time_list[-1], "/ possible stock :,",Rest_Stock_list[-1],"  / possible time :        ",next_check_time)
					print("From :",Point_list[-2],"To :",Point_list[-1])

					next_check_time = (temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]/temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0])*60
					distance= distance+temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]
					# 회차  >>DP
					Rest_time_list.append(Rest_time_list[-1]-next_check_time)
					up_time= up_time+ next_check_time
					print("누적거리 :   ",distance,"/ 누적시간  :        ",up_time, "/ Rest_time :   ",Rest_time_list[-1], " / possible stock :,",Rest_Stock_list[-1],">> 복귀 완료 \n")
					print("\n")
					
				else:
					print(" >>>>>>>>>>>>>>>>>>>>>회차 시 누적 시간 이 360 초과 여서 운행 종료 <<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
				

				# 회차만 
				Rest_time_list.append(Rest_time_list[-1]- next_check_time)
				print("VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV",distance)
				truck_list.append(Point_list)
				col_list.append(len(Point_list))
				one_truck_list.append(len(Point_list)-Point_list.count("DP"))
				time_list.append(distance)
				T_time_list.append(up_time)

				current_point = 'DP'
				print("From :",Point_list[-2],"To :", current_point)

				Point_list = ['DP']
				Rest_Stock_list = [temp_df.iloc[0]['대당 적재능력(box)']]
				Rest_time_list = [temp_df.iloc[0]['대당 운행시간(분)']]
				DP_count.append(Point_list.count("DP")-1)
				#one_truck_list.append(len(Point_list))
				distnace = 0
				up_time =0
				car_num= car_num+1
				how_many_car_list.append("No. "+str(car_num))


			if up_time> 360:
				distance =0
				print("VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV",distance)
				#one_truck_list.append(len(Point_list))
				#truck_list.append(Point_list)
				current_point = 'DP'
				Rest_Stock_list = [temp_df.iloc[0]['대당 적재능력(box)']]
				Rest_time_list = [temp_df.iloc[0]['대당 운행시간(분)']]
				up_time =0



		
		Max_row = max(col_list)
		temp_df.to_csv("temp1.csv", index=False, encoding='utf-8-sig')
		result0 = pd.DataFrame(how_many_car_list, columns=["Truck Count"])		
		result0["Region"]= DF_element
		result0["Week"]= day
		result1 = pd.DataFrame(truck_list,columns = ["Client-"+str(i) for i in range(Max_row)])
		result2 = pd.DataFrame(one_truck_list,columns=["Client Count"])
		result3 = pd.DataFrame(time_list,columns=["누적 KM"])
		per_dist =[]
		take = result3["누적 KM"].values.tolist()
		for i in range(0, len(take)):
			
			if i == 0:
				per_dist.append(take[i])
			else:
				per_dist.append((take[i]-take[i-1]))

		result3 = pd.DataFrame(per_dist,columns=["KM"])
		result5 = pd.DataFrame(T_time_list,columns=["Time"])
		result6 = pd.DataFrame(DP_count,columns=["DP를 몇 번"])
		
		result4 = pd.concat([result0,result5,result3,result2,result6, result1],axis=1)
		result4.to_csv("./kk/"+DF_element+day+"check.csv",encoding='utf-8-sig', index=False)
		#print(result)
		for f_row in result4.values.tolist():
			print(f_row)
			how_long_list.append(len(f_row)-6)
			total1_list.append(f_row)
	print("max :", max(how_long_list))
	t_colums_list = ["Truck Count","Region","Week","Time","KM","Client Count"]
	client_list = ["Client-"+str(i) for i in range(max(how_long_list))]

	t_colums_list= t_colums_list + client_list
	print(t_colums_list)
	result_t =  pd.DataFrame(total1_list, columns = t_colums_list)
	result_t.to_csv("./kk/"+"Interal_Region"+day+"analysis.csv",encoding='utf-8-sig', index=False)
	
	total_max.append(max(how_long_list))
	for i in total1_list:
		at_once.append(i)

DR_columns_list = ["Truck Count","Region","Week","Time","KM","Client Count"]
client_list1 = ["Client-"+str(i) for i in range(max(total_max))]
DR_columns_list = DR_columns_list + client_list1 
DR_df = pd.DataFrame(at_once, columns=DR_columns_list)

DR_df.to_csv("./kk/"+"Interal_Region_Week_analysis.csv",encoding='utf-8-sig', index=False)






