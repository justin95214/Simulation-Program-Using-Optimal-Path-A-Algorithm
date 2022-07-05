import subprocess
import sys
import warnings
import string_source as ss
import logging

warnings.filterwarnings('ignore')

# 로그 생성
logger = logging.getLogger()

# 로그의 출력 기준 설정
logger.setLevel(logging.INFO)

# log 출력 형식
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# log 출력
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# log를 파일에 출력
file_handler = logging.FileHandler('./logging/my.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# 패키지 설치

try:
	import module_source as ms
	logging.info("module import succeed!!\n")
except:
	libraries = ['numpy','pandas', 'io']
	logging.warning("module import except process\n")

	for i in libraries:
		subprocess.check_call([sys.executables, '-m', 'pip', 'install', i])


	import module_source as ms


# 데이터 로드 
def load_data():
	df_client = ms.pd.read_excel(ss.DF_client)
	df_DP = ms.pd.read_excel(ss.DP)	
	df_info = ms.pd.read_excel(ss.df_info)
	logging.info("load the data completed!!\n")
	return df_client, df_DP, df_info


# 데이터 합병
def merge_data(data_left, data_right, on_columns, how_data):
	df_merge = ms.pd.merge(left=df_client, right=df_DP, on="DP", how="right")
	logging.info("merage data!")
	return df_merge


# 곡률값 계산& 위도 경도 운행 거리 계산
def cal_xy_DP(temp_df):
	
	temp_df['DP_x곡률값']=temp_df['Latitude']*3600
	temp_df['DP_y곡률값']=temp_df['Longitude']*3600

	logging.info("DP 곡률값 계산 완료")
	return temp_df	

def cal_xy_client(temp_df):
	temp_df['x곡률값']=temp_df['경도(X좌표)']*3600
	temp_df['y곡률값']=temp_df['위도(Y좌표)']*3600
	logging.info("client 곡률값 계산완료")
	return temp_df


#현재 위치에서 거래처간 거리 계산
def cal_xy_distance(temp_df,current_point):
	logging.info(f'current_point : {current_point}')
	temp_df[current_point+'_위도거리']= abs(temp_df['x곡률값']-temp_df[current_point+'_y곡률값'])*0.0245
	temp_df[current_point+'_경도거리']= abs(temp_df['y곡률값']-temp_df[current_point+'_x곡률값'])*0.0306
	temp_df[current_point+'_운행거리'] = temp_df[current_point+'_위도거리']+temp_df[current_point+'_경도거리']
	logging.info("cal_xy_distance")	
	return temp_df


#각도 계산
def cal_xy_angle(temp_df):
	temp_df['각도']= ms.np.degrees(ms.np.arctan(temp_df[current_point+'_위도거리']/temp_df[current_point+'_경도거리']))
	return temp_df


def update_location(row, current_point, Point_list, temp_df, current_point_stock,  Rest_time_list, up_time, distance, next_check_time):
	print("update completed!!!\n")
	logging.info("#현재 위치 변경")
	current_point = row['code']
	#print(check,"거리로 선정 Client: ",row['DP_운행거리'],current_point)
	logging.info(f'current _point : >> {current_point}')

	logging.info("#장소 리스트 업데이트")
	Point_list.append(current_point)

	#stopby =1 check
	temp_df.loc[(temp_df['code']==current_point),'stopby']=1

	logging.info("#적재량 리스트 업데이트")
	current_point_stock = row[day].tolist()

	Rest_Stock_list.append(Rest_Stock_list[-1]-current_point_stock)

	logging.info(f" >> currrent_p : {current_point} | current_stock : {current_point_stock}")
	dist = temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]
	fast = temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0]
	time =  (dist / fast)*60
	logging.info(f"dist :   {dist} / fast : {fast} / time : {time}")

	logging.info("#남은 시간 리스트  업데이트")
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
	print("누적거리 :   ",distance,"/ 누적시간  :        ",up_time,"/ time :        ",next_check_time , "possible stock :   ",Rest_Stock_list[-1])
	print("Point_list : ", Point_list)
	print("From :",Point_list[-2],"To :",Point_list[-1])	






# 지역별 요일별 while문을 돌리기 위해서 디폴트 설정
def setting_dataframe(temp_df):
	temp_df['stopby'] = 0
	temp_df =temp_df.dropna(axis=0)
	return temp_df


df_client, df_DP, df_info = load_data()
df_client = merge_data(df_client, df_DP, "DP", "right")

# 데이터 컬럼 가져오기
info_col=df_info.columns[1:]


#df_client[info_col] = df_info[info_col].iloc[0]
df_client = ms.pd.merge(left=df_client, right=df_info, on="DP", how="right")

week_num = ss.week_list

at_once = []
total_max = []

# change the day
for day in week_num:
	how_long_list=[]
	total1_list =[]
	
	#Per DP, get the client info

	#조건DP에서 DP에 해당되는 지역 리스트별로 for문

	for DF_element in  df_DP['DP'].values.tolist():
		logging.info(f'day :{day} / location : {DF_element}')

		#지역 요일 별 df
		temp_df = df_client.loc[df_client['DP']==DF_element].copy()
		logging.info(f"지역 요일별 dataframe 생성완료/ shape : {temp_df.shape} / small shape : {temp_df.loc[df_client[day]==1].shape}")


		#요일을 뺀 나머지 columns
		filter_columns = list(set(temp_df.columns.values.tolist())- set(week_num))

		#해당 요일은 filter_col에서 붙이기
		filter_columns.append(day)
		temp_df = temp_df[filter_columns]	
		
		#얼마나 들렸는지 체크하기 위한 지표 stopby
		#temp_df['stopby'] = 0
		#temp_df =temp_df.dropna(axis=0)
		
		temp_df = setting_dataframe(temp_df)

		logging.info(f"지역 요일 별 df shape : {temp_df.shape}")
		
		#stopby check!!
		temp_df.to_csv("./result/test_"+DF_element+"_"+day+".csv",index=False ,encoding='utf-8-sig')

		# how many truck we need!
		# how many back to DP
		logging.info("varable inited for location week for문")
		Point_list =['DP']
		Rest_time_list =[]
		Rest_Stock_list=[]

		#디폴트 거리 =0
		distance = 0

		print(temp_df.iloc[0]['대당 적재능력(box)'])
		Rest_Stock_list.append(temp_df.iloc[0]['대당 적재능력(box)'])
		Rest_time_list.append(temp_df.iloc[0]['대당 운행시간(분)'])
		
		#디폴트 시작은 DP 에서 출발
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
		


		# 방문하지않은곳이 있을 때까지 while문
		while(len(temp_df.loc[temp_df['stopby']==0]) >0):
			logging.info(f"Point_list : , {Point_list} | Time list :, {Rest_time_list} | Stock list :, {Rest_Stock_list}")
			print(f"@@@@@@@@@@@ check the stopby : {temp_df.loc[temp_df['stopby']==0].shape}")
			#check = check+1
			
			next_check_time = 0
			current_point_stock = []
			logging.info("---------------------------------------------------------------------------\n")
			if current_point =='DP':

				temp_df = cal_xy_DP(temp_df)
				temp_df = cal_xy_client(temp_df)
				temp_df = cal_xy_distance(temp_df,"DP")
				

				logging.info("#최초와 중간 회차 뒤에 경우") 
				Max_distance_row = temp_df[temp_df['stopby']==0].sort_values(by='DP_운행거리', ascending=False).iloc[0]
				update_location(Max_distance_row, current_point, Point_list, temp_df, current_point_stock, Rest_time_list, up_time, distance, next_check_time)
				
				"""
				logging.info("#현재 위치 변경")
				current_point = Max_distance_row['code']
				#print(check,"거리로 선정 Client: ",Max_distance_row['DP_운행거리'],current_point)
				logging.info(f'current _point : >> {current_point}')

				logging.info("#장소 리스트 업데이트") 
				Point_list.append(current_point)

				#stopby =1 check
				temp_df.loc[(temp_df['code']==current_point),'stopby']=1

				logging.info("#적재량 리스트 업데이트")
				current_point_stock = row[day].tolist()

				Rest_Stock_list.append(Rest_Stock_list[-1]-current_point_stock)

				logging.info(f" >> currrent_p : {current_point} | current_stock : {current_point_stock}")
				dist = temp_df.loc[temp_df['code']==current_point]['DP_운행거리'].values.tolist()[0]
				fast = temp_df.loc[temp_df['code']==current_point]['평균 운행 속도(km/h)'].values.tolist()[0]
				time =  (dist / fast)*60
				logging.info(f"dist :	{dist} / fast :	{fast} / time :	{time}")
							
				logging.info("#남은 시간 리스트  업데이트") 
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
				"""

			elif current_point !='DP':
				#print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>y!!!!!!DP")
				#거래처 위치 
				print(current_point)
				#temp_df['x곡률값']=temp_df['경도(X좌표)']*3600
				#temp_df['y곡률값']=temp_df['위도(Y좌표)']*3600
				temp_df = cal_xy_client(temp_df)
				
				#현재 위치 (DP >> 현재 위치 
				temp_df[current_point+'_x곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['경도(X좌표)'].values.tolist()[0]*3600
				temp_df[current_point+'_y곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['위도(Y좌표)'].values.tolist()[0]*3600

				#현재 위치와 거래처간의 계산
				temp_df[current_point+'_위도거리']= abs(temp_df['x곡률값'] -temp_df[current_point+'_x곡률값'] )  *0.0245
				temp_df[current_point+'_경도거리']= abs(temp_df['y곡률값'] -temp_df[current_point+'_y곡률값'] )  *0.0306
				temp_df[current_point+'_운행거리'] = temp_df[current_point+'_위도거리']+temp_df[current_point+'_경도거리']

				#temp_df = cal_xy_distance(temp_df,current_point)


				#print(temp_df.head(5))
				cal_xy_angle(temp_df)
				#temp_df['각도']= ms.np.degrees(ms.np.arctan(temp_df[current_point+'_위도거리']/temp_df[current_point+'_경도거리']))

				#print("주행거리 : ",lowest_degree_row[current_point+'_운행거리'])
				#distance =distance+ lowest_degree_row[current_point+'_운행거리']


				#각도가 최적인 것을 선택
				#lowest_degree_row = temp_df[temp_df['stopby']==0].sort_values(by= '각도').iloc[0]
				tdf= temp_df[temp_df['stopby']==0]
				tdf = tdf.copy()

				lowest_degree_row = tdf.sort_values(by=current_point+'_운행거리')
				print("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
				#print(lowest_degree_row.head(3))

				lowest_degree_row =  lowest_degree_row.iloc[0]

				dist = lowest_degree_row[current_point+'_운행거리'].tolist()
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
				if temp_df.loc[temp_df['stopby']==0].shape[0] ==0:
					truck_list.append(Point_list)
					col_list.append(len(Point_list))
					one_truck_list.append(len(Point_list)-Point_list.count("DP"))
					time_list.append(distance)
					T_time_list.append(up_time)

					DP_count.append(Point_list.count("DP")-1)
					car_num= car_num+1
					how_many_car_list.append("No. "+str(car_num))


			
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
					#truck_list.append(Point_list)
					print("here here here ")
					if len(temp_df.loc[temp_df['stopby']==0]) <1:
						print("eeeeeeeeeeeeeeeweeeeeeeeeeeeeeeeeeeeeeeeewwwwwwwwwwwwwwwwwwe")
						truck_list.append(Point_list) 
						col_list.append(len(Point_list))
						one_truck_list.append(len(Point_list)-Point_list.count("DP"))
						time_list.append(distance)
						T_time_list.append(up_time)
						DP_count.append(Point_list.count("DP")-1)
						car_num= car_num+1
						how_many_car_list.append("No. "+str(car_num))
				
				elif up_time > 360 or Rest_time_list[-1] < 0 :
					
					print(" >>>>>>>>>>>>>>>>>>>>>회차 시 누적 시간 이 360 초과 여서 운행 종료 <<<<<<<<<<<<<<<<<<<<<<<<<<<\n")

					truck_list.append(Point_list)
					col_list.append(len(Point_list))
					one_truck_list.append(len(Point_list)-Point_list.count("DP"))
					time_list.append(distance)
					T_time_list.append(up_time)
					DP_count.append(Point_list.count("DP")-1)
					car_num= car_num+1
					how_many_car_list.append("No. "+str(car_num))

					current_point = 'DP'
					Point_list = ['DP']
					Rest_Stock_list = [temp_df.iloc[0]['대당 적재능력(box)']]
					Rest_time_list = [temp_df.iloc[0]['대당 운행시간(분)']]
					up_time =0


				#elif up_time <360 or Rest_time_list[-1] > 0 and len(temp_df.loc[temp_df['stopby']==0]) < 1:
					#truck_list.append(Point_list)


			elif len(temp_df.loc[temp_df['stopby']==0]) ==1:
				print("조건 4 :1개 남음 \n")
				continue


				# 여유시간이 없을 때 
			elif Rest_time_list[-1]- next_check_time <0 or Rest_time_list[-1]>0: 
				print("조건 3",Rest_time_list[-1]- next_check_time,Rest_Stock_list[-1])
				print("Left  0 count :",len(temp_df.loc[temp_df['stopby']==0]),"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
				if up_time < 360 or len(temp_df.loc[temp_df['stopby']==0]) >1:
				# 여유시간이 없을 때 마지막 거래처는 갔다온다

					#거래처 위치
					#temp_df['x곡률값']=temp_df['경도(X좌표)']*3600
					#temp_df['y곡률값']=temp_df['위도(Y좌표)']*3600
					temp_df = cal_xy_client(temp_df)
					
					#현재 위치 (Cient >> 마지막 복귀
					temp_df[current_point+'_x곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['경도(X좌표)'].values.tolist()[0]*3600
					temp_df[current_point+'_y곡률값']=temp_df.loc[temp_df['code']==Point_list[-1]]['위도(Y좌표)'].values.tolist()[0]*3600

					#현재 위치와 거래처간의 계산
					temp_df[current_point+'_위도거리']= abs(temp_df['x곡률값'] -temp_df[current_point+'_x곡률값'] )  *0.0245
					temp_df[current_point+'_경도거리']= abs(temp_df['y곡률값'] -temp_df[current_point+'_y곡률값'] )  *0.0306
					temp_df[current_point+'_운행거리'] = temp_df[current_point+'_위도거리']+temp_df[current_point+'_경도거리']

					#print(temp_df.head(5))
					temp_df['각도']= ms.np.degrees(ms.np.arctan(temp_df[current_point+'_위도거리']/temp_df[current_point+'_경도거리']))

					#print("주행거리 : ",lowest_degree_row[current_point+'_운행거리'])
					#distance =distance+ lowest_degree_row[current_point+'_운행거리']


					#각도가 최적인 것을 선택
					#lowest_degree_row = temp_df[temp_df['stopby']==0].sort_values(by='각도').iloc[0]

					tdf= temp_df[temp_df['stopby']==0]
					tdf = tdf.copy()
					lowest_degree_row = tdf.sort_values(by=current_point+'_운행거리')
					lowest_degree_row =  lowest_degree_row.iloc[0]

	
					dist = lowest_degree_row[current_point+'_운행거리'].tolist()
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

				Rest_Stock_list = [temp_df.iloc[0]['대당 적재능력(box)']]
				Rest_time_list = [temp_df.iloc[0]['대당 운행시간(분)']]
				DP_count.append(Point_list.count("DP")-1)
				Point_list = ['DP']
				#one_truck_list.append(len(Point_list))
				distnace = 0
				up_time =0
				car_num= car_num+1
				how_many_car_list.append("No. "+str(car_num))


			if up_time> 360:
				distance =0
				print("here i am")
				print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",distance)
				#one_truck_list.append(len(Point_list))
				#truck_list.append(Point_list)
				current_point = 'DP'
				Rest_Stock_list = [temp_df.iloc[0]['대당 적재능력(box)']]
				Rest_time_list = [temp_df.iloc[0]['대당 운행시간(분)']]
				up_time =0



		
		Max_row = max(col_list)
		temp_df.to_csv("temp1.csv", index=False, encoding='utf-8-sig')
		result0 = ms.pd.DataFrame(how_many_car_list, columns=["Truck Count"])		
		result0["Region"]= DF_element
		result0["Week"]= day
		result1 = ms.pd.DataFrame(truck_list,columns = ["Client-"+str(i) for i in range(Max_row)])
		result2 = ms.pd.DataFrame(one_truck_list,columns=["Client Count"])
		result3 = ms.pd.DataFrame(time_list,columns=["누적 KM"])
		per_dist =[]
		take = result3["누적 KM"].values.tolist()
		for i in range(0, len(take)):
			
			if i == 0:
				per_dist.append(take[i])
			else:
				per_dist.append((take[i]-take[i-1]))

		result3 = ms.pd.DataFrame(per_dist,columns=["KM"])
		result5 = ms.pd.DataFrame(T_time_list,columns=["Time"])
		result6 = ms.pd.DataFrame(DP_count,columns=["DP를 몇 번"])
		
		result4 = ms.pd.concat([result0,result5,result3,result2,result6, result1],axis=1)
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
	result_t =  ms.pd.DataFrame(total1_list, columns = t_colums_list)
	result_t.to_csv("./kk/"+"Interal_Region"+day+"analysis.csv",encoding='utf-8-sig', index=False)
	
	total_max.append(max(how_long_list))
	for i in total1_list:
		at_once.append(i)

DR_columns_list = ["Truck Count","Region","Week","Time","KM","Client Count"]
client_list1 = ["Position-"+str(i) for i in range(max(total_max))]
DR_columns_list = DR_columns_list + client_list1 
DR_df = ms.pd.DataFrame(at_once, columns=DR_columns_list)

DR_df.to_csv("./kk/"+"Interal_Region_Week_analysis.csv",encoding='utf-8-sig', index=False)






