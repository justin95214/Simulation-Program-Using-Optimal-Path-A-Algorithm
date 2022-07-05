import math
import inspect
import subprocess
import sys
import warnings
import string_source as ss
import logging
import log_package as lp
warnings.filterwarnings('ignore')
import tt as tt
import string_source as ss

#log 설정
logger = lp.log_setting(logging)


# 패키지 설치
try:
	import module_source as ms
	logger.info("module import succeed!!\n")
except:
	libraries = ['numpy','pandas', 'io']
	logger.warning("module import except process\n")

	for i in libraries:
		subprocess.check_call([sys.executables, '-m', 'pip', 'install', i])


	import module_source as ms


class sch_sys():
	
	def __init__(self, client, dp, df_info):
		
		try:
			self.client = client
			self.dp = dp
			self.df_info = df_info
			tt.autolog()
		except:
			tt.error_autolog()

	#데이터 로드
	def load_data(self):
		df_client = ms.pd.read_excel(self.client)
		df_DP = ms.pd.read_excel(self.dp)
		df_info = ms.pd.read_excel(self.df_info)
		tt.autolog()
		return df_client, df_DP, df_info

	#데이터프레임 병합	
	def merge_data(self, client, dp, how_way):
		df_merge = ms.pd.merge(left=client, right=dp, on="DP", how= how_way)
		tt.autolog()
		return df_merge

	#DP x,y 곡률값계산
	def cal_xy_DP(self, temp_df):
		temp_df['DP_y곡률값']=temp_df['Latitude']*3600
		temp_df['DP_x곡률값']=temp_df['Longitude']*3600
		tt.autolog()
		return temp_df

	#client x,y 곡률값
	def cal_xy_client(self, temp_df):
		
		temp_df['x곡률값']=temp_df['경도(X좌표)']*3600
		temp_df['y곡률값']=temp_df['위도(Y좌표)']*3600
		tt.autolog()	
		return temp_df


	def cal_xy_from(self, temp_df, current_point):
		temp_df[current_point+'_y곡률값']=temp_df[temp_df['code']== current_point]['y곡률값'].values.tolist()[0]
		temp_df[current_point+'_x곡률값']=temp_df[temp_df['code']== current_point]['x곡률값'].values.tolist()[0]
		tt.autolog()
		return temp_df


	#현재 위치에서 거래처간 거리 계산
	def cal_xy_distance(self, temp_df, current_point):
		temp_df[current_point+'_경도거리']= abs(temp_df['x곡률값']-temp_df[current_point+'_x곡률값'])*0.0245
		temp_df[current_point+'_위도거리']= abs(temp_df['y곡률값']-temp_df[current_point+'_y곡률값'])*0.0306
		temp_df[current_point+'_운행거리'] = temp_df[current_point+'_위도거리']+temp_df[current_point+'_경도거리']
		temp_df[current_point+'_위도거리'] = temp_df[current_point+'_위도거리'].astype(float)
		temp_df[current_point+'_경도거리'] = temp_df[current_point+'_경도거리'].astype(float)
                
		tt.autolog()
		return temp_df

	#데이터프레임 초기화
	def dateframe_init(self, df_client):
		temp_df = df_client.loc[df_client['DP']==DF_element].copy()

		#요일을 뺀 나머지 columns
		filter_columns = list(set(temp_df.columns.values.tolist())- set(week_num))

		#해당 요일은 filter_col에서 붙이기
		filter_columns.append(day)
		temp_df = temp_df[filter_columns]

		#얼마나 들렸는지 체크하기 위한 지표 stopby
		temp_df['stopby'] = 0
		#temp_df =temp_df.dropna(axis=0)
		tt.autolog()
		return temp_df

	#데이터프레임 방위각 계산
	def cal_azimuth(self, temp_df, current_point):

		CPI = math.pi
		CstLatVal = 0.0245
		CstLonVal = 0.0306
		CStVal = 3600
		temp_df =temp_df.dropna(axis=0)
		temp_df['rad'] = ms.np.arctan2(temp_df[current_point+'_위도거리'],temp_df[current_point+'_경도거리']) 
		temp_df['deg'] = temp_df['rad'] * 180 / CPI
		tt.autolog()
		return temp_df

# route의 첫번째돌때
def first_time(temp_df, current_point):
	temp_df = sch.cal_xy_DP(temp_df)
	temp_df = sch.cal_xy_client(temp_df)
	temp_df = sch.cal_xy_distance(temp_df, current_point)
	temp_df = sch.cal_azimuth(temp_df, current_point)
	temp_df = temp_df[temp_df['stopby']==0].sort_values(by=[current_point+'_운행거리'], ascending=[True])
	tt.autolog()
	return temp_df


def after_first_time(temp_df, day, current_point, trace_list ,before_distance, kappa, drive_time, average_speed, get_up, get_off):
	
	while (drive_time > 0 and kappa > 0):
		temp_df = sch.cal_xy_from(temp_df, current_point)
		temp_df = sch.cal_xy_distance(temp_df, current_point)
		temp_df = sch.cal_azimuth(temp_df, current_point)
		temp_df = temp_df[temp_df['stopby']==0].sort_values(by=[current_point+'_운행거리','deg'], ascending=[False, False])
		
		next_point = temp_df.iloc[0]['code']

		trace_list , drive_time, kappa, before_distance = situation_update(temp_df, day, next_point, current_point, trace_list ,before_distance, kappa, drive_time, average_speed, get_up, get_off)

		temp_df.loc[(temp_df['code']==next_point),'stopby']=1
		#temp_df = temp_df[temp_df['stopby']==0].copy()
		
		current_point = next_point
		logger.info(f'>> start : {trace_list[-2]} |  arrive : {trace_list[-1]} | left time : {drive_time}  | left kappa : {kappa}')
	

	tt.autolog()

	return temp_df, trace_list, before_distance

def update_init_element(df_info, df_element):
	kappa = df_info[df_info['DP']==df_element]['대당 적재능력(box)'].values.tolist()[0]
	drive_time = df_info[df_info['DP']==df_element]['대당 운행시간(분)'].values.tolist()[0]
	average_speed = df_info[df_info['DP']==df_element]['평균 운행 속도(km/h)'].values.tolist()[0]
	get_up = df_info[df_info['DP']==df_element]['상차시간(분)'].values.tolist()[0]
	get_off = df_info[df_info['DP']==df_element]['하차시간(분)'].values.tolist()[0]
	tt.autolog()
	return kappa, drive_time, average_speed, get_up, get_off


def situation_update(temp_df, day, to_point, current_point, trace_list ,before_distance, kappa, drive_time, average_speed, get_up, get_off):

	
	location = to_point
	trace_list.append(location)
	left_drive_time = drive_time
	left_kappa = kappa

	this_distance = temp_df[temp_df['code']==to_point][current_point+'_운행거리'].values.tolist()[0]
	total_distance = before_distance + this_distance
	this_time = this_distance / average_speed
	left_drive_time = left_drive_time - this_time*60 - get_off
	left_kappa = kappa - temp_df[temp_df['code']==to_point][day].values.tolist()[0]


	return trace_list, left_drive_time, int(left_kappa), total_distance
		


# route 전체
def route(temp_df, df_element,day, current_point):
		
	#DP별 조건
	kappa, drive_time, average_speed, get_up, get_off = update_init_element(df_info, df_element)	


	trace_list =['DP']
	total_distance = 0
	left_drive_time = drive_time
	left_kappa = kappa
	#처음 상황 
	

	logger.info(f'>> start : {trace_list[-1]} |  arrive : {trace_list[-1]} | left time : { left_drive_time}  | left kappa : { left_kappa}')
	
	while (len(temp_df.loc[temp_df['stopby']==0]) >0):
		#첫타임 긴거리 정렬 ascending
		temp_df = first_time(temp_df[temp_df['stopby']==0], current_point)


		#간 곳은 체크 stopby = 1, and stopby =0 인 temp_df 다시 구조화
		Max_distance_row_point = temp_df[temp_df['stopby']==0].iloc[0]['code']
		total_distance = total_distance + temp_df[temp_df['stopby']==0].iloc[0][current_point+'_운행거리']
		temp_df[temp_df['stopby']==0].loc[(temp_df['code']==Max_distance_row_point),'stopby']=1

		#update situation
		trace_list , left_drive_time, left_kappa, total_distance = situation_update(temp_df, day, Max_distance_row_point,current_point, trace_list, total_distance, left_kappa, left_drive_time, average_speed, get_up, get_off)
		logger.info(f'>> start : {trace_list[-2]} |  arrive : {trace_list[-1]} | left time : { left_drive_time}  | left kappa : { left_kappa}')	

		#temp_df = temp_df[temp_df['stopby']==0].copy()
		current_point = Max_distance_row_point
		#두번쨰 긴거리, 방위각 정렬 descending descending
		temp_df, trace_list, total_distance = after_first_time(temp_df[temp_df['stopby']==0], day, current_point, trace_list ,total_distance, left_kappa, left_drive_time, average_speed, get_up, get_off)

		tt.autolog()
	return temp_df, trace_list, total_distance


#CSV FILE

client = ss.DF_client
dp = ss.DP
df_info = ss.Df_info

sch = sch_sys(client, dp, df_info)
df_client, df_DP, df_info = sch.load_data()
df_merge = sch.merge_data(df_client, df_DP, "right")
df_client = df_merge.copy()

week_num = ss.week_list

for day in week_num:
	for DF_element in  df_DP['DP'].values.tolist():
		
		current_point = 'DP'

		temp_df = sch.dateframe_init(df_client)
		
		temp_df, trace_list, total_distance = route(temp_df, DF_element, day, current_point)
			
		temp_df.to_csv("./test0/"+day+"_"+DF_element+"_dataframe.csv",columns = temp_df.columns,encoding='euc-kr')
		logger.debug("---------------------------------------------------------------------------------------------")
		logger.info("---------------------------------------------------------------------------------------------")

