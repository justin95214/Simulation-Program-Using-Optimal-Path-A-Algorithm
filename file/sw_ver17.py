import subprocess
import sys
import warnings
import string_source as ss
import logging
import log_package as lp
warnings.filterwarnings('ignore')
import tt as tt

#log 설정
logger = lp.log_setting(logging)


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
	df_info = ms.pd.read_excel(ss.Df_info)
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



############################################################################3
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



