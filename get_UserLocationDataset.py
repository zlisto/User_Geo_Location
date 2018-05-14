'''
  Created by aqm1152 on 7/31/17.
'''
import sys
import os
from  twitter_credentials import *
from userData_class import *
from function_HELPER import *

_SOURCE = 1
_SINK = 0


#####################################################################################
#####  setup
working_dir="./"
os.chdir(working_dir)


#####################################################################################
#####  Upper bound runtime
runtime_hours=5
total_collection_seconds=runtime_hours*60*60  
stop_collecting_time=datetime.now()+timedelta(0,total_collection_seconds)


#####################################################################################
#####  Do not forget to fill up twitter_credentials.py with your API keys
twitter = Twython(twitter_auth['consumer_key'],twitter_auth['consumer_secret'],twitter_auth['access_token'],twitter_auth['access_token_secret'])



#####################################################################################
#####  location specific information
languages=['fr']
utc_offsets=[1*60*60]
db_name='Lille'  #Use the city name
geocode_name="Lille, France" ### To obtain location coordinates
label_radius=10 #miles
geo_location = getGeoCode(geocode_name)
#####################################################################################
#####  Location terms likely to be found in profile description

location_terms=[
    'lille, france',
    'lille france'
    'lille-france',
    'lille',
    'lille, france',
    'france-lille',
    'france lille',
    'france, lille',
    'Nord, lille',
    'lille, Nord',
    'lille Nord',
    'lille-Nord',
    'lille-Nord',
    'france'
]


#####################################################################################
##### large world cities
wcs=['dubai', 'kabul', 'yerevan', 'luanda', 'cordoba', 'rosario', 'vienna', 'adelaide', 'brisbane', 'melbourne', 'perth', 'sydney', 'baku', 'dhaka', 'khulna', 'brussels', 'ouagadougou', 'sofia', 'belem', 'belo horizonte', 'brasilia', 'campinas', 'curitiba', 'fortaleza', 'goiania', 'guarulhos', 'manaus', 'nova iguacu', 'porto alegre', 'recife', 'rio de janeiro', 'salvador', 'sao paulo', 'minsk', 'montreal', 'toronto', 'vancouver', 'kinshasa', 'lubumbashi', 'brazzaville', 'abidjan', 'santiago', 'douala', 'yaounde', 'anshan', 'changchun', 'chengdu', 'chongqing', 'dalian', 'datong', 'fushun', 'fuzhou', 'guangzhou', 'guiyang', 'handan', 'hangzhou', 'harbin', 'hefei', 'huainan', 'jilin', 'jinan', 'kunming', 'lanzhou', 'luoyang', 'nanchang', 'nanjing', 'peking', 'qingdao', 'rongcheng', 'shanghai', 'shenyang', 'shenzhen', 'suzhou', 'taiyuan', 'tangshan', 'tianjin', 'urumqi', 'wuhan', 'wuxi', 'xian', 'xianyang', 'xinyang', 'xuzhou', 'barranquilla', 'bogota', 'cali', 'medellin', 'prague', 'berlin', 'hamburg', 'munich', 'copenhagen', 'santo domingo', 'algiers', 'guayaquil', 'quito', 'alexandria', 'cairo', 'gizeh', 'barcelona', 'madrid', 'addis abeba', 'paris', 'london', 'tbilisi', 'accra', 'kumasi', 'conakry', 'port-au-prince', 'budapest', 'bandung', 'bekasi', 'depok', 'jakarta', 'makasar', 'medan', 'palembang', 'semarang', 'surabaya', 'tangerang', 'dublin', 'agra', 'ahmadabad', 'allahabad', 'amritsar', 'aurangabad', 'bangalore', 'bhopal', 'bombay', 'calcutta', 'delhi', 'faridabad', 'ghaziabad', 'haora', 'hyderabad', 'indore', 'jabalpur', 'jaipur', 'kalyan', 'kanpur', 'lakhnau', 'ludhiana', 'madras', 'nagpur', 'new delhi', 'patna', 'pimpri', 'pune', 'rajkot', 'surat', 'thana', 'vadodara', 'varanasi', 'visakhapatnam', 'baghdad', 'esfahan', 'karaj', 'mashhad', 'qom', 'shiraz', 'tabriz', 'milan', 'rome', 'hiroshima', 'kawasaki', 'kobe', 'nagoya', 'saitama', 'tokyo', 'nairobi', 'phnum penh', 'seoul', 'almaty', 'bayrut', 'beirut', 'tripoli', 'casablanca', 'fez', 'rabat', 'antananarivo', 'bamako', 'mandalay', 'rangoon', 'ecatepec', 'guadalajara', 'juarez', 'leon', 'mexico', 'monterrey', 'nezahualcoyotl', 'puebla', 'tijuana', 'kuala lumpur', 'maputo', 'benin', 'ibadan', 'kaduna', 'kano', 'lagos', 'maiduguri', 'port harcourt', 'managua', 'lima', 'davao', 'manila', 'faisalabad', 'gujranwala', 'hyderabad', 'karachi', 'lahore', 'multan', 'peshawar', 'rawalpindi', 'warsaw', 'bucharest', 'belgrade', 'chelyabinsk', 'kazan', 'moscow', 'nizhniy novgorod', 'novosibirsk', 'omsk', 'rostov-na-donu', 'saint petersburg', 'samara', 'ufa', 'volgograd', 'yekaterinburg', 'jiddah', 'mecca', 'riyadh', 'khartoum', 'umm durman', 'stockholm', 'singapore', 'freetown', 'dakar', 'mogadishu', 'aleppo', 'damascus', 'bangkok', 'adana', 'ankara', 'bursa', 'gaziantep', 'istanbul', 'izmir', 'kaohsiung', 'kaohsiung', 'taichung', 'taipei', 'dar es salaam', 'kiev', 'odesa', 'kampala', 'phoenix', 'los angeles', 'san diego', 'chicago', 'new york', 'philadelphia', 'dallas', 'houston', 'san antonio', 'montevideo', 'tashkent', 'caracas', 'maracaibo', 'valencia', 'hanoi', 'ha noi', 'ho chi minh city', 'cape town', 'durban', 'johannesburg', 'pretoria', 'soweto', 'lusaka', 'harare']
outside_location_terms=[i[1] for i in wcs if db_name.lower() not in i[1].lower()]




#####################################################################################
#####  Seed Users : manually find set of seed users
seed_user_ids=[
2650617458, # @lillefrance
19720121, # @lille3000
82892603, # @MEL_Lille
288804720, # @losclive
26031424, # @MartineAubry
577342970, # @pdesaintignon
]


#####################################################################################
##### Test connection to API
test_profile=twitter.show_user(screen_name='Twitter')
test_x=features(test_profile,location_terms,outside_location_terms,languages,utc_offsets)




seed_users=twitter.lookup_user(user_id=seed_user_ids)


#####################################################################################
#####  Database
conn=sqlite3.connect(db_name.lower()+".db")
user_data_set=UserData(conn,geo_location,db_name)


### Note that if the database already exists, new tables will NOT be created.
### This will be a problem if you are attempting to use a new probability model on an existing database.
def X(profile):
    return(features(profile,location_terms,wcs,languages,utc_offsets)[0])

user_data_set.create_tables(test_x[1])
user_data_set.add_users(seed_users,X,label=1) ##if table already exists, this will return an error



#####################################################################################
#####  expand-classify

## First iteration
current_time=datetime.now()
iteration=1
print("Starting iteration {0:d} expand".format(iteration))
sys.stdout.flush()
user_count=user_data_set.expand_step(twitter,X,sample_size=15)
print("Expand complete; {0:d} users in data set".format(user_count))
sys.stdout.flush()
user_data_set.collect_geo_users(twitter,min_num_geo=500)
phi=fit_LR(user_data_set,label_radius,'Iteration{0:d}_LogReg_ROC'.format(iteration))
user_data_set.classify_step(phi,psi)
user_data_set.make_map_ROC('Iteration{0:d}'.format(iteration),label_radius)
wait_time=max(0,900-(datetime.now()-current_time).total_seconds())
user_data_set.collect_geo_users(twitter,max_time_collecting=wait_time)
phi=fit_LR(user_data_set,label_radius,'Iteration{0:d}_LogReg_ROC'.format(iteration))
user_data_set.update_phi_Optim0(phi)
wait_time=max(0,900-(datetime.now()-current_time).total_seconds())
time.sleep(wait_time)
current_time=datetime.now()
iteration+=1


while(current_time<stop_collecting_time):
    print("Starting iteration {0:d} expand".format(iteration))
    user_count=user_data_set.expand_step(twitter,X,sample_size=15)
    print("Expand complete; {0:d} users in data set".format(user_count))
    sys.stdout.flush()
    user_data_set.classify_step(phi,psi)
    user_data_set.make_map_ROC('Iteration{0:d}'.format(iteration),label_radius)
    wait_time=max(0,900-(datetime.now()-current_time).total_seconds())
    user_data_set.collect_geo_users(twitter,max_time_collecting=wait_time)
    phi=fit_LR(user_data_set,label_radius,'Iteration{0:d}_LogReg_ROC'.format(iteration))
    user_data_set.update_phi_Optim0(phi)
    wait_time=max(0,900-(datetime.now()-current_time).total_seconds())
    time.sleep(wait_time)
    current_time=datetime.now()
    iteration+=1
    

### Complete the record

user_data_set.collect_geo_users(twitter)
phi=fit_LR(user_data_set,label_radius,'Final_LogReg_ROC')
user_data_set.update_phi_Optim0(phi)
user_data_set.classify_step(phi,psi)
user_data_set.make_map_ROC('Final',label_radius)

conn.close()