# This is part of code form a project
# Idea is to get data from mysql (an open source medical records system called 'openmrs')
# The do analytics on it.
# Display the results via frontend ( done in flask , graphs were created using Bokeh)



## Getting required Libraries
## Commenting out modules not required for this demo
from __future__ import division

#from flask import Flask, app,session,render_template, request,url_for
import pandas as pd
#from bokeh.charts import Scatter,Histogram,Bar,Horizon,Donut,BoxPlot
#from bokeh.embed import components
#from bokeh.plotting import figure
#from bokeh.charts.attributes import cat, color
#from bokeh.charts.operations import blend
#from bokeh.io import gridplot
#from bokeh.models import Legend,HoverTool,ColumnDataSource,Panel,Tabs
#from bokeh.models.widgets import Paragraph
#from bokeh.palettes import Blues5
import time
import numpy as np
from datetime import datetime,timedelta
import MySQLdb as mdb
import pandas.io.sql as sql
from collections import OrderedDict
#from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from passlib.hash import django_pbkdf2_sha256 as handler
from geopy.distance import vincenty
#app = Flask(__name__)
# Below is a list of numbers indicating concepts in the openmrs mysql db
# The names are in the list below, most are vitals collected from patient ( in our case by devces which transfer data via bluetooth to tab, which in turn transfers to cloud) Expanded names are Temperature, Systolic Blood Pressure, Diastolic Blood Pressur, Heart rate, Fasting Blood Sugar, Post Prandial Blood Sugar, Oxygen saturation, Electrocardigram result, haemoglobin, and Body mass Index
clist = [5088,5085,5087,5086,6106,6107,6118,5092,6117,21,1342]

#['Temp','SBP','HR','DBP','RBG','FBG','PPBG', 'Spo2', 'ECG','Hb', 'BMI']

#auth = HTTPBasicAuth()
# Get list of users from system
passd = 'rootsy'
conn = mdb.connect('localhost','root',passd,'chikitsak');
cursor = conn.cursor()
query = 'select username from user_type'
cursor.execute(query)
users = cursor.fetchall();
#user = list(users)
user = []
for i in users:
       user.append(i[0])
# This is used to check user when he/she logs in 
#@auth.verify_password
#def verify_password(username, password):
#
 #   if username in user:
  #      cursor.execute("select password from user_type where username=%s" ,(username,))
   #     row = cursor.fetchone();
    #print row[0]
    #    return handler.verify(password, row[0])
    #return False
#conn.close()
# usually gotten via username when user logs in
username='HWorker'


# Function to get all chikitsaks assigned to user . An aside Each chikitsak is authorized to enter data via tab to the server. The user is someone who manages say some chikitsaks.
def get_chikitsaks(chiki):
        conn = mdb.connect('localhost','root',passd,'chikitsak');
        cursor = conn.cursor()
        q1 = 'select user_id from user_type where username=%s'
        cursor.execute(q1,(chiki,))
        row1 = cursor.fetchone();
        q2 = 'select username,oid,target,avg_time from user_type where parent_id = %s'
        cursor.execute(q2,(row1[0],))
        row2 = cursor.fetchall();
        conn.close()
        un = [i[0] for i in row2]
        oid = [int(i[1]) for i in row2]
        target = [int(i[2]) for i in row2]
        avg_time = [float(i[3]) for i in row2]
        return un,oid,target,avg_time


# Some funstions for filtering data based on whteher the reading is in range or not and assigning values like 'normal', 'at risk' or 'sick'
def filter_n(n):
        ans = ''
        if n == 'at_risk_l' or n == 'at_risk_h' or n == 'at_risk':
                ans = 'at_risk'
        elif n == 'normal' or n == 1:
                ans = 'normal'

        elif n == 'sick_l' or n == 'sick_h' or n == 0:
                ans = 'sick'
        elif pd.isnull(n):
                ans =  np.nan

        return ans

def filter_n1(row):
        if (pd.isnull(row['rSug_cat']) and pd.isnull(row['fSug_cat'])):
                ans1 = row['ppSug_cat']
        elif (pd.isnull(row['rSug_cat']) and pd.isnull(row['ppSug_cat'])):
                ans1 = row['fSug_cat']
        else:
                ans1 = row['rSug_cat']


        return ans1
def filter_n2(row):
	if row['SBP'] == 'normal' and row['DBP'] == 'normal':
	      ans2 = 'normal'
	elif row['SBP'] == 'sick' or row['DBP'] == 'sick':

	      ans2 = 'sick'
	 
	else:
              ans2 = 'at_risk'

        return ans2
# A function written to get distance from lat and long of places visited by chikitsak. A dummy right now as the db does not have lat and long values
def avg_dist(df):
        df_ll = df[['Date','Longitude','Latitude']]
    	df_ll['Date'] = pd.to_datetime(df_ll['Date'])
    	df_ll['lat_long'] = df_ll[['Latitude','Longitude']].apply(tuple, axis=1)
            #df_ll['week_no'] = df_ll["Date"].apply(lambda x: x.week)
            #current_week = datetime.datetime.now().isocalendar()[1]
    	current_week = 4
    	df_ll_1 = df_ll[df_ll['week_no']==current_week]
    	#print df_ll.head(10)
    	lat_long = df_ll_1['lat_long'].tolist()
    	avg_dist = []
    	for i in range(len(lat_long)-1):
    		dist = vincenty(i, i+1).km
    		avg_dist.append(dist)
    	avg_distance = np.mean(avg_dist)
        return avg_distance

# Function to get data for the first screen on the dashboard ( called admin screen)
def time_and_numtest(dfb1,crlist,un,target,avg_time):

        #df2 = df1[['creator','Date']]
	#print dfb1
	#print "papapappa"
	dfb1['creator'] = dfb1['creator'].replace(crlist, un)
	#print dfb1
	#print "tatatatat"
	creators = dfb1['creator'].unique().tolist()
        today = datetime.now()
        currentYear = datetime.now().year
        num_cr = len(un)
        df_today = dfb1[dfb1['Date'] == today]
        cr_in_today = len(df_today['creator'].unique())
        dfb1['Count'] = 1
        dfb1['ddate'] = pd.to_datetime(dfb1['Date'])
        dfb1.index=dfb1['ddate']
        df_www1 = dfb1[['ddate','creator','Count']]
        #print df_www1
	#print "agagagagag"
	df2_week = dfb1.pivot_table(index=['ddate'],columns='creator',values='Count',aggfunc=np.sum)
        df_month = df2_week.resample('BM',how='sum')
        df_month['month_sum'] = df_month.sum(axis=1)
        df_year = df2_week.resample('A',how='sum')
        df_week = df2_week.resample('W',how='sum')
        df_year['tots'] = df_year.sum(axis=1)
        df_week.reset_index(inplace=True)
        df_week.ddate = df_week.ddate.apply(lambda x: str(x).split(' 00:00:00')[0])
        #print df_week.head(30)
	df_ws = df_week.mean(axis=0)
	#print df_ws.head(20)
        df_ws = df_ws.to_frame().reset_index()
        df_ws.reset_index(inplace=True)
	#print "hahahah"
        #print df_ws
	#print target
        df_ws['target1'] = target#se.values
        df_ws.columns = ['index','health_worker','no_of_tests','target1']
        df_ws['target'] = df_ws['target1'] - df_ws['no_of_tests']
        df_ws1 = pd.melt(df_ws.reset_index(), id_vars=['health_worker'],value_vars =['no_of_tests','target'])
        #print df_ws1
        #print df_ws1
        #data2 = df_ws1.to_json(orient='records')
        #print data2
        df_at = pd.DataFrame()
        df_at['health_worker']=un
        df_at['avg_time'] = avg_time
        #dist = vincenty(i,j).metres
        df_week['week_sum'] = df_week.sum(axis=1)
        num_this_week = int(df_week.iloc[-1]['week_sum'])
        num_last_week = int(df_week.iloc[-2]['week_sum'])
        num_this_month = int(df_month.iloc[-1]['month_sum'])
        num_last_month = int(df_month.iloc[-2]['month_sum'])
        num_this_year = int(df_year.iloc[-1]['tots'])
        per_week_change = round((num_this_week-num_last_week)/float(num_last_week) * 100,2)
        per_month_change = round((num_this_month-num_last_month)/float(num_last_month) * 100,2)

        year_target = int(today.isocalendar()[1]*sum(target))
        per_covered = round((num_this_year/float(year_target))*100,2)

        tots_weekly = 0
        for i in creators:
                tots_weekly  = tots_weekly + df_week[i].mean()

        avg_test = round(tots_weekly/float(num_cr),2)
        test_numbers = []
        test_numbers.append(num_this_week)
        test_numbers.append(num_last_week)
        test_numbers.append(per_week_change)
        test_numbers.append(num_this_month)
        test_numbers.append(num_last_month)
        test_numbers.append(per_month_change)
        test_numbers.append(num_this_year)
        test_numbers.append(year_target)
        test_numbers.append(per_covered)
        test_numbers.append(avg_test)
        test_numbers.append(num_cr)
        test_numbers.append(cr_in_today)

        return df_ws,df_ws1,df_at,test_numbers
# A function to get data for second screen on dashboard called community.
def dis_bur(df1,a_categ):
	if a_categ == "None":
                pass
        elif a_categ == "child":
                df1 = df1[df1['age'] < 19]
        elif a_categ == "Adult":
                df1 = df1[(df1['age'] > 19) & (df1['age'] < 45)]
        elif a_categ == "MiddleAge":
                 df1 = df1[(df1['age'] > 45) & (df1['age'] < 65)]
        elif a_categ == "Aged":
                df1 = df1[(df1['age'] > 65) & (df1['age'] < 80)]
        elif a_categ == "Old":
                 df1 = df1[df1['age'] > 80]

        bins = [1,4,12,14,20]
        group_names = ['sick_l','normal','at_risk','sick_h']
        df1['Hb_cat'] = pd.cut(df1['Hb'], bins, labels=group_names)

        bins = [5,70,120,180,400]
        group_names = ['sick_l','normal','at_risk','sick_h']
        df1['rSug_cat'] = pd.cut(df1['rsugar'], bins, labels=group_names)

	bins = [5,80,100,126,400]
        group_names = ['sick_l','normal','at_risk','sick_h']
        df1['fSug_cat'] = pd.cut(df1['fsugar'], bins, labels=group_names)

	bins = [5,120,140,200,400]
        group_names = ['sick_l','normal','at_risk','sick_h']
        df1['ppSug_cat'] = pd.cut(df1['ppsugar'], bins, labels=group_names)

        bins = [5,60,100,140,200]
        group_names = ['sick_l','at_risk','normal','sick_h']
        df1['SBP_cat'] = pd.cut(df1['SBP'], bins, labels=group_names)

        bins = [5,40,60,80,90]
	group_names = ['sick_l','at_risk','normal','sick_h']
        df1['DBP_cat'] = pd.cut(df1['DBP'], bins, labels=group_names)

        bins = [60,90,95,100]
        group_names = ['sick_l','normal','sick_h']
        df1['SpO2_cat'] = pd.cut(df1['Spo2'], bins, labels=group_names)

        bins = [30,60,100,120]
        group_names = ['sick_l','normal','sick_h']
        df1['HeartRate_cat'] = pd.cut(df1['HR'], bins, labels=group_names)

	bins = [8,18,25,30,40]
        group_names = ['sick_l','normal','at_risk','sick_h']
        df1['BMI_cat'] = pd.cut(df1['BMI'], bins, labels=group_names)

	bins = [90,98.4,99.4,101]
        group_names = ['sick_l','normal','sick_h']
        df1['Temp_cat'] = pd.cut(df1['Temp'], bins, labels=group_names)

        df1['Sugar1'] = df1[['rSug_cat','fSug_cat','ppSug_cat']].apply(filter_n1 , axis=1)

        df7 = df1[['Gender','Hb_cat','Sugar1','SBP_cat','DBP_cat','HeartRate_cat','SpO2_cat','Temp_cat','ECG','BMI_cat','pid','eid']]
        #print df7.head(4)
        #print df7.head(20)
        df7["SBP"] = df7['SBP_cat'].apply(filter_n)
        df7['DBP'] = df7['DBP_cat'].apply(filter_n)
        #df7['rSugar'] = df7['rSug_cat'].apply(filter_n)
	#df7['fSugar'] = df7['fSug_cat'].apply(filter_n)
	#df7['ppSugar'] = df7['ppSug_cat'].apply(filter_n)
        df7['Sugar'] = df1['Sugar1'].apply(filter_n)
        #print df7.head(20)
        df7['Hb'] = df7['Hb_cat'].apply(filter_n)
        df7['HR'] = df7['HeartRate_cat'].apply(filter_n)
        df7['SPO2'] = df7['SpO2_cat'].apply(filter_n)
	df7['BMI'] = df7['BMI_cat'].apply(filter_n)
	df7['Temp'] = df7['Temp_cat'].apply(filter_n)
	df7['ECG'] = df7['ECG'].apply(filter_n)
	#df7['Sugar'] = df7[['rSugar', 'fSugar','ppSugar']].fillna('').sum(axis=1)
	df7['BP'] = df7[['SBP','DBP']].apply(filter_n2 , axis=1)
	#df7.dropna(inplace=True)
	#df7['BP'] =
        df8 = df7[['Gender','Hb','BP','Sugar','HR','SPO2','ECG','BMI','eid','pid']]
	#print df8.head(5)
        df81 = df7[['Hb','BP','Sugar','HR','SPO2','ECG','BMI']]
        df9 = df81.apply(pd.value_counts)#.T
        df9.index.names=['health_condition']
        df9.reset_index(inplace=True)
	df9 = df9.fillna(0)
        #df91 = pd.melt(df9, id_vars=['health_condition'],value_vars =['normal','at_risk'])
        #df91.columns=['health_condition','status','num_people']
        #data2 = df9.to_json(orient='index')
        data2 = df9.values.tolist()
	#print df9
	df9a=df9.set_index('health_condition').T
        df9a.reset_index(inplace=True)
        df9a.columns=['Factor','at_risk','normal','sick']
	df9a['tot'] = df9a.sum(axis=1)
        tval = np.mean(df9a['tot'].tolist())
        for i in ['at_risk','normal','sick']:
                df9a[i] = 100*(df9a[i]/tval)
        df9a[['at_risk','normal','sick']] = df9a[['at_risk','normal','sick']].astype(int)
        #df9a = df9a.drop('NaN', 1)
	#df9a.columns=['Factor','at_risk','normal','sick']
	#print df9a
	#print data2
        name = df81.columns.tolist()
        #df9['ratio'] = df9['normal']/df9['at_risk']
        #df9['x'] = range(1,len(df9)+1)
        #df9['y'] = name
        #print df9
        #print df91
        df8.index.names=['health_cond']
        df8.reset_index(inplace=True)
        health_cond = df8['health_cond'].tolist()
        is_m = df8['Gender']=='M'
        is_w = df8['Gender'] == 'F'

	
        


	n_diab = df8['Sugar'] == 'normal'
        n_BP = df8['BP'] == 'normal'
        #nn_BP = df8['BP'] == 'normal'
        #ns_BP = df8['BP'] == 's'
        #print n_BP.head(10)
        # print nn_BP.head(10)
        # print ns_BP.head(10)
        # print len(df8),len(df8[n_BP]),len(df8[nn_BP]),len(df8[ns_BP])
        n_hb = df8['Hb'] == 'normal'
        n_spo2 = df8['SPO2'] == 'normal'
        n_hr = df8['HR'] == 'normal'
        n_ecg = df8['ECG'] == 'normal'
        n_bmi = df8['BMI'] == 'normal'
        tot_m = len(df8[is_m])
        tot_w = len(df8[is_w])
        db_n = len(df8[n_diab])
        BP_n = len(df8[n_BP])
        hb_n = len(df8[n_hb])
        hr_n = len(df8[n_hr])
        spo2_n = len(df8[n_spo2])
        ecg_n = len(df8[n_ecg])
        bmi_n = len(df8[n_bmi])
        all_n = len(df8[n_diab & n_BP & n_hb & n_hr & n_spo2 & n_ecg & n_bmi])
        #print "db_n,BP_n,hb_n,hr_n,spo2_n,ecg_n,bmi_n,all_n"
        #print db_n, BP_n, hb_n, hr_n, spo2_n, ecg_n, bmi_n, all_n
        per_men_n = round(len(df8[is_m & (
        n_diab & n_BP & n_hb & n_hr & n_spo2 & n_ecg & n_bmi)]) / float(tot_m) * 100, 2)
        tot_a = len(df8)
        uni_enc = len(df8['eid'].unique())
        uni_pat = len(df8['pid'].unique())
        #  print tot_a,uni_enc, uni_pat
        #print (len(df8)) - (uni_enc - uni_pat), uni_enc, uni_pat
        per_normal = round(len(df8[n_diab & n_BP & n_hb & n_hr & n_spo2 &
                              n_ecg & n_bmi]) / float((len(df8)) - (uni_enc - uni_pat)) * 100, 2)
        per_rep = round((uni_enc - uni_pat) / float(uni_enc), 2) * 100
        per_women_n = round(len(df8[is_w & (
        n_diab & n_BP & n_hb & n_hr & n_spo2 & n_ecg & n_bmi)]) / float(tot_w) * 100, 2)
        per_not_normal = 100 - per_normal
        list1 = ['at_risk','normal','repeat_customer']
        df10= pd.DataFrame({'category': list1})
        df10['count'] = [per_not_normal,per_normal,per_rep]
        data1 = df10['count'].tolist()
        risk_data=[]
        risk_data.append(per_normal)
        risk_data.append(per_not_normal)
        risk_data.append(per_rep)
        risk_data.append(per_women_n)
        risk_data.append(per_men_n)
	if a_categ == "None":
                return df9a,df10,data1,risk_data
        elif a_categ == "child":
                return df9a
        elif a_categ == "Adult":
                return df9a
        elif a_categ == "MiddleAge":
                return df9a
        elif a_categ == "Aged":
                return df9a
        elif a_categ == "Old":
                return df9a
# A function to get disease burden split by age and sex
def dis_bur_all(df1,a_categ,sex):
	if a_categ == "None":
                pass
        if a_categ == "Young Adult":
                df1 = df1[(df1['age'] > 18) & (df1['age'] <= 36)]
        elif a_categ == "MiddleAge":
                 df1 = df1[(df1['age'] > 36) & (df1['age'] <= 56)]
        elif a_categ == "Old":
                 df1 = df1[df1['age'] > 56]

        bins = [1,7,11,14]
        group_names = ['sick_l','at_risk','normal']
        df1['Hb_cat'] = pd.cut(df1['Hb'], bins, labels=group_names)

        bins = [5,70,120,180,400]
        group_names = ['sick_l','normal','at_risk','sick_h']
        df1['rSug_cat'] = pd.cut(df1['rsugar'], bins, labels=group_names)

	bins = [5,80,100,126,400]
        group_names = ['sick_l','normal','at_risk','sick_h']
        df1['fSug_cat'] = pd.cut(df1['fsugar'], bins, labels=group_names)

	bins = [5,120,140,160,400]
        group_names = ['sick_l','normal','at_risk','sick_h']
        df1['ppSug_cat'] = pd.cut(df1['ppsugar'], bins, labels=group_names)

        bins = [5,60,100,120,140,200]
        group_names = ['sick_l','at_risk_l','normal','at_risk_h','sick_h']
        df1['SBP_cat'] = pd.cut(df1['SBP'], bins, labels=group_names)

        bins = [5,40,60,80,90,120]
	group_names = ['sick_l','at_risk_l','normal','at_risk_h','sick_h']
        df1['DBP_cat'] = pd.cut(df1['DBP'], bins, labels=group_names)

        bins = [60,90,95,100]
        group_names = ['sick_l','normal','sick_h']
        df1['SpO2_cat'] = pd.cut(df1['Spo2'], bins, labels=group_names)

        bins = [30,60,100,120]
        group_names = ['sick_l','normal','sick_h']
        df1['HeartRate_cat'] = pd.cut(df1['HR'], bins, labels=group_names)

	bins = [8,18,25,30,40]
        group_names = ['sick_l','normal','at_risk','sick_h']
        df1['BMI_cat'] = pd.cut(df1['BMI'], bins, labels=group_names)

	bins = [90,98.4,99.4,101]
        group_names = ['sick_l','normal','sick_h']
        df1['Temp_cat'] = pd.cut(df1['Temp'], bins, labels=group_names)

        df1['Sugar1'] = df1[['rSug_cat','fSug_cat','ppSug_cat']].apply(filter_n1 , axis=1)

        df7 = df1[['Gender','Hb_cat','Sugar1','SBP_cat','DBP_cat','HeartRate_cat','SpO2_cat','Temp_cat','ECG','BMI_cat','pid','eid']]
        #print df7.head(20)
        df7["SBP"] = df7['SBP_cat'].apply(filter_n)
        df7['DBP'] = df7['DBP_cat'].apply(filter_n)
        df7['Sugar'] = df1['Sugar1'].apply(filter_n)
        #print df7.head(20)
        df7['Hb'] = df7['Hb_cat'].apply(filter_n)
        df7['HR'] = df7['HeartRate_cat'].apply(filter_n)
        df7['SPO2'] = df7['SpO2_cat'].apply(filter_n)
	df7['BMI'] = df7['BMI_cat'].apply(filter_n)
	df7['Temp'] = df7['Temp_cat'].apply(filter_n)
	df7['ECG'] = df7['ECG'].apply(filter_n)
	df7['BP'] = df7[['SBP','DBP']].apply(filter_n2 , axis=1)
        df8 = df7[['Gender','Hb','BP','Sugar','HR','SPO2','ECG','BMI','eid','pid']]
	#print df8.head(5)
	if (sex == "Male"):
                df7a = df7[df7['Gender']=='M']
        elif (sex == "Female"):
                df7a = df7[df7['Gender']=='F']

	
        df81 = df7a[['Hb','BP','Sugar','HR','SPO2','ECG','BMI']]
        df9 = df81.apply(pd.value_counts)#.T
        df9.index.names=['health_condition']
        df9.reset_index(inplace=True)
	df9 = df9.fillna(0)
        #df91 = pd.melt(df9, id_vars=['health_condition'],value_vars =['normal','at_risk'])
        #df91.columns=['health_condition','status','num_people']
        #data2 = df9.to_json(orient='index')
        data2 = df9.values.tolist()
	#print df9
	df9a=df9.set_index('health_condition').T
        df9a.reset_index(inplace=True)
        df9a.columns=['Factor','at_risk','normal','sick']
	#df9a['tot'] = df9a.sum(axis=1)
        #tval = np.mean(df9a['tot'].tolist())
        #for i in ['at_risk','normal','sick']:
        #        df9a[i] = 100*(df9a[i]/tval)
        df9a[['at_risk','normal','sick']] = df9a[['at_risk','normal','sick']].astype(int)
        #df9a = df9a.drop('NaN', 1)
	#df9a.columns=['Factor','at_risk','normal','sick']

	return df9a


# Called to craete dasboard with data split by age and sex
def analyse_all():
	clista = [7,8,9,10,11]
	conn = mdb.connect('localhost','root',passd,'openmrst');
        cursor = conn.cursor()
        query = 'select obs.value_numeric,person.creator,TIMESTAMPDIFF(YEAR, person.birthdate, CURDATE()) AS age,encounter.date_created,person.gender,person.person_id,obs.concept_id, encounter.encounter_id from obs  left join person on obs.person_id=person.person_id left join person_address on obs.person_id = person_address.person_id left join encounter on encounter.patient_id = obs.person_id where obs.concept_id in %s and person.creator in %s' % (str(tuple(clist)), str(tuple(clista))) #(17,18)'
	df = sql.read_sql(query, conn)
        df1 = df.pivot_table(index=['gender','creator','age','date_created','person_id','encounter_id'],columns='concept_id',values='value_numeric')
        #df1a = df.pivot_table(index=['person_id','encounter_id'],columns='concept_id',values='value_text')
        df1 = df1.reset_index()
	my_columns = ['Gender','creator','age','Date','pid','eid','Hb','BMI','SBP','DBP','HR','Temp','Spo2','rsugar','fsugar','ECG','ppsugar']
        df1.columns=my_columns


	dfmall = dis_bur_all(df1,'None','Male')
        dffall = dis_bur_all(df1,'None','Female')
        dfmYA = dis_bur_all(df1,'Young Adult','Male')
        dffYA = dis_bur_all(df1,'Young Adult','Female')
        dfmMA = dis_bur_all(df1,'MiddleAge','Male')
        dffMA = dis_bur_all(df1,'MiddleAge','Female')
        dfmO = dis_bur_all(df1,'Old','Male')
        dffO = dis_bur_all(df1,'Old','Female')
        #print dfmYA.head(10)
        #print dffO.head(10)
        return dfmall,dffall,dfmYA,dffYA,dfmMA,dffMA,dfmO,dffO
# Called to create dashboard one
def analyse1():
	un,crlist,target,avg_time = get_chikitsaks(username)
	#print un, crlist
        conn = mdb.connect('localhost','root',passd,'openmrst');
        cursor = conn.cursor()
        query2 = 'select person_address.person_id, encounter_id,person_address.longitude,person_address.latitude, encounter.date_created ,encounter.creator from encounter left join person_address on encounter.patient_id=person_address.person_id  where encounter.creator in %s' % (str(tuple(crlist)))
        dfb = sql.read_sql(query2,conn)

#       #print avg_distance
        #avg_diatance = avg_dist(dfb)
        avg_distance = 0
        dfb.rename(columns={'date_created':'Date'},inplace=True)
        dfb1 = dfb[['creator','Date']]

        dfb1['creator'] = dfb1['creator'].replace(crlist, un)

        #print dfb1.head(30)
        #creators = df2['creator'].unique().tolist()
        df_ws,df_ws1,df_at,test_numbers = time_and_numtest(dfb1,crlist,un,target,avg_time)
        return  un,df_ws1,df_ws,df_at,avg_distance,test_numbers

# Called to craete dashboard 2
def analyse2():
        un,crlist,target,avg_time = get_chikitsaks(username)
        conn = mdb.connect('localhost','root',passd,'openmrst');
        cursor = conn.cursor()
        query = 'select obs.value_numeric,person.creator,TIMESTAMPDIFF(YEAR, person.birthdate, CURDATE()) AS age,encounter.date_created,person.gender,person.person_id,obs.concept_id, encounter.encounter_id from obs  left join person on obs.person_id=person.person_id left join person_address on obs.person_id = person_address.person_id left join encounter on encounter.patient_id = obs.person_id where obs.concept_id in %s and person.creator in %s' % (str(tuple(clist)), str(tuple(crlist))) #(17,18)'

        query1 = 'select obs.value_text,person.creator,person.person_id,obs.concept_id ,encounter.encounter_id from obs  left join person on obs.person_id=person.person_id left join person_address on obs.person_id = person_address.person_id left join encounter on encounter.patient_id = obs.person_id where obs.concept_id in %s and person.creator in %s' % (str(tuple(clist)), str(tuple(crlist))) #(17,18)'
	query2 = 'select encounter.patient_id, person_address.longitude,person_address.latitude, encounter.date_created ,encounter.creator from encounter left join person_address on encounter.patient_id=person_address.person_id and encounter.creator in %s' % (str(tuple(crlist)))
	df = sql.read_sql(query, conn)
	dfa = sql.read_sql(query1, conn)
	dfb = sql.read_sql(query2,conn)
        df1 = df.pivot_table(index=['gender','creator','age','date_created','person_id','encounter_id'],columns='concept_id',values='value_numeric')
        #df1a = df.pivot_table(index=['person_id','encounter_id'],columns='concept_id',values='value_text')
        df1 = df1.reset_index()
	df1a = df1.reset_index()
	#print df1.head(30)
	#print df1a.head(10)
	#concepts = [5090,5089,5088,5085,5087,5086,6106,6107,6118,5092,6117,21,1342]
        #concepts = [3,4,5,17,18,19,20,33,34,69]
        #[Ht,Wt,Temp,SBP,HR,DBP,rsugar,fsugar,ppsugar,Spo2,ECG,Hb,BMI]
	my_columns = ['Gender','creator','age','Date','pid','eid','Hb','BMI','SBP','DBP','HR','Temp','Spo2','rsugar','fsugar','ECG','ppsugar']
	df1.columns=my_columns

#	#print avg_distance
        #avg_diatance = avg_dist(dfb)
	avg_distance = 0
	dfb.rename(columns={'date_created':'Date'},inplace=True)
	df2 = df1[['creator','Date']]

        df2['creator'] = df2['creator'].replace(crlist, un)

        #print df1.head(30)
	#creators = df2['creator'].unique().tolist()
        df_ws,df_ws1,df_at,test_numbers = time_and_numtest(df2,crlist,un,target,avg_time)
        df9a,df10,data1,risk_data = dis_bur(df1,"None")
	#df9a1 = dis_bur(df1,"Aged")
	#print("dejajaja")
	#print df9a.head(10)
        #print df9a1.head(10)
	#print("kakakalaka")

	#print df10

        return  un,df10,df_ws1,df_ws,df9a,df_at,avg_distance,risk_data,test_numbers



#dfmall,dffall,dfmYA,dffYA,dfmMA,dffMA,dfmO,dffO = analyse_all()
	

un,df10,df_ws1,df_ws,df9a,df_at,avg_distance,risk_data,test_numbers = analyse2()
print "Unique chikitsaks: ", un
print "Data Frame", df10
print "Data Frame", df_ws1
print "Data Frame", df9a
print "Data Frame", df_at
print "Average Distance: %d" %(avg_distance)
str1 = ["numbers this week","numbers last week","Weekly percentage change","numbers this month","numbers last month","Monthly percentage change","numbers this year","target for the year","percentage covered","Average number of teststest","Number of chiktsaks","Mumber of chikitsaks in today"]

for k in range(len(str1)):
	print str1[k] + ": %s" %(test_numbers[k])
		

#un,df_ws1,df_ws,df_at,avg_distance,test_numbers = analyse1()
        

