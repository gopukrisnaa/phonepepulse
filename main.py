import os
import json
import pandas as pd
from Tools.scripts.var_access_benchmark import B, C
from streamlit_option_menu import option_menu
import streamlit as st
from PIL import Image

phn = Image.open("C:/Users/Admin/Pictures/phonepe/phn.png")
st.set_page_config(page_title="PhonePe Pulse", page_icon=phn, layout="wide", )

path = "C:/Users/Admin/PycharmProjects/pythonProject/Pulse/data/aggregated/transaction/country/india/state/"
user_state_list = os.listdir(path)

#Agg_state_list--> to get the list of states in India

#<------------------------------------------------------------------------------------------------------------------->#

# This is to extract the data's to create a dataframe

clm = {'State': [], 'Year': [], 'Quater': [], 'Transacion_type': [],
       'Transacion_count': [], 'Transacion_amount': []}
for i in user_state_list:
    p_i = path+i+"/"
    Agg_yr = os.listdir(p_i)
    for j in Agg_yr:
        p_j = p_i+j+"/"
        Agg_yr_list = os.listdir(p_j)
        for k in Agg_yr_list:
            p_k = p_j+k
            Data = open(p_k, 'r')
            D = json.load(Data)
            try:
                for z in D['data']['transactionData']:
                    Name = z['name']
                    count = z['paymentInstruments'][0]['count']
                    amount = z['paymentInstruments'][0]['amount']
                    clm['Transacion_type'].append(Name)
                    clm['Transacion_count'].append(count)
                    clm['Transacion_amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))
            except:
                pass
# Succesfully created a dataframe
df_aggregated_transaction = pd.DataFrame(clm)
#df_aggregated_transaction.to_csv('df_aggregated_transaction.csv')
#df_aggregated_transaction

# TO GET THE DATA-FRAME OF AGGREGATED <--> USER

path2 = "C:/Users/Admin/PycharmProjects/pythonProject/Pulse/data/aggregated/transaction/country/india/state/"
user_list = os.listdir(path2)

col2 = {'State': [], 'Year': [], 'Quater': [], 'brands': [], 'Count': [],
        'Percentage': []}
for i in user_list:
    p_i = path+i+"/"
    Agg_yr = os.listdir(p_i)
    for j in Agg_yr:
        p_j = p_i+j+"/"
        Agg_yr_list = os.listdir(p_j)
        for k in Agg_yr_list:
            p_k = p_j+k
            Data = open(p_k, 'r')
            D = json.load(Data)
            try:
                for w in B["data"]["usersByDevice"]:
                    brand_name = w["brand"]
                    count_ = w["count"]
                    ALL_percentage = w["percentage"]
                    col2["brands"].append(brand_name)
                    col2["Count"].append(count_)
                    col2["Percentage"].append(ALL_percentage)
                    col2["State"].append(i)
                    col2["Year"].append(j)
                    col2["Quater"].append(int(k.strip('.json')))
            except:
                pass
df_aggregated_user = pd.DataFrame(col2)
#df_aggregated_user.to_csv('df_aggregated_user.csv')

# TO GET THE DATA-FRAME OF MAP <--> TRANSACTION

path3 = "C:/Users/Admin/PycharmProjects/pythonProject/Pulse/data/map/transaction/hover/country/india/state"
hover_list = os.listdir(path3)

col3 = {'State': [], 'Year': [], 'Quater': [], 'District': [], 'count': [],
        'amount': []}
for i in hover_list:
    p_i = path+i+"/"
    Agg_yr = os.listdir(p_i)
    for j in Agg_yr:
        p_j = p_i+j+"/"
        Agg_yr_list = os.listdir(p_j)
        for k in Agg_yr_list:
            p_k = p_j+k
            Data = open(p_k, 'r')
            D = json.load(Data)
            try:
                for x in C["data"]["hoverDataList"]:
                    District = x["name"]
                    count = x["metric"][0]["count"]
                    amount = x["metric"][0]["amount"]
                    col3["District"].append(District)
                    col3["count"].append(count)
                    col3["amount"].append(amount)
                    col3['State'].append(i)
                    col3['Year'].append(j)
                    col3['Quater'].append(int(k.strip('.json')))
            except:
                pass
df_map_transaction = pd.DataFrame(col3)
#df_map_transaction.to_csv('df_map_transaction.csv')
#df_map_transaction

# TO GET THE DATA-FRAME OF MAP <--> USER

path4 = "C:/Users/Admin/PycharmProjects/pythonProject/Pulse/data/map/user/hover/country/india/state/"
state_list = os.listdir(path4)

col4 = {"State": [], "Year": [], "Quater": [], "District": [],
        "RegisteredUser": []}
for i in state_list:
    p_i = path+i+"/"
    year = os.listdir(p_i)
    for j in year:
        p_j = p_i+j+"/"
        file = os.listdir(p_j)
        for k in file:
            p_k = p_j+k
            Data = open(p_k, 'r')
            D = json.load(Data)
            try:
                for u in D['data']["hoverData"]:
                    district = z
                    registered_user =  D['data']["hoverData"][z]["registeredUsers"]
                    app_opening = D['data']["hoverData"][z]["appOpens"]
                    clm['District'].append(district)
                    clm['Registered_user'].append(registered_user)
                    clm['App_opening'].append(app_opening)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))
            except:
                pass
df_map_user = pd.DataFrame(col4)
#df_map_user.to_csv('df_map_user.csv')
#df_map_user

# TO GET THE DATA-FRAME OF TOP <--> TRANSACTION

path5 = "C:/Users/Admin/PycharmProjects/pythonProject/Pulse/data/top/transaction/country/india/state/"
TOP_list = os.listdir(path5)

col5 = {'State': [], 'Year': [], 'Quater': [], 'District': [], 'Transaction_count': [],
        'Transaction_amount': []}
for i in TOP_list:
    p_i = path5 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            # print(p_k)
            Data = open(p_k, 'r')
            E = json.load(Data)
            try:
                for z in E['data']['pincodes']:
                    Name = z['entityName']
                    count = z['metric']['count']
                    amount = z['metric']['amount']
                    col5['District'].append(Name)
                    col5['Transaction_count'].append(count)
                    col5['Transaction_amount'].append(amount)
                    col5['State'].append(i)
                    col5['Year'].append(j)
                    col5['Quater'].append(int(k.strip('.json')))
            except:
                pass
df_top_transaction = pd.DataFrame(col5)
#df_top_transaction.to_csv('df_top_transaction.csv')
#df_top_transaction

# TO GET THE DATA-FRAME OF TOP <--> USER

path6 = "C:/Users/Admin/PycharmProjects/pythonProject/Pulse/data/top/user/country/india/state/"
USER_list = os.listdir(path6)

col6 = {'State': [], 'Year': [], 'Quater': [], 'District': [],
        'RegisteredUser': []}
for i in USER_list:
    p_i = path6 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            # print(p_k)
            Data = open(p_k, 'r')
            F = json.load(Data)
            try:
                for t in F['data']['pincodes']:
                    Name = t['name']
                    registeredUser = t['registeredUsers']
                    col6['District'].append(Name)
                    col6['RegisteredUser'].append(registeredUser)
                    col6['State'].append(i)
                    col6['Year'].append(j)
                    col6['Quater'].append(int(k.strip('.json')))
            except:
                pass
df_top_user = pd.DataFrame(col6)
#df_top_user.to_csv('df_top_user.csv')
#df_top_user

#CREATING CONNECTION WITH SQL SERVER
import sqlite3
import sqlalchemy

user = 'root'
password = 'Gopusql05$'
host = 'localhost'
port = 3306
database = 'phonepe_pulse'
connection = sqlalchemy.create_engine("mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
    user, password, host, port, database
))

connection = sqlite3.connect("phonepe_pulse.db")
cursor = connection.cursor()
df_aggregated_transaction.to_sql('aggregated_transaction', connection, if_exists='replace')
df_aggregated_user.to_sql('aggregated_user', connection, if_exists='replace')
df_map_transaction.to_sql('map_transaction', connection, if_exists='replace')
df_map_user.to_sql('map_user', connection, if_exists='replace')
df_top_transaction.to_sql('top_transaction', connection, if_exists='replace')
df_top_user.to_sql('top_user', connection, if_exists='replace')


#with st.sidebar:
SELECT = option_menu(
    menu_title = None,
    options = ["About","Search","Home","Basic insights"],
    icons =["bar-chart","search","house","toggles","at"],
    default_index=2,
    orientation="horizontal",
    styles={
        "container": {"padding": "0!important", "background-color": "white","size":"cover"},
        "icon": {"color": "black", "font-size": "20px"},
        "nav-link": {"font-size": "20px", "text-align": "center", "margin": "-2px", "--hover-color": "#6F36AD"},
        "nav-link-selected": {"background-color": "#6F36AD"},}
    )

if SELECT == "Basic insights":
    st.title("BASIC INSIGHTS")
    st.write("----")
    st.subheader("Let's know some basic insights about the data")
    options = ["--select--","Top 10 states based on year and amount of transaction","Least 10 states based on type and amount of transaction",
               "Top 10 mobile brands based on percentage of transaction","Top 10 Registered-users based on States and District(pincode)",
               "Top 10 Districts based on states and amount of transaction","Least 10 Districts based on states and amount of transaction",
               "Least 10 registered-users based on Districts and states","Top 10 transactions_type based on states and transaction_amount"]
    select = st.selectbox("Select the option",options)
    if select=="Top 10 states based on year and amount of transaction":
        cursor.execute("SELECT DISTINCT State,Transaction_amount,Year,Quater FROM top_transaction GROUP BY State ORDER BY transaction_amount DESC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['State','Transaction_amount','Year','Quater'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 states based on type and amount of transaction")
            st.bar_chart(data=df,x="State",y="Transaction_amount")
    elif select=="Least 10 states based on type and amount of transaction":
        cursor.execute("SELECT DISTINCT State,Transaction_amount,Year,Quater FROM top_transaction GROUP BY State ORDER BY transaction_amount ASC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['State','Transaction_amount','Year','Quater'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Least 10 states based on type and amount of transaction")
            st.bar_chart(data=df,x="State",y="Transaction_amount")
    elif select=="Top 10 mobile brands based on percentage of transaction":
        cursor.execute("SELECT DISTINCT brands,Percentage FROM aggregated_user GROUP BY brands ORDER BY Percentage DESC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['brands','Percentage'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 mobile brands based on percentage of transaction")
            st.bar_chart(data=df,x="brands",y="Percentage")
    elif select=="Top 10 Registered-users based on States and District(pincode)":
        cursor.execute("SELECT DISTINCT State,District,RegisteredUser FROM top_user GROUP BY State,District ORDER BY RegisteredUser DESC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['State','District','RegisteredUser'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 Registered-users based on States and District(pincode)")
            st.bar_chart(data=df,x="State",y="RegisteredUser")
    elif select=="Top 10 Districts based on states and amount of transaction":
        cursor.execute("SELECT DISTINCT State,District,Transaction_amount FROM map_transaction GROUP BY State,District ORDER BY Transaction_amount DESC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['State','District','Transaction_amount'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 Districts based on states and amount of transaction")
            st.bar_chart(data=df,x="State",y="Transaction_amount")
    elif select=="Least 10 Districts based on states and amount of transaction":
        cursor.execute("SELECT DISTINCT State,District,Transaction_amount FROM map_transaction GROUP BY State,District ORDER BY Transaction_amount ASC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['State','District','Transaction_amount'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Least 10 Districts based on states and amount of transaction")
            st.bar_chart(data=df,x="State",y="Transaction_amount")
    elif select=="Least 10 registered-users based on Districts and states":
        cursor.execute("SELECT DISTINCT State,District,RegisteredUser FROM top_user GROUP BY State,District ORDER BY RegisteredUser ASC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['State','District','RegisteredUser'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Least 10 registered-users based on Districts and states")
            st.bar_chart(data=df,x="State",y="RegisteredUser")
    elif select=="Top 10 transactions_type based on states and transaction_amount":
        cursor.execute("SELECT DISTINCT State,Transaction_type,Transaction_amount FROM aggregated_transaction GROUP BY State,Transaction_type ORDER BY Transaction_amount DESC LIMIT 10");
        df = pd.DataFrame(cursor.fetchall(),columns=['State','Transaction_type','Transaction_amount'])
        col1,col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 transactions_type based on states and transaction_amount")
            st.bar_chart(data=df,x="State",y="Transaction_amount")

if SELECT == "Home":
    col1,col2, = st.columns(2)
    col1.image(Image.open("C:/Users/Admin/Pictures/phonepe/phonepe.png"),width = 500)
    with col1:
        st.subheader("PhonePe  is an Indian digital payments and financial technology company headquartered in Bengaluru, Karnataka, India. PhonePe was founded in December 2015, by Sameer Nigam, Rahul Chari and Burzin Engineer. The PhonePe app, based on the Unified Payments Interface (UPI), went live in August 2016. It is owned by Flipkart, a subsidiary of Walmart.")
        st.download_button("DOWNLOAD THE APP NOW", "https://www.phonepe.com/app-download/")
    with col2:
        st.video("C:/Users/Admin/Pictures/phonepe/upi.mp4")

if SELECT == "About":
    col1,col2 = st.columns(2)
    with col1:
        st.video("C:/Users/Admin/Pictures/phonepe/pulse-video.mp4")
    with col2:
        st.image(Image.open("C:/Users/Admin/Pictures/phonepe/PhonePe_Logo.jpg"),width = 600)
        st.write("---")
        st.subheader("The Indian digital payments story has truly captured the world's imagination."
                 " From the largest towns to the remotest villages, there is a payments revolution being driven by the penetration of mobile phones, mobile internet and state-of-the-art payments infrastructure built as Public Goods championed by the central bank and the government."
                 " Founded in December 2015, PhonePe has been a strong beneficiary of the API driven digitisation of payments in India. When we started, we were constantly looking for granular and definitive data sources on digital payments in India. "
                 "PhonePe Pulse is our way of giving back to the digital payments ecosystem.")
    st.write("---")
    col1,col2 = st.columns(2)
    with col1:
        st.title("THE BEAT OF PHONEPE")
        st.write("---")
        st.subheader("Phonepe became a leading digital payments company")
        st.image(Image.open("C:/Users/Admin/Pictures/phonepe/top.png"),width = 400)
        with open("C:/Users/Admin/Pictures/phonepe/annual report.pdf","rb") as f:
            data = f.read()
        st.download_button("DOWNLOAD REPORT",data,file_name="annual report.pdf")
    with col2:
        st.image(Image.open("C:/Users/Admin/Pictures/phonepe/report.jpeg"),width = 800)

if SELECT =="Search":
    Topic = ["","Transaction-Type","District","Brand","Top-Transactions","Registered-users"]
    choice_topic = st.selectbox("Search by",Topic)

#creating functions for query search in sqlite to get the data
    def type_(type):
        cursor.execute(f"SELECT DISTINCT State,Quater,Year,Transaction_type,Transaction_amount FROM aggregated_transaction WHERE Transaction_type = '{type}' ORDER BY State,Quater,Year");
        df = pd.DataFrame(cursor.fetchall(), columns=['State','Quater', 'Year', 'Transaction_type', 'Transaction_amount'])
        return df
    def type_year(year,type):
        cursor.execute(f"SELECT DISTINCT State,Year,Quater,Transaction_type,Transaction_amount FROM aggregated_transaction WHERE Year = '{year}' AND Transaction_type = '{type}' ORDER BY State,Quater,Year");
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year',"Quater", 'Transaction_type', 'Transaction_amount'])
        return df
    def type_state(state,year,type):
        cursor.execute(f"SELECT DISTINCT State,Year,Quater,Transaction_type,Transaction_amount FROM aggregated_transaction WHERE State = '{state}' AND Transaction_type = '{type}' And Year = '{year}' ORDER BY State,Quater,Year");
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year',"Quater", 'Transaction_type', 'Transaction_amount'])
        return df
    def district_choice_state(_state):
        cursor.execute(f"SELECT DISTINCT State,Year,Quater,District,amount FROM map_transaction WHERE State = '{_state}' ORDER BY State,Year,Quater,District");
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year',"Quater", 'District', 'amount'])
        return df
    def dist_year_state(year,_state):
        cursor.execute(f"SELECT DISTINCT State,Year,Quater,District,amount FROM map_transaction WHERE Year = '{year}' AND State = '{_state}' ORDER BY State,Year,Quater,District");
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year',"Quater", 'District', 'amount'])
        return df
    def district_year_state(_dist,year,_state):
        cursor.execute(f"SELECT DISTINCT State,Year,Quater,District,amount FROM map_transaction WHERE District = '{_dist}' AND State = '{_state}' AND Year = '{year}' ORDER BY State,Year,Quater,District");
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year',"Quater", 'District', 'amount'])
        return df
    def brand_(brand_type):
        cursor.execute(f"SELECT State,Year,Quater,brands,Percentage FROM aggregated_user WHERE brands='{brand_type}' ORDER BY State,Year,Quater,brands,Percentage DESC");
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year',"Quater", 'brands', 'Percentage'])
        return df
    def brand_year(brand_type,year):
        cursor.execute(f"SELECT State,Year,Quater,brands,Percentage FROM aggregated_user WHERE Year = '{year}' AND brands='{brand_type}' ORDER BY State,Year,Quater,brands,Percentage DESC");
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year',"Quater", 'brands', 'Percentage'])
        return df
    def brand_state(state,brand_type,year):
        cursor.execute(f"SELECT State,Year,Quater,brands,Percentage FROM aggregated_user WHERE State = '{state}' AND brands='{brand_type}' AND Year = '{year}' ORDER BY State,Year,Quater,brands,Percentage DESC");
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year',"Quater", 'brands', 'Percentage'])
        return df
    def transaction_state(_state):
        cursor.execute(f"SELECT State,Year,Quater,District,Transaction_count,Transaction_amount FROM top_transaction WHERE State = '{_state}' GROUP BY State,Year,Quater")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year',"Quater", 'District', 'Transaction_count', 'Transaction_amount'])
        return df
    def transaction_year(_state,_year):
        cursor.execute(f"SELECT State,Year,Quater,District,Transaction_count,Transaction_amount FROM top_transaction WHERE Year = '{_year}' AND State = '{_state}' GROUP BY State,Year,Quater")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year',"Quater", 'District', 'Transaction_count', 'Transaction_amount'])
        return df
    def transaction_quater(_state,_year,_quater):
        cursor.execute(f"SELECT State,Year,Quater,District,Transaction_count,Transaction_amount FROM top_transaction WHERE Year = '{_year}' AND Quater = '{_quater}' AND State = '{_state}' GROUP BY State,Year,Quater")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year',"Quater", 'District', 'Transaction_count', 'Transaction_amount'])
        return df
    def registered_user_state(_state):
        cursor.execute(f"SELECT State,Year,Quater,District,RegisteredUser FROM map_user WHERE State = '{_state}' ORDER BY State,Year,Quater,District")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year',"Quater", 'District', 'RegisteredUser'])
        return df
    def registered_user_year(_state,_year):
        cursor.execute(f"SELECT State,Year,Quater,District,RegisteredUser FROM map_user WHERE Year = '{_year}' AND State = '{_state}' ORDER BY State,Year,Quater,District")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year',"Quater", 'District', 'RegisteredUser'])
        return df
    def registered_user_district(_state,_year,_dist):
        cursor.execute(f"SELECT State,Year,Quater,District,RegisteredUser FROM map_user WHERE Year = '{_year}' AND State = '{_state}' AND District = '{_dist}' ORDER BY State,Year,Quater,District")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year',"Quater", 'District', 'RegisteredUser'])
        return df


    if choice_topic=="Transaction_Type":
        col1,col2,col3 = st.columns(3)
        with col1:
            st.subheader("-- 5 TYPES OF TRANSACTION --")
            transaction_type = st.selectbox("search by", ["Choose an option", "Peer-to-peer payments",
                                                          "Merchant payments", "Financial Services",
                                                          "Recharge & bill payments", "Others"], 0)
        with col2:
            st.subheader("-- 5 YEARS --")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader("-- 36 STATES --")
            menu_state = ["", 'uttar-pradesh', 'jharkhand', 'puducherry', 'rajasthan', 'odisha', 'nagaland',
                          'chandigarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'assam', 'haryana', 'jammu-&-kashmir',
                          'tamil-nadu', 'himachal-pradesh', 'ladakh', 'bihar', 'maharashtra', 'uttarakhand',
                          'karnataka', 'lakshadweep', 'andhra-pradesh', 'sikkim', 'madhya-pradesh', 'mizoram',
                          'kerala', 'manipur', 'arunachal-pradesh', 'andaman-&-nicobar-islands', 'delhi', 'tripura',
                          'chhattisgarh', 'meghalaya', 'goa', 'west-bengal', 'telangana', 'gujarat', 'punjab']
            choice_state = st.selectbox("State", menu_state, 0)

        if transaction_type:
            col1,col2,col3, = st.columns(3)
            with col1:
                st.subheader(f'{transaction_type}')
                st.write(type_(transaction_type))
        if transaction_type and choice_year:
            with col2:
                st.subheader(f' in {choice_year}')
                st.write(type_year(choice_year,transaction_type))
        if transaction_type and choice_state and choice_year:
            with col3:
                st.subheader(f' in {choice_state}')
                st.write(type_state(choice_state,choice_year,transaction_type))

    if choice_topic=="District":
        col1,col2,col3 = st.columns(3)
        with col1:
            st.subheader("-- 36 STATES --")
            menu_state = ["", 'uttar-pradesh', 'jharkhand', 'puducherry', 'rajasthan', 'odisha', 'nagaland',
                          'chandigarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'assam', 'haryana', 'jammu-&-kashmir',
                          'tamil-nadu', 'himachal-pradesh', 'ladakh', 'bihar', 'maharashtra', 'uttarakhand',
                          'karnataka', 'lakshadweep', 'andhra-pradesh', 'sikkim', 'madhya-pradesh', 'mizoram',
                          'kerala', 'manipur', 'arunachal-pradesh', 'andaman-&-nicobar-islands', 'delhi', 'tripura',
                          'chhattisgarh', 'meghalaya', 'goa', 'west-bengal', 'telangana', 'gujarat', 'punjab']
            choice_state = st.selectbox("State", menu_state, 0)
        with col2:
            st.subheader("-- 5 YEARS --")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader("-- SELECT DISTRICTS --")
            district = st.selectbox("search by", df_map_transaction["District"].unique().tolist())
        if choice_state:
            col1,col2,col3 = st.columns(3)
            with col1:
                st.subheader(f'{choice_state}')
                st.write(district_choice_state(choice_state))
        if choice_year and choice_state:
            with col2:
                st.subheader(f'in {choice_year} ')
                st.write(dist_year_state(choice_year,choice_state))
        if district and choice_state and choice_year:
            with col3:
                st.subheader(f'in {district} ')
                st.write(district_year_state(district,choice_year,choice_state))

    if choice_topic=="Brand":
        col1,col2,col3 = st.columns(3)
        with col1:
            st.subheader("-- TYPES OF BRANDS --")
            mobiles = ["",'Xiaomi', 'Vivo', 'Samsung', 'Oppo', 'Realme', 'Apple', 'Huawei', 'Motorola', 'Tecno', 'Infinix',
                       'Lenovo', 'Lava', 'OnePlus', 'Micromax', 'Asus', 'Gionee', 'HMD Global', 'COOLPAD', 'Lyf',
                       'Others']
            brand_type = st.selectbox("search by",mobiles, 0)
        with col2:
            st.subheader("-- 5 YEARS --")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader("-- 36 STATES --")
            menu_state = ["", 'uttar-pradesh', 'jharkhand', 'puducherry', 'rajasthan', 'odisha', 'nagaland',
                          'chandigarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'assam', 'haryana', 'jammu-&-kashmir',
                          'tamil-nadu', 'himachal-pradesh', 'ladakh', 'bihar', 'maharashtra', 'uttarakhand',
                          'karnataka', 'lakshadweep', 'andhra-pradesh', 'sikkim', 'madhya-pradesh', 'mizoram',
                          'kerala', 'manipur', 'arunachal-pradesh', 'andaman-&-nicobar-islands', 'delhi', 'tripura',
                          'chhattisgarh', 'meghalaya', 'goa', 'west-bengal', 'telangana', 'gujarat', 'punjab']
            choice_state = st.selectbox("State", menu_state, 0)

        if brand_type:
            col1,col2,col3, = st.columns(3)
            with col1:
                st.subheader(f'{brand_type}')
                st.write(brand_(brand_type))
        if brand_type and choice_year:
            with col2:
                st.subheader(f' in {choice_year}')
                st.write(brand_year(brand_type,choice_year))
        if brand_type and choice_state and choice_year:
            with col3:
                st.subheader(f' in {choice_state}')
                st.write(brand_state(choice_state,brand_type,choice_year))

    if choice_topic=="Top-Transactions":
        col1,col2,col3 = st.columns(3)
        with col1:
            st.subheader("-- 36 STATES --")
            menu_state = ["", 'uttar-pradesh', 'jharkhand', 'puducherry', 'rajasthan', 'odisha', 'nagaland',
                          'chandigarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'assam', 'haryana', 'jammu-&-kashmir',
                          'tamil-nadu', 'himachal-pradesh', 'ladakh', 'bihar', 'maharashtra', 'uttarakhand',
                          'karnataka', 'lakshadweep', 'andhra-pradesh', 'sikkim', 'madhya-pradesh', 'mizoram',
                          'kerala', 'manipur', 'arunachal-pradesh', 'andaman-&-nicobar-islands', 'delhi', 'tripura',
                          'chhattisgarh', 'meghalaya', 'goa', 'west-bengal', 'telangana', 'gujarat', 'punjab']
            choice_state = st.selectbox("State", menu_state, 0)
        with col2:
            st.subheader("-- 5 YEARS --")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader("--4 Quaters --")
            menu_quater = ["", "1", "2", "3", "4"]
            choice_quater = st.selectbox("Quater", menu_quater, 0)

        if choice_state:
            with col1:
                st.subheader(f'{choice_state}')
                st.write(transaction_state(choice_state))
        if choice_state and choice_year:
            with col2:
                st.subheader(f'{choice_year}')
                st.write(transaction_year(choice_state,choice_year))
        if choice_state and choice_quater:
            with col3:
                st.subheader(f'{choice_quater}')
                st.write(transaction_quater(choice_state,choice_year,choice_quater))

    if choice_topic=="Registered-users":
        col1,col2,col3 = st.columns(3)
        with col1:
            st.subheader("-- 36 STATES --")
            menu_state = ["", 'uttar-pradesh', 'jharkhand', 'puducherry', 'rajasthan', 'odisha', 'nagaland',
                          'chandigarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'assam', 'haryana', 'jammu-&-kashmir',
                          'tamil-nadu', 'himachal-pradesh', 'ladakh', 'bihar', 'maharashtra', 'uttarakhand',
                          'karnataka', 'lakshadweep', 'andhra-pradesh', 'sikkim', 'madhya-pradesh', 'mizoram',
                          'kerala', 'manipur', 'arunachal-pradesh', 'andaman-&-nicobar-islands', 'delhi', 'tripura',
                          'chhattisgarh', 'meghalaya', 'goa', 'west-bengal', 'telangana', 'gujarat', 'punjab']
            choice_state = st.selectbox("State", menu_state, 0)
        with col2:
            st.subheader("-- 5 YEARS --")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader("-- SELECT DISTRICTS --")
            district = st.selectbox("search by", df_map_transaction["District"].unique().tolist())

        if choice_state:
            with col1:
                st.subheader(f'{choice_state}')
                st.write(registered_user_state(choice_state))
        if choice_state and choice_year:
            with col2:
                st.subheader(f'{choice_year}')
                st.write(registered_user_year(choice_state,choice_year))
        if choice_state and choice_year and district:
            with col3:
                st.subheader(f'{district}')
                st.write(registered_user_district(choice_state,choice_year,district))