# phonepepulse
#Using Phonepe pulse data and analyzing the data
!git clone https://github.com/PhonePe/pulse.git

#Once created the clone of GIT-HUB repository then,
#Required libraries for the program

import pandas as pd
import json
import os

#This is to direct the path to get the data as states

path="/content/pulse/data/aggregated/transaction/country/india/state/"
Agg_state_list=os.listdir(path)
Agg_state_list
#Agg_state_list--> to get the list of states in India


#This is to extract the data's to create a dataframe

clm={'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}
for i in Agg_state_list:
    p_i=path+i+"/"
    Agg_yr=os.listdir(p_i)    
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)        
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              clm['Transacion_type'].append(Name)
              clm['Transacion_count'].append(count)
              clm['Transacion_amount'].append(amount)
              clm['State'].append(i)
              clm['Year'].append(j)
              clm['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
pd.DataFrame(clm)


# insert the dataframe into a sqlite database

import sqlite3
conn = sqlite3.connect('pulse.db')
c = conn.cursor()

#create a table in the database
c.execute('''CREATE TABLE IF NOT EXISTS transaction_data(
            State TEXT,
            Year TEXT,
            Quater TEXT,
            Transacion_type TEXT,
            Transacion_count TEXT,
            Transacion_amount TEXT
            )''')
            
#insert the All data into the table
c.executemany('INSERT INTO transaction_data VALUES(?,?,?,?,?,?)', zip(clm['State'], clm['Year'], clm['Quater'], clm['Transacion_type'], clm['Transacion_count'], clm['Transacion_amount']))


#show the data in the table
x=c.execute("SELECT * FROM transaction_data")
x=c.fetchall()
for row in x:
    print(row)
    
#fetch the data from the table where the state is Tamil Nadu and year is 2018 and quater is 1 and transaction type is Recharge and bill payments

d=c.execute("SELECT * FROM transaction_data WHERE State='tamil-nadu' AND Year='2018' AND Quater='1' AND Transacion_type='Recharge & bill payments' AND Transacion_amount>0")
d=c.fetchall()
for row in d:
  print(row)
#output - ('tamil-nadu', '2018', '1', 'Recharge & bill payments', '3308548', '646028338.395153')
