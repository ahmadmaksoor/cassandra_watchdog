# -*- coding: utf-8 -*-
"""
Created on Wed Jun 30 14:39:28 2021

@author: ahmad almaksour
"""


from datetime import datetime,timedelta
import pytz
from apscheduler.schedulers.background import BlockingScheduler 
import logging

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import pandas as pd

def updating_script():
#%% connection to cassandra database 
    db_url = "10.64.253."
    username = ""
    password = "p@" 

    if ',' in db_url:
        db_url= db_url.split(',')
    else :    
        db_url = [db_url]
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    cluster = Cluster(db_url, auth_provider=auth_provider, connect_timeout=20)

    session = cluster.connect()
    session.default_fetch_size = None

    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)
#%% get the data from cassandra database and verify if there are an interruption or errors in the columns
    def get_data_from_cassandra(table):
        coupure=""
        date_last_mins = datetime.utcnow() - timedelta(minutes=20) #actual date minus 20 minutes (get the data of last 20mins)
        date =date_last_mins.strftime('%Y-%m-%d %H:%M')
        session.set_keyspace('previsions_data')
        session.row_factory = pandas_factory
        session.default_fetch_size = None
        query = '''SELECT * FROM "'''+table+'''"where "Date" >= %s ALLOW FILTERING '''
        rslt = session.execute(query,(date,), timeout=None)
        global df
        df = rslt._current_rows
        if 'name' in df.columns and 'Date' in df.columns : #eliminer les colonnes 'Date' et 'name' qui seront jamais erronées
            df=df.drop(['name','Date'],axis=1)
        elif 'Date' in df.columns:
            df=df.drop('Date',axis=1)
        elif 'name' in df.columns:
            df=df.drop('name',axis=1)
                
        l_bool1=df.isin([9999991]).all(axis=0).values # Vérifier les colonnes erronées par 9999991
        
        l_bool2=df.isin([9999999]).all(axis=0).values # Vérifier les colonnes erronées par 9999999 
    
        result1= all(elem == True for elem in l_bool1)
        
        result2= all(elem == True for elem in l_bool2)
        
        if result1 or result2 or df.empty: # si toutes les colonnes sont erronées ou on n'a pas de données 
            coupure="coupure"
            
        else :
            #print("pas_copure")
            coupure="pas_coupure"
        
        if (table == "equipment_data_ICAM_rough"):
            
            l_bool_tesla3=df[['Ptot_ICAM_MI','Ptot_ICAM_Ond_1','Ptot_ICAM_Ond_2','Ptot_ICAM_Transfo_400KVA']].isin([9999991]).all(axis=0).values #pour le cas de tesla
            
            df1 = df.drop(['Ptot_ICAM_MI','Ptot_ICAM_Ond_1','Ptot_ICAM_Ond_2','Ptot_ICAM_Transfo_400KVA'],axis=1)
            l_bool_tesla1=df1.isin([9999991]).all(axis=0).values
            l_bool_tesla2=df1.isin([9999999]).all(axis=0).values
            
            result_tesla1= all(elem == True for elem in l_bool_tesla1)
            
            result_tesla2= all(elem == True for elem in l_bool_tesla2)
            
            result_tesla3= all(elem == False for elem in l_bool_tesla3)
    
            if result_tesla1 or result_tesla2 and result_tesla3:
                coupure="coupure_tesla"
        

        return coupure
#%% send email to notify the people concerned
    def send_email(mail_content):
    
            #The mail addresses and password
            sender_address = 'sender@gmail.com'
            sender_pass = ''
            receiver_address = ['reciever@gmail.com']
            #Setup the MIME
            message = MIMEMultipart()
            message['From'] = sender_address
            message['To'] = ",".join(receiver_address)
            message['Subject'] = 'Notification coupure'   #The subject line
            #The body and the attachments for the mail
            message.attach(MIMEText(mail_content, 'html'))
            #Create SMTP session for sending the mail
            session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
            session.starttls() #enable security
            session.login(sender_address, sender_pass) #login with mail_id and password
            text = message.as_string()
            session.sendmail(sender_address, receiver_address, text)
            session.quit()
            print('Mail Sent')

#%% send an email in the case of an interruption 
    def update(table):
        with open(f'{table}.txt') as f :
            error_date= f.read()
        data_cassandra= get_data_from_cassandra(table)
        if data_cassandra == 'coupure' or data_cassandra == 'coupure_tesla': 
            if error_date == "":
                with open(f'{table}.txt','w') as f :
                    error_date= f.write( datetime.now(pytz.timezone("Europe/Paris")).strftime("%d-%m-%Y %H:%M"))
                if data_cassandra == 'coupure':
                    send_email( f'{datetime.now(pytz.timezone("Europe/Paris")).strftime("%d-%m-%Y %H:%M")}: Il y a eu une coupure <b>Totale</b> des données dans le tableau {table} .')
                    
                elif data_cassandra == 'coupure_tesla':
                    send_email( f'{datetime.now(pytz.timezone("Europe/Paris")).strftime("%d-%m-%Y %H:%M")}: Il y a eu une coupure des données de la <b>Tesla</b> dans le tableau {table} .')
                
            else : 
                if datetime.now(pytz.timezone("Europe/Paris")).replace(tzinfo=None) > (datetime.strptime(error_date,"%d-%m-%Y %H:%M") + timedelta(hours=6)) : #rappel dans 6 heures
                    with open(f'{table}.txt','w') as f :
                        error_date= f.write( datetime.now(pytz.timezone("Europe/Paris")).strftime("%d-%m-%Y %H:%M"))
                        
                    send_email(f'rappel : Il y a toujours une coupure dans le tableau {table}.')
                    
        elif data_cassandra == 'pas_coupure' and  error_date !="" :
            with open(f'{table}.txt','w') as f :
                error_date = f.write("")
            send_email(f'plus de coupure dans le tableau {table} .')
           

    update("equipment_data_rough")
    update("equipment_data_ICAM_rough")

    cluster.shutdown()

#%% Main 

logging.basicConfig(level=logging.ERROR, filename='./logs/cassandra_watchdog_logs.log')

scheduler = BlockingScheduler()
scheduler.add_job(updating_script, 'interval',minutes=20,next_run_time=datetime.utcnow())
scheduler.start()
