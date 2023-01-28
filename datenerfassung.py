import time
import requests
import psycopg2
from psycopg2 import Error
data_list = []
i = 1
try:
    #Verbindung zur Datenbank
    connection = psycopg2.connect(
    host="localhost",
    database="",
    user="",
    password="",
    port=)

    #Erzeuge Cursor um SQL-Befehle auszuführen
    cursor = connection.cursor()
    
    #Ausführen einer SQL-query
    cursor.execute("SELECT version();")
    
    #Ergbnisse einholen
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")
    
    #Vorlage für Daten-query
    insert_query = f""" INSERT INTO stock_data5 (id, bv, av, bp, ap, time_stamp) VALUES (%s, %s, %s, %s, %s, %s)"""
    
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)



class Dublicates():
    '''Prüft Daten auf gleiche Zeistempel'''
    
    def __init__(self) -> None:
        self.time = ""
    
    def set(self, time_stamp):
        '''Setzt den momentanen Zeitstempel'''
        self.time = time_stamp
    
    def check(self):
        '''Gibt den gespeicherten Zeitstempel aus'''
        return self.time

#Erstellung einer Instanz
d = Dublicates()

#Start der Datenerfassung
while True:
    try:
        #API-Anfrage an die lemon markets GmbH
        request = requests.get("https://data.lemon.markets/v1/quotes/latest?isin=US0378331005",
            headers={"Authorization": "X"})
        
        #Timer um das technische Anfragelimit der API nicht zu übersteigen
        time.sleep(0.5)
    
    except (Exception, Error) as error:
        print("Error while connecting to lemon markets: ", error)
        time.sleep(0.5)
        pass

    
    try:
        #Konvertierung der API-Daten in JSON-Format
        raw_data = request.json()
        raw_data = raw_data['results'][0]
    
        #Überprüung auf identische Quotes
        if raw_data['t'] == d.check():
            print('Same time stamp')
       
       #Speicherung der benötigten Inforamtionen auf der Datenbank
        else:
            data_tuple = (i, raw_data['b_v'], raw_data['a_v'], raw_data['b'], raw_data['a'], raw_data['t'])
            cursor.execute(insert_query, data_tuple)
            connection.commit()
            i +=1
       
        d.set(raw_data['t']) 
    
    except (Exception, Error) as error:
        print("Error with json conversion", error, str(raw_data))
    
    print(i)
