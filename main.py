import psycopg2
import matplotlib.pyplot as plt

class Quote():
    """Klasse in der die Quote-Daten analysiert werden und die Tradingstrategie beinhaltet und Signale erkennt (Algorithmus)"""
    
    def __init__(self) -> None:
        '''Definition aller benötigten Variablen (Anfangswerte wurden auf 1000 gesetzt um beim ersten Durchlauf keine Strategieparameter zu triggern)'''

        #Variablen für die momentanen Werte 
        #Geldkurs (bid-price)
        self.bid = 1000
        #Briefkurs (ask-price)
        self.ask = 1000
        #Preisunterschied (spread)
        self.spread = 1000

        #bid und ask Volumen
        self.bv = 1000
        self.av = 1000

        #Durchschnittlicher Preis aus Geld- und Briefkurs
        self.mid_price = 1000

        #Variablen für vorherige Werte aus dem Datensatz 
        self.prev_bid = 1000
        self.prev_ask = 1000
        self.prev_spread = 1000
        self.prev_bv = 1000
        self.prev_av = 1000
    
        #Zeitstempel
        self.time = 0

        #Varbialen zur Berechnung der Tradingstrategie
        self.order_imbalance = 0
        self.delta_bv = 0
        self.delta_av = 0

        #letzter gekaufter Kurs, für Stop/Loss und Gewinnmitnahme  
        self.last_buy_price = 1000

    def update(self, data):
        '''Methode zur Aktualiserung aller wichitgen Variablen

        :param data: Dictionary mit allen neuen Daten
        :type: dict'''
        
        #Momentane Werte, werden zu vorherigen Werten
        self.prev_bid = self.bid
        self.prev_ask = self.ask
        self.prev_bv = self.bv
        self.prev_av = self.av 
        
        #Neue Werte, werden mit den Daten aus der Datenbank definiert
        self.bid = data['bid_price']  
        self.ask = data['ask_price']   
        self.time = data['timestamp']
        self.bv = data['bv']
        self.av = data['av']

        #Berechnung der Spreads
        self.prev_spread = round(self.ask - self.bid, 5)
        self.spread = round(data['ask_price']   - data['bid_price']  , 5)

    def check(self, data):
        '''Methode welche erstige Überprüfung auf ein mögliches Kursmomentum vornimmt

        :param data: Dictionary mit allen neuen Daten
        :type: dict'''

        #Aktualisierung aller Daten
        self.update(data)
        
        #Vorherige und momentane Geldkurse werden untersucht und die daraus resultierenden Werte für das delta-bid-volumen berechnet
        if self.bid > self.prev_bid:
            self.delta_bv = self.bv

        elif self.bid == self.prev_bid:
            self.delta_bv = self.bv - self.prev_bv
        
        elif self.bid < self.prev_bid:
            self.delta_bv = 0
        
        #Vorherige und momentane Briefkurse werden untersucht und die daraus resultierenden Werte für das delta-ask-volumen berechnet
        if self.ask < self.prev_ask:
            self.delta_av = self.av

        elif self.ask == self.prev_ask:
            self.delta_av = self.av - self.prev_av 
        
        elif self.ask > self.prev_ask:
            self.delta_av = 0
  
 
            
    def calculation(self, data_bp, data_ap):
        '''Methode zur Berechnung aller Strategieparameter und Kauf- oder Verkaufsentscheidung

        :param data_bp: die Briefkurse der letzen fünf Zeitstempel
        :type: list
        :param data_ap: die Geldkurse der letzen fünf Zeitstempel
        :type: list
        '''

        #Berechnung der Volume-Order-Imbalance (VOI), bei postiven Werten ein Kaufsignal und vice versa
        self.order_imbalance = self.delta_bv - self.delta_av


        #Erstellung der Variable, um die Summe der mittleren Preise der Geld- und Briefkurse (mid price) zu berechnen
        sum_mid_price = 0
        
        
        for x in range(len(data_bp)):
            
            #Berechnung des momentanen mid price 
            mid_price = (data_bp[x] + data_ap[x]) / 2

            #Aufaddieren der mid prices
            sum_mid_price += mid_price

        #Berechnung des durchschnittlichen mid price der letzten fünf Ticks
        mean_mid_price = sum_mid_price / len(data_bp)

        #Berechnung des momentan durchschnittlichen mittleren Preises der Geld- und Briefkurse
        cur_mid_price = (self.bid + self.ask) / 2
        
        #Kaufsignal laut Order Imbalance (OI) Indikator
        if self.order_imbalance > 0:
            #print(mean_mid_price - cur_mid_price)
            
            #Überprüfung weiterer Indikatoren
            if (mean_mid_price - cur_mid_price) >= 0.1: #der durchschnittlicher mittlerer Preis ist mind. um 0.1 größer als der Momentane
                #print('2')
                if self.spread <= 0.2: #der Spread ist kleiner oder gleich 0.2, bedeutet höhere Volatilität
                    self.last_buy_price = self.ask
                    
                    #Wenn alle dieser Parameter erfüllt sind werden die ask und bid Preis Daten zurückgegeben
                    return self.__params_for_order(True)

        #Gewinnmitnahme, wenn der Preis 2% gegenüber des gekauften Kurses gestiegen ist 
        elif (self.bid / self.last_buy_price) > 1.02:
            return self.__params_for_order(False)
            

        #Stopp loss, bei mehr als -0.5% Verlust
        elif (self.bid / self.last_buy_price) < 0.995:
            return self.__params_for_order(False)
        
        
        #Verkaufsignal laut Indikatoren
        elif self.order_imbalance < 0:
            if mean_mid_price < cur_mid_price:
                if self.spread <= 0.2:
                    return self.__params_for_order(False)
        
    
    def __params_for_order(self, order):
        '''Methode zur Ausgabe der wichtigen Datenwerte für die Order
        :param order: True oder False, True bei Kauforder und False bei Verkaufsorder
        :type: bool'''
        return self.ask, self.bid, order, self.time


class Balance():
    '''Klasse zur Order- und Kontosimulation'''
    def __init__(self) -> None:
        #Variablen zur Kontosimulation und der Dokumentation der Orders 
        self.budget = 1000000
        
        self.sell_trades = []
        self.buy_trades = []
        self.prices_at_sell = []
        self.prices_at_buy = []
        self.sell_trades_bid = []

        self.trades = 0
        self.shares = 0

    def action(self, order):
        '''Methode, welche die Kauf- oder Verkaufsmethode aktiviert
        :param order: Informationen um Order zu tätigen, ask und bid Preis, Orderbefehl und Zeitstempel
        :type: tuple'''
        
        #Überprüfung, ob die calculation-Methode der Klasse Quote überhaupt einen Befehl zur Order übermittelt wurde
        if order != None:
            
            #Überfrüfung, ob Kauf- oder Verkaufsbefehl
            if order[2] == True:
                self.buy(order)
            else:
                self.sell(order)

    def shares_check(self):
        '''Methode, ob es ggf. bereits innehabende Aktien gibt'''
        
        if self.shares == 0:
            return True
        else:
            return False
    
    def buy(self, params):
        '''Methode für Kauforder
        :param params: Informationen um Order zu tätigen, ask und bid Preis, Orderbefehl und Zeitstempel
        :type: tuple'''
        
        #Wenn shares_check = True dann werden Aktien gekauft andernfalls nicht
        if self.shares_check() == True:
            price = float(params[0])

            #Berechnung wie viele Aktien ich mit 10% meines Gesamtbudget kaufen kann 
            shares_amount = round((self.budget*0.1)/price)

            #Abbuchung in Höhe der gekauften Aktien
            self.budget = self.budget - (shares_amount*price)

            #Vermerk, dass ich Aktien besitze
            self.shares += shares_amount

            #Hinzufügen des Zeitstempel in die Liste, in dem alle Kauforder aufgeführt werden
            self.buy_trades.append(str(params[3]))
            #Hinzufügen des Kaufpreises in die Liste, in dem alle Kaufpreise aufgeführt werden
            self.prices_at_buy.append(price)
            #Inkrementierung der trades Variable, um die Gesamtanzahl der Trades zu dokumentieren
            self.trades += 1

    def sell(self, params):
        '''Methode für Verkaufsorder
        :param params: Informationen um Order zu tätigen, ask und bid Preis, Orderbefehl und Zeitstempel
        :type: tuple'''
        
        #Wenn shares_check = False dann werden Aktien verkauft andernfalls nicht
        if self.shares_check() == False:
            price = float(params[1])

            #Berechnung der neuen Bilanz
            self.budget = self.budget + (self.shares*price)

            #Besitz der Aktien werden auf Null gesetzt 
            self.shares = 0
            #Hinzufügen des Zeitstempel in die Liste, in dem alle Verkaufsorder aufgeführt werden
            self.sell_trades.append(str(params[3]))
            
            #Hinzufügen des Verkaufspreises in die Liste, in dem alle Verkaufspreise aufgeführt werden 
            #einmal aks für das Diagramm und einmal der bid für die Erfolgsberechnung
            self.prices_at_sell.append(float(params[0]))
            self.sell_trades_bid.append(float(params[1]))
            #Inkrementierung der trades Variable, um die Gesamtanzahl der Trades zu dokumentieren
            self.trades += 1
    
    def cur_balance(self, cur_price):
        '''Ausgabe der theoretischen momentanen Bilanz, wenn alle Aktien liquidiert werden
        :param cur_price: momentaner Verkaufspreis
        :type: float'''
        #Berechnung und Ausgabe der momentanen Bilanz
        budget = self.budget + self.shares*float(cur_price)
        return budget

    def end_balance(self, cur_price):
        '''Methode, um den Endstand des Budget zu bekommen und alle Aktien zu verkaufen
        :param cur_price: momentaner Verkaufspreis
        :type: float'''

        #Berechnung des endgültigen Kontostands, nach der Liquidation Aktien 
        self.budget += self.shares*float(cur_price)
        #Ausgabe des momentanen 'Kontostands', der Anzahl aller Verkaufs- und Kauftrades und die Gesmatanzahl aller Trades
        return self.budget, len(self.sell_trades), len(self.buy_trades)

#Verbindung zur Datenbank
connection = psycopg2.connect(
host="localhost",
database="Seminararbeit",
user="postgres",
password="X",
port=X)

#Erzeuge Cursor um SQL-Befehle auszuführen
cursor = connection.cursor()

#Ausführen einer SQL-query
cursor.execute("SELECT version();")

#Ergebnisse einholen
record = cursor.fetchone()
print("You are connected to - ", record, "\n")

#Erzeugung von jeweils einer Instanz der Quote und Balance Klasse 
Q = Quote()
B = Balance()

#Liste zur Dokumentation aller Kontostände über den Zeitverlauf 
balance_list = []
#Variable für verwendetes Datenbank Table 
table = 'stock_data3'
#Einträge in dem jeweiligen Table
entires = 27000
#Schleife um alle Einträge der Datenbank durchzugehen
for i in range(entires):

    #SQl-Query für die primäre Datenreihe mit der id = i
    cursor.execute(f"SELECT bv, av, bp, ap, time_stamp FROM {table} WHERE id = {i+1}")
    data = cursor.fetchall()
    #Umwandlung der Daten in ein Dictionary
    for row in data:
        data_dict = {
            'bv': row[0],
            'av': row[1],
            'bid_price': row[2],
            'ask_price': row[3],
            'timestamp': row[4]
        }
    
    #SQL-Query um die letzten fünf ask Preise einzuholen zur Berechnung des mid_prices
    cursor.execute(f"SELECT ap FROM {table} WHERE id BETWEEN {i+1} AND {i+5}")
    data_ap = cursor.fetchall()
    data_ap_list = []

    #Speicherung der Daten in einer Liste 
    for row in data_ap:
        data_ap_list.append(row[0])

    #SQL-Query um die letzten fünf bid Preise einzuholen zur Berechnung des mean_mid_prices
    cursor.execute(f"SELECT bp FROM {table} WHERE id BETWEEN {i+1} AND {i+5}")
    data_bp = cursor.fetchall()
    data_bp_list = []
    for row in data_bp:
        data_bp_list.append(row[0])

    #Überführung des primären Datenpackets in die check-Methode
    Q.check(data_dict)

    #Weitergabe der beiden anderen Datenlisten in die calculation-Methode der Quote Klasse, 
    #welche wiederum die Orderinformation für action-Methode der Balance Klasse bilden
    B.action(Q.calculation(data_bp_list, data_ap_list))

    #Den momentanen Kontostand in die balance_list eintragen
    balance_list.append(B.cur_balance(Q.bid))

#Ausgabe des endgültigen Kontostands
print(B.end_balance(Q.bid))

#Definierung der Listen mit den Werten aus der Klasse
sell_trades_list = B.sell_trades
buy_trades_list = B.buy_trades

price_at_sell = B.prices_at_sell
price_at_sell_bid = B.sell_trades_bid
price_at_buy = B.prices_at_buy


def trade_results():
    '''Funktion um die Anzahl der positiven und negativen Trades festzustellen'''
    i = 0
    num_pos_trades = 0
    num_neg_trades = 0
    #Für jeden Kaufpreis wird überprüft, ob der darauffolgende Verkaufspreis größer ist
    for x in price_at_buy:
        
        if x > price_at_sell_bid[i]:
            #Ist er größer, war der Trade profitabel
            num_neg_trades += 1
        else:
            #Ist er kleiner, war der Trade nicht profitabel
            num_pos_trades += 1

        i+=1
    return num_pos_trades, num_neg_trades

#Beginn der grafischen Auswertung des Tradingtags

#Erzuegung zwei Listen, welche die x- und y-Achse darstellen
price_list = []
time_list = []

#Einholung aller ask Preise und die dazugehörigen Zeitstempel
for i in range(entires): #stock_data1 11870 stockdata_2 19644

    cursor.execute(f"SELECT bv, av, bp, ap, time_stamp FROM {table} WHERE id = {i+1}")
    data = cursor.fetchall()
    for row in data:
        price_list.append(float(row[3]))
        time_list.append(str(row[4]))

        
#Name der Firma der gehandelten Aktien
company = 'Apple'

r = len(time_list)
#Festlegung der angezeigten Werte für die x-Achse
xticks = [0, round(r/4, 1), round(r/2, 1), round(r*0.75, 1), r-80]

fig, ax1 = plt.subplots()

#Erstellung zweiter y-Achse
ax2 = ax1.twinx()

ax1.set_xticks(xticks)

#Plotten der Achsen und Punkte 
ax1.plot(time_list, price_list, color="black", label=f'{company} Kurs')
ax2.plot(balance_list, color="blue", label=f"Budget \n{trade_results()[1]} Trades mit Verlust \n{trade_results()[0]} Trades mit Gewinn")
ax1.scatter(sell_trades_list, price_at_sell, color='red', s=70, alpha=1, label=f'Sell trades')
ax1.scatter(buy_trades_list, price_at_buy, color ='green', s=70, alpha=1, label=f'Buy trades')

#Namensgebung der Achsen
plt.title(f"Trading Test")
ax1.set_xlabel("Zeit", fontsize=15)
ax1.set_ylabel("Aktienpreis", fontsize=15)
ax2.set_ylabel("Kontostand", fontsize=15)

print(f'{trade_results()[1]} Trades mit Verlust \n{trade_results()[0]}')
#Positionierung der Legenden
ax1.legend(loc='center right', framealpha = 0.9)
ax2.legend(loc='upper right',framealpha = 0.9)
plt.show()
