import random
from confluent_kafka import Producer,KafkaException,Consumer
import time

# Configuration du consommateur Kafka
config = {
    'bootstrap.servers': 'localhost:9092',  # Adresse du serveur Kafka
    'group.id': 'my_consumer_group',         # Identifiant du groupe de consommateurs
    'auto.offset.reset': 'earliest'          # Débuter la consommation depuis le début des sujets
}

# Créer un consommateur
consumer = Consumer(config)

# S'abonner au sujet de livraison
consumer.subscribe(['delivered'])

# Configuration du producteur Kafka
configBis = {
    'bootstrap.servers': 'localhost:9092',  # Adresse du serveur Kafka
}

# Créer un producteur
producer = Producer(configBis)

# Envoyer un message à l'étape de vente
def delivery_report(err, msg):
    if err is not None:
        print('Erreur de livraison : {}'.format(err))
    else:
        print('Message livré : {}'.format(msg.value().decode('utf-8')))


#Les différents Modeles
listeModele = ["M","I","A","G","E"]

while True :

    random_index = random.randint(0,len(listeModele)-1)

    # Simuler un message de vente
    producer.produce('selling', key='carSold', value=listeModele[random_index], callback=delivery_report)

    # Attendre la livraison des messages
    producer.flush()
    print("Vente d'une modele "+str(listeModele[random_index]))
    time.sleep(4)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaException._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    # Afficher le message de livraison
    print('Message reçu: {}'.format(msg.value().decode('utf-8')))
