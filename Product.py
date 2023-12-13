from confluent_kafka import Consumer, KafkaException,Producer
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
consumer.subscribe(['selling'])

# Configuration du producteur Kafka
configProd = {
    'bootstrap.servers': 'localhost:9092',  # Adresse du serveur Kafka
}

# Créer un producteur
producer = Producer(configProd)

# Envoyer un message à l'étape de vente
def delivery_report(err, msg):
    if err is not None:
        print('Erreur de livraison : {}'.format(err))
    else:
        print('Message livré : {}'.format(msg.value().decode('utf-8')))

#Temps de construction 
TempsConstruction = {'M': 10, 'I': 2, 'A': 3, 'G': 4, 'E': 7}

# Consommer les messages de livraison
try:
    while True:
        msg = consumer.poll(timeout=1000)  # Timeout en millisecondes
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
        #On construit la voiture
        time.sleep(TempsConstruction[msg.value()])
        #On envoie pour Livraison
        producer.produce('Delivery', key='ready', value=msg.value(), callback=delivery_report)
        # Attendre la livraison des messages
        producer.flush()


finally:
    # Fermer le consommateur
    consumer.close()