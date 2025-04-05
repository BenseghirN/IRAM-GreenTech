import adafruit_dht
import board
import time
import csv
from datetime import datetime
import RPi.GPIO as GPIO
import psycopg

# === CONFIGURATION GPIO ===
GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)

# Pins
LED_ROUGE = 20
LED_JAUNE = 16
LED_VERTE = 13
LED_BLEUE = 19
VENTILATEUR = 26
DHT_PIN = board.D21

# Setup GPIO
for pin in [LED_ROUGE, LED_JAUNE, LED_VERTE, LED_BLEUE, VENTILATEUR]:
    GPIO.setup(pin, GPIO.OUT)

# Capteur
dhtDevice = adafruit_dht.DHT22(DHT_PIN, use_pulseio=False)

import os

def create_new_csv():
    # Génère un nom de fichier avec le préfixe du groupe + date et heure
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"E{now}.csv"
    filepath = f"/data/records/{filename}"
    csv_file = "/data/records/donnees_dht.csv"
    # Crée le fichier et écrit l'en-tête
    with open(filepath, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "rlog_filename", "rlog_grp", "rlog_id", "rlog_timestamp", "rlog_temp",
            "rlog_temp_sensor", "rlog_temp_error",
            "rlog_hum", "rlog_hum_sensor", "rlog_hum_error"
        ])
    print(f"📁 Nouveau fichier CSV créé : {filepath}")
    return filepath, filename

# Connexion BDD
DB_HOST = "172.20.2.200"
DB_PORT = "5432"
DB_NAME = "tbl_log_raspberry"
DB_USER = "jpo2025"
DB_PASS = "jpo2025"

def call_stored_procedure(csv_path):
    try:
        conn = psycopg.connect(
            host=DB_HOST, port=DB_PORT,
            database=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cursor = conn.cursor()
        cursor.callproc('load_csv', [csv_path])
        conn.commit()
        cursor.close()
        conn.close()
        print(f" Données envoyées via procédure : {csv_path}")
    except psycopg.DatabaseError as e:
        print(f"[Erreur BDD] {e}")
    except Exception as e:
        print(f"[Erreur générale] {e}")

# Variables fixes
rlog_grp = 'E'
rlog_id = 5
rlog_temp_sensor = 'V'
rlog_hum_sensor = 'V'

# === BOUCLE ===
print(" Démarrage du contrôle (Ctrl+C pour arrêter)")

try:
    while True:
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rlog_filename = 'E'+timestamp
            try:
                temperature_c = dhtDevice.temperature
                humidity = dhtDevice.humidity
                temp_error = ""
                hum_error = ""
            except RuntimeError as sensor_error:
                temperature_c = 0.0
                humidity = 0.0
                temp_error = "Erreur lecture T°"
                hum_error = "Erreur lecture % Hum"
                print(f"[ Capteur] {sensor_error}")

            print(f"{timestamp} | Temp: {temperature_c:.1f}°C | Humidité: {humidity:.1f}%")

            if temperature_c > 25:
                GPIO.output(VENTILATEUR, GPIO.HIGH)
                GPIO.output(LED_ROUGE, GPIO.HIGH)
                GPIO.output(LED_BLEUE, GPIO.HIGH)
                GPIO.output(LED_VERTE, GPIO.LOW)
                GPIO.output(LED_JAUNE, GPIO.LOW)
                rlog_temp_sensor = 'R'
            elif temperature_c> 21.25:
                GPIO.output(VENTILATEUR, GPIO.LOW)
                GPIO.output(LED_JAUNE, GPIO.HIGH)
                GPIO.output(LED_ROUGE, GPIO.LOW)
                GPIO.output(LED_BLEUE, GPIO.LOW)
                GPIO.output(LED_VERTE, GPIO.LOW)
                rlog_temp_sensor = 'J'
            elif temperature_c<=20 :
                GPIO.output(VENTILATEUR, GPIO.LOW)
                GPIO.output(LED_JAUNE, GPIO.LOW)
                GPIO.output(LED_ROUGE, GPIO.LOW)
                GPIO.output(LED_BLEUE, GPIO.LOW)
                GPIO.output(LED_VERTE, GPIO.LOW)
                rlog_temp_sensor = ''

            else:
                GPIO.output(VENTILATEUR, GPIO.LOW)
                GPIO.output(LED_VERTE, GPIO.HIGH)
                GPIO.output(LED_ROUGE, GPIO.LOW)
                GPIO.output(LED_BLEUE, GPIO.LOW)
                GPIO.output(LED_JAUNE, GPIO.LOW)
                rlog_temp_sensor = 'V'

            with open(csv_file, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([
                    rlog_filename,
                    rlog_grp,
                    rlog_id,
                    timestamp,
                    temperature_c,
                    rlog_temp_sensor,
                    temp_error,
                    humidity,
                    rlog_hum_sensor,
                    hum_error
                ])
                rlog_id += 1

            call_stored_procedure(csv_file)

        except Exception as e:
            print(f"[ Erreur] {e}")

        time.sleep(15)


except KeyboardInterrupt:
    print(" Arrêt du programme.")
    GPIO.cleanup()





