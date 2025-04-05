import adafruit_dht
import board
import time
import csv
from datetime import datetime
import RPi.GPIO as GPIO
import psycopg2

# === CONFIGURATION GPIO ===
GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)

# Pins
LED_ROUGE = 20
LED_JAUNE = 16
LED_VERTE = 13
LED_BLEUE = 19
VENTILATEUR = 26
DHT_PIN = board.D21  # GPIO 21 (Pin 40)

# Setup GPIO
for pin in [LED_ROUGE, LED_JAUNE, LED_VERTE, LED_BLEUE, VENTILATEUR]:
    GPIO.setup(pin, GPIO.OUT)

# Initialisation du capteur
dhtDevice = adafruit_dht.DHT22(DHT_PIN, use_pulseio=False)

# Fichier CSV
csv_file = "/home/pi/donnees_dht.csv"  #  Chemin absolu conseillé pour PostgreSQL
with open(csv_file, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Horodatage", "Temperature", "Humidite", "Ventilateur"])

# Paramètres PostgreSQL
DB_HOST = "172.20.2.200"
DB_PORT = "5432"
DB_NAME = "tbl_log_raspberry"
DB_USER = "jpo2025"
DB_PASS = "jpo2025"

# Fonction d'appel de la procédure stockée
def call_stored_procedure(csv_path):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()
        cursor.callproc('load_csv', [csv_path])
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Données envoyées via procédure : {csv_path}")
    except psycopg2.DatabaseError as e:
        print(f"[Erreur BDD] {e}")
    except Exception as e:
        print(f"[Erreur générale] {e}")

# === BOUCLE PRINCIPALE ===
print("🌡 Démarrage du contrôle (Ctrl+C pour arrêter)")

try:
    while True:
        try:
            temperature_c = dhtDevice.temperature
            humidity = dhtDevice.humidity
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            print(f"{timestamp} | Température: {temperature_c:.1f}°C | Humidité: {humidity:.1f}%")

            if temperature_c > 25:
                GPIO.output(VENTILATEUR, GPIO.HIGH)
                GPIO.output(LED_ROUGE, GPIO.HIGH)
                GPIO.output(LED_BLEUE, GPIO.HIGH)
                GPIO.output(LED_VERTE, GPIO.LOW)
                GPIO.output(LED_JAUNE, GPIO.LOW)
                ventilo_etat = "ON"

            elif 24 < temperature_c <= 25:
                GPIO.output(VENTILATEUR, GPIO.LOW)
                GPIO.output(LED_JAUNE, GPIO.HIGH)
                GPIO.output(LED_ROUGE, GPIO.LOW)
                GPIO.output(LED_BLEUE, GPIO.LOW)
                GPIO.output(LED_VERTE, GPIO.LOW)
                ventilo_etat = "OFF"

            else:
                GPIO.output(VENTILATEUR, GPIO.LOW)
                GPIO.output(LED_VERTE, GPIO.HIGH)
                GPIO.output(LED_ROUGE, GPIO.LOW)
                GPIO.output(LED_BLEUE, GPIO.LOW)
                GPIO.output(LED_JAUNE, GPIO.LOW)
                ventilo_etat = "OFF"

            # Écriture CSV
            with open(csv_file, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([timestamp, temperature_c, humidity, ventilo_etat])

            # Appel à la procédure PostgreSQL
            call_stored_procedure(csv_file)

        except RuntimeError as e:
            print(f"[Erreur capteur] {e}")
        except Exception as e:
            print(f"[Erreur interne] {e}")

        time.sleep(2)

except KeyboardInterrupt:
    print(" Arrêt du programme.")
    GPIO.cleanup()
