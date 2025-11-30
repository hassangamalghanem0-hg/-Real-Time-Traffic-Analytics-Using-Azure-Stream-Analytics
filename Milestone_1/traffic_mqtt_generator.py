import random
import time
import json
import yaml
import logging
import csv
from datetime import datetime, timedelta
import paho.mqtt.client as mqtt

logging.basicConfig(
    filename="traffic_mqtt.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class TrafficDataGeneratorMQTT:
    def __init__(self, config_path="config.yaml"):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.frequency = self.config["frequency"]
        self.streets = self.config["streets"]
        self.rush_hours = self.config["rush_hours"]
        self.broker = self.config["mqtt"]["broker"]
        self.port = self.config["mqtt"]["port"]
        self.topic = self.config["mqtt"]["topic"]
        self.output_format = self.config["output"]["format"]
        self.output_file = self.config["output"]["file"]

        self.client = mqtt.Client()
        self.client.connect(self.broker, self.port, 60)

        self.weather_states = ["clear", "rain", "fog", "storm"]
        self.weather = random.choice(self.weather_states)
        self.last_weather_update = datetime.now()

        self.traffic_lights = ["green", "yellow", "red"]
        self.current_light = random.choice(self.traffic_lights)
        self.last_light_update = datetime.now()

        if self.output_format == "csv":
            with open(self.output_file, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file, delimiter=",")
                writer.writerow([
                    "timestamp", "street_name", "vehicle_count", "vehicle_speed",
                    "light_level", "weather", "traffic_light",
                    "solar_energy_level", "lighting_demand"
                ])

    def is_rush_hour(self, now):
        current_time = now.strftime("%H:%M")
        for rh in self.rush_hours:
            if rh["start"] <= current_time <= rh["end"]:
                return rh["multiplier"]
        return 1

    def get_light_level(self, now):
        hour = now.hour
        if 6 <= hour < 17:
            return "day"
        elif 17 <= hour < 19:
            return "dusk"
        else:
            return "night"

    def get_weather(self, now):
        update_interval = self.config["weather"]["update_interval"]
        if now - self.last_weather_update > timedelta(seconds=update_interval):
            self.weather = random.choice(self.weather_states)
            self.last_weather_update = now
        return self.weather

    def get_traffic_light(self, now):
        if now - self.last_light_update > timedelta(seconds=30):
            self.current_light = random.choice(self.traffic_lights)
            self.last_light_update = now
        return self.current_light

    def apply_weather_effects(self, speed, count, weather):
        if weather == "rain":
            speed *= 0.8
            count *= 0.8
        elif weather == "fog":
            speed *= 0.7
            count *= 0.7
        elif weather == "storm":
            speed *= 0.5
            count *= 0.5
        return round(speed, 2), int(count)

    def apply_traffic_light_effects(self, speed, count, light):
        if light == "red":
            speed = random.uniform(0, 10)
            count *= 1.5
        elif light == "yellow":
            speed *= 0.6
        return round(speed, 2), int(count)

    def generate_record(self):
        now = datetime.now()
        street = random.choice(self.streets)

        multiplier = self.is_rush_hour(now)
        vehicle_count = int(random.randint(5, 30) * multiplier)

        # سرعة أساسية بين 40 و120
        base_speed = random.uniform(40, 120)
        traffic_light = self.get_traffic_light(now)
        weather = self.get_weather(now)

        # تعديل السرعة حسب الإشارة
        if traffic_light == "red":
            vehicle_speed = random.uniform(0, 10)
        elif traffic_light == "yellow":
            vehicle_speed = base_speed * 0.6
        else:  # green
            vehicle_speed = base_speed

        # تأثير الطقس
        vehicle_speed, vehicle_count = self.apply_weather_effects(vehicle_speed, vehicle_count, weather)

        light_level = self.get_light_level(now)

        # طاقة شمسية والطلب على الإضاءة
        solar_energy_level = round(random.uniform(0.0, 1.0), 2)
        if light_level == "night" or weather in ["fog", "rain", "storm"]:
            lighting_demand = "high"
        elif light_level == "dusk":
            lighting_demand = "medium"
        else:
            lighting_demand = "low"

        record = {
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "street_name": street["name"],
            "vehicle_count": max(0, int(vehicle_count)),
            "vehicle_speed": round(max(0, min(vehicle_speed, 150)), 2),
            "light_level": light_level,
            "weather": weather,
            "traffic_light": traffic_light,
            "solar_energy_level": solar_energy_level,
            "lighting_demand": lighting_demand
        }
        return record

    def publish_record(self, record):
        payload = json.dumps(record)
        self.client.publish(self.topic, payload)
        logging.info(f"Published to MQTT: {payload}")
        print(f"Published to MQTT: {payload}")

    def save_to_csv(self, record):
        with open(self.output_file, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file, delimiter=",")
            writer.writerow([
                record["timestamp"],
                record["street_name"],
                record["vehicle_count"],
                record["vehicle_speed"],
                record["light_level"],
                record["weather"],
                record["traffic_light"],
                record["solar_energy_level"],
                record["lighting_demand"]
            ])
        logging.info(f"Saved to CSV: {record}")

    def run_generator(self):
        logging.info("Traffic MQTT + CSV data generation started.")
        while True:
            record = self.generate_record()
            self.publish_record(record)
            if self.output_format == "csv":
                self.save_to_csv(record)
            time.sleep(self.frequency)

if __name__ == "__main__":
    generator = TrafficDataGeneratorMQTT("config.yaml")
    generator.run_generator()
