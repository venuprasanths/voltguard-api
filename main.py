from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import joblib, requests
import numpy as np
import threading
import paho.mqtt.client as mqtt
import json, time, random

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

model = joblib.load("soh_model.pkl")

# ─── CONFIG ───────────────────────────────────────────────
BROKER = "05057d625d2e401ab73955b4957644e1.s1.eu.hivemq.cloud"
PORT   = 8883
USER   = "voltguard"
PASS   = "Voltguard123"

SB_URL = "https://ikstgezcupssexwkqglf.supabase.co"
SB_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imlrc3RnZXpjdXBzc2V4d2txZ2xmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzg2ODQzMzksImV4cCI6MjA5NDI2MDMzOX0.X5wbJ3eTcO8-0GajfiXQXxFBw0_kRsw_Vggz9U3sFxk"

SB_HEADERS = {
    "apikey": SB_KEY,
    "Authorization": "Bearer " + SB_KEY,
    "Content-Type": "application/json"
}

# ─── SIMULATOR THREAD ─────────────────────────────────────
vehicles = ["VG-001", "VG-002", "VG-003", "VG-004", "VG-005", "VG-006"]
soh_state = {"VG-001": 85, "VG-002": 62, "VG-003": 21,
             "VG-004": 78, "VG-005": 45, "VG-006": 91}

def run_simulator():
    sim = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    sim.username_pw_set(USER, PASS)
    sim.tls_set()

    def on_connect(client, userdata, flags, rc, props):
        print("[Simulator] Connected to HiveMQ!")

    sim.on_connect = on_connect
    sim.connect(BROKER, PORT)
    sim.loop_start()
    time.sleep(3)

    print("[Simulator] Starting data publish...")
    while True:
        for v in vehicles:
            soh_state[v] = max(5, soh_state[v] - random.uniform(0.3, 0.8))
            if soh_state[v] <= 5:
                soh_state[v] = random.uniform(85, 100)
                print(f"[Simulator] Battery Swapped: {v}")
            data = {
                "vehicle_id": v,
                "soh": round(soh_state[v], 1),
                "voltage": round(48 * (soh_state[v] / 100) + random.uniform(-1, 1), 2),
                "temp": round(28 + random.uniform(-3, 8), 1)
            }
            sim.publish(f"voltguard/{v}/telemetry", json.dumps(data))
            print(f"[Simulator] Sent -> {v} | SOH: {data['soh']}%")
        time.sleep(5)

# ─── STATION SIMULATOR THREAD ─────────────────────────────
stations_state = {
    "SS-001": {"charged": 7, "charging": 2, "empty": 1, "total": 10},
    "SS-002": {"charged": 3, "charging": 3, "empty": 2, "total": 8},
    "SS-003": {"charged": 9, "charging": 1, "empty": 2, "total": 12},
}

def run_station_simulator():
    print("[StationSim] Starting...")
    time.sleep(5)
    while True:
        for sid, s in stations_state.items():
            if s["charging"] > 0 and random.random() > 0.6:
                done = random.randint(0, s["charging"])
                s["charged"] = min(s["total"], s["charged"] + done)
                s["charging"] = max(0, s["charging"] - done)
            if s["charged"] > 0 and random.random() > 0.7:
                taken = random.randint(0, min(2, s["charged"]))
                s["charged"] = max(0, s["charged"] - taken)
                s["empty"] = min(s["total"], s["empty"] + taken)
            if s["empty"] > 0 and random.random() > 0.5:
                new_charge = random.randint(0, s["empty"])
                s["charging"] = min(s["total"], s["charging"] + new_charge)
                s["empty"] = max(0, s["empty"] - new_charge)

            payload = {
                "charged_batteries": s["charged"],
                "charging_batteries": s["charging"],
                "empty_slots": s["empty"],
                "updated_at": "now()"
            }
            try:
                requests.patch(
                    SB_URL + "/rest/v1/swap_stations?station_id=eq." + sid,
                    json=payload,
                    headers=SB_HEADERS,
                    timeout=5
                )
                print(f"[StationSim] {sid} | Charged:{s['charged']} Charging:{s['charging']} Empty:{s['empty']}")
            except Exception as e:
                print(f"[StationSim] Error: {e}")
        time.sleep(10)

# ─── MQTT → SUPABASE LISTENER THREAD ──────────────────────
def save_to_db(data):
    payload = {
        "vehicle_id": data["vehicle_id"],
        "soh": data["soh"],
        "voltage": data["voltage"],
        "temp": data["temp"]
    }
    try:
        r = requests.post(
            SB_URL + "/rest/v1/battery_telemetry",
            json=payload,
            headers=SB_HEADERS,
            timeout=5
        )
        if r.status_code == 201:
            print(f"[DB] Saved: {data['vehicle_id']} SOH={data['soh']}%")
        else:
            print(f"[DB] Error: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"[DB] Exception: {e}")

def run_mqtt_listener():
    def on_connect(client, userdata, flags, rc, props):
        print("[Listener] MQTT Connected!")
        client.subscribe("voltguard/+/telemetry")
        print("[Listener] Listening...")

    def on_message(client, userdata, msg):
        data = json.loads(msg.payload)
        save_to_db(data)

    listener = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    listener.username_pw_set(USER, PASS)
    listener.tls_set()
    listener.on_connect = on_connect
    listener.on_message = on_message
    listener.connect(BROKER, PORT)
    print("[Listener] Connecting to MQTT...")
    listener.loop_forever()

# ─── TELEGRAM ALERT THREAD ────────────────────────────────
BOT_TOKEN = "8856869624:AAEdNKimQsENol5gUGdlyQwLCKoXf_b6Syg"
CHAT_ID   = "1480220606"

def send_telegram(text):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text},
            timeout=5
        )
        print(f"[Telegram] Sent: {text[:50]}...")
    except Exception as e:
        print(f"[Telegram] Error: {e}")

def run_alert_bot():
    print("[AlertBot] Starting...")
    time.sleep(10)
    alerted_critical = set()
    alerted_low      = set()
    station_alerted  = set()

    while True:
        try:
            # Vehicle alerts
            r = requests.get(
                SB_URL + "/rest/v1/battery_telemetry?select=*&order=created_at.desc&limit=600",
                headers=SB_HEADERS, timeout=5
            )
            rows = r.json()
            latest = {}
            for row in rows:
                if row["vehicle_id"] not in latest:
                    latest[row["vehicle_id"]] = row

            for vid, row in latest.items():
                soh     = round(row["soh"], 1)
                voltage = round(row["voltage"], 2)
                temp    = round(row["temp"], 1)

                if soh < 25 and vid not in alerted_critical:
                    msg = (
                        f"🚨 VoltGuard CRITICAL ALERT!\n\n"
                        f"Vehicle: {vid}\n"
                        f"Battery SOH: {soh}%\n"
                        f"Voltage: {voltage}V\n"
                        f"Temp: {temp}°C\n\n"
                        f"⚠️ ACTION: Swap battery immediately!\n"
                        f"📍 Nearest Station: Anna Nagar (1.2km)\n"
                        f"✅ Available batteries: 5"
                    )
                    send_telegram(msg)
                    alerted_critical.add(vid)
                    alerted_low.discard(vid)

                elif soh < 50 and vid not in alerted_low and vid not in alerted_critical:
                    msg = (
                        f"⚠️ VoltGuard LOW BATTERY WARNING!\n\n"
                        f"Vehicle: {vid}\n"
                        f"Battery SOH: {soh}%\n\n"
                        f"Plan a swap within 2 hours."
                    )
                    send_telegram(msg)
                    alerted_low.add(vid)

                elif soh >= 50:
                    alerted_critical.discard(vid)
                    alerted_low.discard(vid)

            # Station alerts
            rs = requests.get(
                SB_URL + "/rest/v1/swap_stations?select=*",
                headers=SB_HEADERS, timeout=5
            )
            for s in rs.json():
                sid = s["station_id"]
                if s["charged_batteries"] <= 1 and sid not in station_alerted:
                    msg = (
                        f"🔴 VoltGuard STATION LOW STOCK!\n\n"
                        f"Station: {s['station_name']}\n"
                        f"Charged batteries left: {s['charged_batteries']}\n\n"
                        f"⚠️ Restock this station immediately!"
                    )
                    send_telegram(msg)
                    station_alerted.add(sid)
                elif s["charged_batteries"] > 3:
                    station_alerted.discard(sid)

            print(f"[AlertBot] Checked | Vehicles: {len(latest)}")

        except Exception as e:
            print(f"[AlertBot] Error: {e}")

        time.sleep(15)

# ─── START ALL BACKGROUND THREADS ON STARTUP ──────────────
@app.on_event("startup")
def startup_event():
    threading.Thread(target=run_simulator,         daemon=True).start()
    threading.Thread(target=run_mqtt_listener,     daemon=True).start()
    threading.Thread(target=run_station_simulator, daemon=True).start()
    threading.Thread(target=run_alert_bot,         daemon=True).start()
    print("[VoltGuard] All background services started!")

# ─── API ENDPOINTS ────────────────────────────────────────
@app.get("/")
def root():
    return {"message": "VoltGuard API Running!", "status": "online"}

@app.get("/fleet")
def get_fleet():
    r = requests.get(
        SB_URL + "/rest/v1/battery_telemetry?select=*&order=created_at.desc&limit=600",
        headers=SB_HEADERS
    )
    rows = r.json()

    latest = {}
    for row in rows:
        vid = row["vehicle_id"]
        if vid not in latest:
            latest[vid] = row

    result = []
    for vid, row in latest.items():
        soh   = round(row["soh"], 1)
        voltage = round(row["voltage"], 2)
        temp  = round(row["temp"], 1)
        pred_soh = round(max(5, min(100, (voltage / 48) * 100)), 1)

        if soh > 50:
            status = "healthy"
        elif soh > 25:
            status = "low"
        else:
            status = "critical"

        result.append({
            "vehicle_id":    vid,
            "soh":           soh,
            "predicted_soh": pred_soh,
            "voltage":       voltage,
            "temp":          temp,
            "status":        status
        })

    result.sort(key=lambda x: x["soh"])
    return {"fleet": result, "count": len(result)}

@app.get("/stations")
def get_stations():
    r = requests.get(
        SB_URL + "/rest/v1/swap_stations?select=*&order=station_id",
        headers=SB_HEADERS
    )
    stations = r.json()

    result = []
    for s in stations:
        if s["charged_batteries"] >= 5:
            availability = "high"
        elif s["charged_batteries"] >= 2:
            availability = "medium"
        else:
            availability = "low"
        s["availability"] = availability
        result.append(s)

    return {"stations": result}