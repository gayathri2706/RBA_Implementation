import os
import requests
import json

# Get the current directory
cd = os.getcwd()
config_dir = os.path.join(cd, "config")

# Load configuration JSON file
config = None
for file_name in os.listdir(config_dir):
    if file_name.endswith('.json'):
        file_path = os.path.join(config_dir, file_name)
        with open(file_path, "r") as config_file:
            config = json.load(config_file)

if not config:
    print("❌ Configuration file not found!")
    exit()

# Extract API details from config
API_URL = config.get("api_url")
LOGIN_URL = config.get("login_url")
USERNAME = config.get("j_username")
PASSWORD = config.get("j_password")

if not all([API_URL, LOGIN_URL, USERNAME, PASSWORD]):
    print("❌ Missing API configuration details in the JSON file.")
    exit()

# Create a session
session = requests.Session()

# Login request payload
payload = {"j_username": USERNAME, "j_password": PASSWORD}
login_response = session.post(LOGIN_URL, data=payload)

if login_response.status_code == 200:
    print("✅ Login successful!")
else:
    print(f"❌ Login failed! Status Code: {login_response.status_code}, Response: {login_response.text}")
    exit()

# Extract JSESSIONID
jsessionid = session.cookies.get("JSESSIONID")
if not jsessionid:
    print("❌ Failed to retrieve JSESSIONID. Authentication may not have succeeded.")
    exit()

print(f"🔑 Using JSESSIONID: {jsessionid}")

# Read JSON data from file
json_file_path = os.path.join(cd, "data.json")
try:
    with open(json_file_path, "r", encoding="utf-8") as json_file:
        data = json.load(json_file)  # Load JSON data
except FileNotFoundError:
    print(f"❌ Error: {json_file_path} not found!")
    exit()
except json.JSONDecodeError:
    print(f"❌ Error: Invalid JSON format in {json_file_path}!")
    exit()

# Debug: Print data before sending
print("📤 Sending Data:", json.dumps(data, indent=4))

# Define headers with extracted JSESSIONID
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Cookie": f"JSESSIONID={jsessionid}"
}

# Send POST request

response = requests.post(API_URL, json=data, headers=headers)

# Debug: Print response details
print(f"📩 Response Status Code: {response.status_code}")
print("📩 Response Headers:", response.headers)
print("📩 Response Body:", response.text)
