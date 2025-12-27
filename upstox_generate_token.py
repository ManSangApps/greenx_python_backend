import requests

API_KEY = "609a3a41-dc34-4ea5-be8b-d8a9cf18cc74"
API_SECRET = "b0619lwgum"
REDIRECT_URI = "http://localhost:8000/callback"

AUTH_CODE = "rGzO7J"

url = "https://api.upstox.com/v2/login/authorization/token"

payload = {
    "code": AUTH_CODE,
    "client_id": API_KEY,
    "client_secret": API_SECRET,
    "redirect_uri": REDIRECT_URI,
    "grant_type": "authorization_code",
}

headers = {"Content-Type": "application/x-www-form-urlencoded"}

response = requests.post(url, data=payload, headers=headers)

print(response.json())
