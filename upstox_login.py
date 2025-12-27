import webbrowser
import urllib.parse

API_KEY = "609a3a41-dc34-4ea5-be8b-d8a9cf18cc74"
REDIRECT_URI = "http://localhost:8000/callback"

login_url = (
    "https://api.upstox.com/v2/login/authorization/dialog?"
    + urllib.parse.urlencode(
        {"response_type": "code", "client_id": API_KEY, "redirect_uri": REDIRECT_URI}
    )
)

print("Opening browser for Upstox login...")
print(login_url)

webbrowser.open(login_url)
