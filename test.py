import requests
from requests.auth import HTTPBasicAuth

raw_password = "kr7S fuuv dkVm ZQGa Sh8z 1T8q"
clean_password = raw_password.replace(" ", "")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Try all three variations
credentials = [
    ("texasfashioninsider", clean_password),
    ("pangeaiimp@gmail.com", clean_password),        # ← replace with your actual email
    ("admin", clean_password),                  # ← if admin is the username
]

for username, password in credentials:
    resp = requests.get(
        "https://sanfranciscobriefing.com/wp-json/wp/v2/users/me",
        headers=headers,
        auth=HTTPBasicAuth(username, password),
        timeout=20
    )
    print(f"{username}: {resp.status_code}")