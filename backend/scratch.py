import requests

url = "https://api.groq.com/openai/v1/chat/completions"
headers = {
    "Authorization": "Bearer YOUR_GROQ_API_KEY",
    "Content-Type": "application/json"
}
payload = {
    "model": "llama-3.3-70b-versatile",
    "messages": [{"role": "user", "content": "Hello"}]
}
try:
    resp = requests.post(url, headers=headers, json=payload)
    print(resp.status_code)
    print(resp.text)
except Exception as e:
    print("Error:", e)
