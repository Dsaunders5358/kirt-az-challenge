import requests

csv_content = requests.get('https://docs.google.com/spreadsheets/d/1AGHKXAd4hPa12kvyjAr9WGb4Cm2ZsYNIhiEAj_Hu1Ss/export?format=csv&gid=0').text
print(csv_content)