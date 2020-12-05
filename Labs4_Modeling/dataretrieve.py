#Script to pull data from ACG GitHub and load into ufodata.csv
#run 'python dataretrieve.py from command line to pull up data'
import requests

resp = requests.get('https://raw.githubusercontent.com/ACloudGuru-Resources/Course_AWS_Certified_Machine_Learning/master/Chapter6/ufo_fullset.csv')
with open('ufodata.csv','w') as c:
    c.write(resp.text)
