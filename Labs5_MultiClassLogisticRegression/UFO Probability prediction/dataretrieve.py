#Script to pull data from ACG GitHub and load into file
#run 'python dataretrieve.py' from command line to pull up data
import requests

resp = requests.get('https://raw.githubusercontent.com/ACloudGuru-Resources/Course_AWS_Certified_Machine_Learning/master/Chapter7/ufo-algorithms-lab.ipynb')
with open('ACloudGuru_FullSolution.ipynb','w') as c:
    c.write(resp.text)
