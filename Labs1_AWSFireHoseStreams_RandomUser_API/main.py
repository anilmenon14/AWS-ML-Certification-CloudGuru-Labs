"""Using API call to randomuser, transforming to format asked in the Labs and sending to the firehose stream"""

import requests
import click
import json
import boto3
import botocore
import time

kinesisStreamClient = boto3.client('firehose')

#helper function

def apiCall(records):
    apiResponse = requests.get("https://randomuser.me/api/?results={}".format(records))
    userList = json.loads(apiResponse.text)['results'];
    transformedList = []
    for user in userList:
        tempDict = {}
        if user['dob']['age'] >= 21:
            tempDict["FIRST"] = user["name"]["first"];
            tempDict["LAST"] = user["name"]["last"];
            tempDict["AGE"] = user['dob']['age'];
            tempDict["GENDER"] = user["gender"];
            tempDict["LATITUDE"] = user['location']['coordinates']['latitude'];
            tempDict["LONGITUDE"] = user['location']['coordinates']['longitude'];
            transformedList.append(tempDict)
    transformedListJson = json.dumps(transformedList)
    response = kinesisStreamClient.put_record(
    DeliveryStreamName='randomuserstream',
    Record={
        'Data': bytes(transformedListJson,encoding='utf-8')
    }
    )
    print(response)
    #transmit to API

@click.command()
@click.option('--records',type=int,default =5,help="Number of records to pull back from RandomUser API")
@click.option('--timeinmins',type=int,default=0,help="Number of minutes to run the program (1 call per 5 seconds)")
def cli(records,timeinmins):
    if timeinmins > 0:
        for itr in range(0,(timeinmins*12)): #every 5 seconds
            apiCall(records)
            print('Iteration # {} finished running. Pausing for 5 secs.....'.format(itr+1))
            time.sleep(5)

    elif timeinmins == 0:
        apiCall(records)
    else:
        print('Please pass timeinmins as a positive number')

if __name__ == "__main__":
    cli();
