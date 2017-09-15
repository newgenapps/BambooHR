import argparse
import datetime
import sys
import requests
from re import search, sub


# Setup the CLI arguments parser
parser = argparse.ArgumentParser()
parser.add_argument('auth', help='User API auth key.', type=str)
parser.add_argument('dest', help='Full path to CSV and artifacts destination.', type=str)
args = parser.parse_args()

epochNow = datetime.datetime.today().strftime('%Y%m%d_%s')
APIPrefix = 'https://api.bamboohr.com/api/gateway.php/ggh/v1/employees'
userKeys = ['address1', 'address2', 'age', 'bestEmail', 'city', 'country', 'dateOfBirth', 'department', 'division',
    'employeeNumber', 'employmentHistoryStatus', 'firstName', 'fullName1', 'fullName2', 'fullName3', 'fullName4',
    'fullName5', 'displayName', 'gender', 'hireDate', 'homeEmail', 'homePhone', 'id', 'jobTitle', 'lastChanged',
    'lastName', 'location', 'maritalStatus', 'middleName', 'mobilePhone', 'payChangeReason', 'payGroupId', 'payRate',
    'payRateEffectiveDate', 'payType', 'paidPer', 'payPeriod', 'ssn', 'state', 'stateCode', 'supervisor', 'supervisorId',
    'supervisorEId', 'terminationDate', 'workEmail', 'workPhone', 'workPhonePlusExtension', 'workPhoneExtension',
    'zipcode', 'isPhotoUploaded', 'employmentStatus', 'nickname', 'photoUploaded', 'customBenefitDue',
    'customBenefitDue', 'customCompany', 'customDateofConfirmation', 'customGrade1', 'customLagosGrade', 'customLevel',
    'customNationalInsuranceNumber', 'customNationality', 'customNHFNumber', 'customNIC', 'customNigeriaMobilePhone',
    'customNon-DomStatus', 'customPakistanMobilePhone', 'customRwandaMobilePhone', 'customStateofOrigin',
    'customTaxIDNumber', 'customUKWorkPermit']
userTables = [
    #, 'compensation', 'customBankDetails', 'customRSADetails'
    'jobInfo', 'employmentStatus', 'emergencyContacts', 'compensation'
]
userIDs = []


def fetchFromAPI(url):
    try:
        results = requests.get(url, headers={'Accept': 'application/json'}, auth=(args.auth, ":x"))
        if results.status_code == 200:
            return results.json()
        else:
            sys.stderr.write('Could not fetch userIDs; exiting...' + "\n")
            exit(1)
    except (ConnectionError, requests.exceptions.HTTPError, requests.exceptions.Timeout) as e:
        sys.stderr.write('ERROR: ' + str(e) + '; exiting...' + "\n")
        exit(1)


def openFileHandler(fileName):
    try:
        fh = open(fileName, 'a')
        return fh
    except (PermissionError, OSError, IOError) as e:
        sys.stderr.write('ERROR: ' + str(e) + '; exiting...' + "\n")
        exit(1)


def processAttrValue(String):
    if str(String) == "None" or str(String) == "":
        return '-,'
    else:
        if search("'", str(String)):
            return sub("'", '', str(String)) + ','
        elif search(",", str(String)):
            return sub(r'(.*)', r'"\1"', str(String)) + ','
        else:
            return str(String) + ','


def checkHeaderForAttribute(fileName, keyword):
    try:
        fh = open(fileName, 'r')
        firstLine = fh.readline()
        fh.close()

        if search(keyword, firstLine):
            return True
        else:
            return False
    except FileNotFoundError:
        return False
    except (PermissionError, OSError, IOError) as e:
        sys.stderr.write('ERROR: ' + str(e) + '; exiting...' + "\n")
        exit(1)


def writeSingleLevelCSV(tableName, keyList):
    outputFile = args.dest + '/' + epochNow + '_' + tableName + '.csv'
    headerPresent = checkHeaderForAttribute(outputFile, 'displayName')
    jobInfoCSV = openFileHandler(outputFile)

    if headerPresent == False:
        jobInfoCSV.write('displayName,' + str(','.join(map(str, keyList)) + "\n"))

    tableInfoGet = fetchFromAPI(APIPrefix + '/' + str(id) + '/tables/' + tableName)
    for elem in tableInfoGet:
        csvOutput = processAttrValue(employee)
        for key in keyList:
            csvOutput += processAttrValue(elem[key])
        jobInfoCSV.write(csvOutput.rstrip(',') + "\n")
    jobInfoCSV.close()


'''
def writeTwoLevelCSV(tableName, keyList, subKey, subKeyList):
    outputFile = args.dest + '/' + epochNow + '_' + tableName + '.csv'
    headerPresent = checkHeaderForAttribute(outputFile, 'displayName')
    jobInfoCSV = openFileHandler(outputFile)

    if headerPresent == False:
        jobInfoCSV.write('displayName,' + str(','.join(map(str, keyList)) + "\n"))

    tableInfoGet = fetchFromAPI(APIPrefix + '/' + str(id) + '/tables/' + tableName)
    for elem in tableInfoGet:
        csvOutput = processAttrValue(employee)
        for key in keyList:
            print(type(elem[key]))
            if key == subKey:
                for tag in elem[key]:
                    csvOutput += processAttrValue(elem[key][tag])
            else:
                csvOutput += processAttrValue(elem[key])
        print(csvOutput)
        #jobInfoCSV.write(csvOutput.rstrip(',') + "\n")
    #jobInfoCSV.close()
'''


def exec_jobInfo(tableName):
    jobInfoKeys = ['jobTitle', 'reportsTo', 'location', 'division', 'department', 'date']
    writeSingleLevelCSV(tableName, jobInfoKeys)


def exec_employmentStatus(tableName):
    statusKeys = ['employmentStatus', 'employeeId', 'date']
    writeSingleLevelCSV(tableName, statusKeys)


def exec_emergencyContacts(tableName):
    contactKeys = ['employeeId', 'name', 'relationship', 'homePhone', 'addressLine1', 'addressLine2', 'mobilePhone',
                   'email', 'zipcode', 'city', 'state', 'country', 'workPhone', 'workPhoneExtension']
    writeSingleLevelCSV(tableName, contactKeys)


def exec_compensation(tableName):
    allKeys = compKeys = ['type', 'payPeriod', 'employeeId', 'startDate']
    subKey = 'rate'
    subKeyheaders = ['currency', 'value']
    allKeys.append(subKey)
    #writeTwoLevelCSV(tableName, compKeys, subKey, subKeyheaders)

    fileName = args.dest + '/' + epochNow + '_' + tableName + '.csv'
    headerPresent = checkHeaderForAttribute(fileName, 'displayName')

    statusCSV = openFileHandler(fileName)
    if headerPresent == False:
        statusCSV.write('displayName,' + str(','.join(map(str, compKeys))) + ','
            + str(','.join(map(str, subKeyheaders))) + "\n")

    compGetInfo = fetchFromAPI(APIPrefix + '/' + str(id) + '/tables/' + tableName)
    for elem in compGetInfo:
        csvOutput = processAttrValue(employee)
        for key in allKeys:
            if key == subKey:
                for tag in subKeyheaders:
                    csvOutput += processAttrValue(elem[key][tag])
            else:
                csvOutput += processAttrValue(elem[key])
        statusCSV.write(csvOutput.rstrip(',') + "\n")
    statusCSV.close()


#-----
ids = [40, 46, 51, 671]
#-----


# Fetch the list of user IDs
userIDGet = fetchFromAPI(APIPrefix + '/directory')
for employee in userIDGet['employees']:
    userIDs.append(employee['id'])

employeeCSV = openFileHandler(args.dest + '/' + epochNow + '_employees.csv')
employeeCSV.write(','.join(map(str, userKeys)) + "\n")
for id in ids:
    # Do not run for ID 671 - Viv Diwakar
    if id != 671:
        csvOutput = ''
        userInfoGet = fetchFromAPI(APIPrefix + '/' + str(id) + '?fields=' + ','.join(map(str, userKeys)))
        for key in userKeys:
            csvOutput += processAttrValue(userInfoGet[key])
        employeeCSV.write(csvOutput.rstrip(',') + "\n")

        employee = userInfoGet['displayName']
        for table in userTables:
            locals()[str('exec_' + table)](table)
employeeCSV.close()