import argparse
import datetime
import sys
import requests
from os import makedirs
from os.path import dirname, exists
from re import search, sub, escape
import xmltodict


# Setup the CLI arguments parser
parser = argparse.ArgumentParser()
parser.add_argument('auth', help='User API auth key.', type=str)
parser.add_argument('company', help='Company name within BambooHR.', type=str)
parser.add_argument('dest', help='Full path to CSV and artifacts destination.', type=str)
args = parser.parse_args()

epochNow = datetime.datetime.today().strftime('%Y%m%d_%s')
APIPrefix = 'https://api.bamboohr.com/api/gateway.php/' + args.company + '/v1'
userTables = ['jobInfo', 'employmentStatus', 'emergencyContacts', 'compensation', 'customBankDetails',
    'customRSADetails', 'employeedependents']


def fetchFromAPI(url, outform):
    try:
        results = requests.get(url, headers={'Accept': 'application/json'}, auth=(args.auth, ":x"))
        if results.status_code == 200:
            if outform == 'json':
                return results.json()
            elif outform == 'xml':
                return results.text
        else:
            sys.stderr.write('API Request error on "' + url + '"; exiting...' + "\n")
            exit(1)
    except (requests.ConnectionError, requests.exceptions.HTTPError, requests.exceptions.Timeout) as e:
        sys.stderr.write('ERROR: ' + str(e) + '; exiting...' + "\n")
        exit(1)


def fetchBinaryFile(url, destination):
    try:
        image = requests.get(url, headers={'Accept': 'application/json'}, auth=(args.auth, ":x"))

        directory = dirname(destination)
        if not exists(directory):
            makedirs(directory)

        with open(destination, 'wb') as f:
            f.write(image.content)
        f.close()

    except (requests.ConnectionError, requests.exceptions.HTTPError, requests.exceptions.Timeout) as e:
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


def processAPIInfo(httpReturn, allKeys, subKeyList):
    csvOutput = ''

    if isinstance(httpReturn, dict):
        csvOutput = processAttrValue(employee)
        for key in allKeys:
            if key in subKeyList.keys():
                for tag in subKeyList[key]:
                    csvOutput += processAttrValue(httpReturn[key][tag])
            else:
                csvOutput += processAttrValue(httpReturn[key])
    else:
        index = -1
        for index in range(len(httpReturn) - 1):
            csvOutput += processAPIInfo(httpReturn[index], allKeys, subKeyList) + '\n'
        csvOutput += processAPIInfo(httpReturn[(index + 1)], allKeys, subKeyList)

    return csvOutput


def writeCSVToFile(fetchInfo, tableName, topKeyList, subKeyList):
    allKeys = topKeyList[:]
    for parKey in sorted(subKeyList.keys()):
        allKeys.append(parKey)

    fileName = args.dest + '/' + epochNow + '_' + tableName + '.csv'
    headerPresent = checkHeaderForAttribute(fileName, 'displayName')
    statusCSV = openFileHandler(fileName)

    if headerPresent == False:
        header = 'displayName,' + str(','.join(map(str, topKeyList)))
        for child in sorted(subKeyList.keys()):
            header += ',' + (str(','.join(map(str, subKeyList[child]))))
        statusCSV.write(header + "\n")

    statusCSV.write(processAPIInfo(fetchInfo, allKeys, subKeyList).rstrip(',') + "\n")
    statusCSV.close()


def exec_jobInfo(tableName):
    jobInfoKeys = ['jobTitle', 'reportsTo', 'location', 'division', 'department', 'date']
    fetchInfo = fetchFromAPI(APIPrefix + '/employees/' + str(employeeID) + '/tables/' + tableName, 'json')
    if len(fetchInfo) > 0:
        writeCSVToFile(fetchInfo, tableName, jobInfoKeys, {})


def exec_employmentStatus(tableName):
    statusKeys = ['employmentStatus', 'employeeId', 'date']
    fetchInfo = fetchFromAPI(APIPrefix + '/employees/' + str(employeeID) + '/tables/' + tableName, 'json')
    if len(fetchInfo) > 0:
        writeCSVToFile(fetchInfo, tableName, statusKeys, {})


def exec_emergencyContacts(tableName):
    contactKeys = ['employeeId', 'name', 'relationship', 'homePhone', 'addressLine1', 'addressLine2', 'mobilePhone',
        'email', 'zipcode', 'city', 'state', 'country', 'workPhone', 'workPhoneExtension']
    fetchInfo = fetchFromAPI(APIPrefix + '/employees/' + str(employeeID) + '/tables/' + tableName, 'json')
    if len(fetchInfo) > 0:
        writeCSVToFile(fetchInfo, tableName, contactKeys, {})


def exec_compensation(tableName):
    compKeys = ['type', 'payPeriod', 'employeeId', 'startDate']
    subKeys = {'rate': ['currency', 'value']}
    fetchInfo = fetchFromAPI(APIPrefix + '/employees/' + str(employeeID) + '/tables/' + tableName, 'json')
    if len(fetchInfo) > 0:
        writeCSVToFile(fetchInfo, tableName, compKeys, subKeys)


def exec_customBankDetails(tableName):
    bankKeys = ['employeeId', 'customBankName', 'customAccountNumber']
    fetchInfo = fetchFromAPI(APIPrefix + '/employees/' + str(employeeID) + '/tables/' + tableName, 'json')
    if len(fetchInfo) > 0:
        writeCSVToFile(fetchInfo, tableName, bankKeys, {})


def exec_customRSADetails(tableName):
    rsaKeys = ['employeeId', 'customPFAName', 'customRSANumber']
    fetchInfo = fetchFromAPI(APIPrefix + '/employees/' + str(employeeID) + '/tables/' + tableName, 'json')
    if len(fetchInfo) > 0:
        writeCSVToFile(fetchInfo, tableName, rsaKeys, {})


def exec_employeedependents(tableName):
    depKeys = ['employeeId', 'firstName', 'middleName', 'lastName', 'relationship', 'gender', 'dateOfBirth',
        'addressLine1', 'addressLine2', 'city', 'state', 'zipCode', 'homePhone', 'country', 'isUsCitizen',
        'isStudent']
    fetchInfo = fetchFromAPI(APIPrefix + '/' + tableName + '/?employeeid=' + str(employeeID), 'json')
    if len(fetchInfo['Employee Dependents']) > 0:
        writeCSVToFile(fetchInfo['Employee Dependents'], tableName, depKeys, {})


def downloadDocuments(employeeID):
    spaces = [' ']

    fetchInfo = fetchFromAPI(APIPrefix + '/employees/' + str(employeeID) + '/files/view', 'xml')
    obj = xmltodict.parse(fetchInfo)
    for i in range(len(obj['employee']['category'])):
        catName = obj['employee']['category'][i]['name']
        try:
            for file in obj['employee']['category'][i]['file']:
                rawfilename = str(args.dest + catName + '/' + file['dateCreated'] + '_' + file['name'])
                filename = sub(u'(?u)[' + escape(''.join(spaces)) + ']', '_', rawfilename)
                fetchBinaryFile(APIPrefix + '/employees/' + str(employeeID) + '/files/' + file['@id'] + '/', filename)
                print(filename)
        except KeyError:
            pass


#-----
ids = [46, 40, 51, 671, 787]
#-----

# Key sets
userKeys = ['id', 'address1', 'address2', 'age', 'bestEmail', 'city', 'country', 'dateOfBirth',
    'employeeNumber', 'employmentHistoryStatus', 'firstName', 'fullName1', 'fullName2', 'fullName3', 'fullName4',
    'fullName5', 'gender', 'hireDate', 'homeEmail', 'homePhone', 'jobTitle', 'lastChanged', 'department',
    'lastName', 'location', 'maritalStatus', 'middleName', 'mobilePhone', 'payChangeReason', 'payGroupId', 'payRate',
    'payRateEffectiveDate', 'payType', 'paidPer', 'payPeriod', 'ssn', 'state', 'stateCode', 'supervisor',
    'supervisorEId', 'terminationDate', 'workEmail', 'workPhone', 'workPhonePlusExtension', 'workPhoneExtension',
    'zipcode', 'isPhotoUploaded', 'employmentStatus', 'nickname', 'photoUploaded', 'customBenefitDue', 'division',
    'customBenefitDue', 'customCompany', 'customDateofConfirmation', 'customGrade1', 'customLagosGrade', 'customLevel',
    'customNationalInsuranceNumber', 'customNationality', 'customNHFNumber', 'customNIC', 'customNigeriaMobilePhone',
    'customNon-DomStatus', 'customPakistanMobilePhone', 'customRwandaMobilePhone', 'customStateofOrigin',
    'customTaxIDNumber', 'customUKWorkPermit', 'supervisorId', 'displayName']

# Fetch the list of user IDs
userIDs = []
userIDGet = fetchFromAPI(APIPrefix + '/employees/directory', 'json')
for employee in userIDGet['employees']:
    userIDs.append(employee['id'])

# for employeeID in userIDs:
for employeeID in ids:
    # Do not run for ID 671 - Viv Diwakar
    if employeeID != 671:
        userInfoGet = fetchFromAPI(APIPrefix + '/employees/' + str(employeeID) + '?fields='
            + ','.join(map(str, userKeys)), 'json')
        employee = userInfoGet['displayName']
        writeCSVToFile(userInfoGet, 'employees', userKeys[:-1], {})

        downloadDocuments(employeeID)

        userPicUploaded = fetchFromAPI(APIPrefix + '/employees/' + str(employeeID) + '?fields=isPhotoUploaded', 'json')
        if userPicUploaded['isPhotoUploaded'] == 'true':
            fetchBinaryFile(APIPrefix + '/employees/' + str(employeeID) + '/photo/small',
                sub(',', '', str(args.dest + '/photos/photo_employeeID_' + str(employeeID) + '_'
                    + sub(' ', '_', employee) + '.jpg')))

        for table in userTables:
            locals()[str('exec_' + table)](table)


