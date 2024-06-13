#!/usr/bin/python

from bs4 import BeautifulSoup
from requests_ntlm import HttpNtlmAuth
import base64
import boto3
import configparser
import json
import os
import requests
import ssl
import sys
import xml.etree.ElementTree as ET

requests.packages.urllib3.disable_warnings()


def getCreds(username=None,
             password=None,
             role=None,
             principal=None,
             profile='saml',
             sessionDurationSeconds=3600,
             logging=True,
             writeToCredsFile=True,
             idpentryurl=None,
             saml2attribute_root_iter=None,
             saml2attribute_name=None,
             saml2attribute_iter=None
             ):
    try:
        _create_unverified_https_context = ssl._create_unverified_context
    except AttributeError:
        pass
    else:
        ssl._create_default_https_context = _create_unverified_https_context

    region = 'us-west-2'
    outputformat = 'json'
    home = os.path.expanduser('~')
    sslverification = False
    idpentryurl = idpentryurl
    if username is None or password is None:
        sys.exit('Username or Password were not passed')

    session = requests.Session()
    session.auth = HttpNtlmAuth(username, password, session)
    headers = {'User-Agent': 'Mozilla/5.0 (compatible, MSIE 11, Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko'}
    response = session.get(idpentryurl, verify=sslverification, headers=headers)

    username = '##############################################'
    password = '##############################################'
    del username
    del password
    soup = BeautifulSoup(response.content, "html.parser")
    assertion = ''

    for inputtag in soup.find_all('input'):
        if (inputtag.get('name') == 'SAMLResponse'):
            assertion = inputtag.get('value')

    awsroles = []
    root = ET.fromstring(base64.b64decode(assertion))

    for saml2attribute in root.iter(saml2attribute_root_iter):
        if (saml2attribute.get('Name') == saml2attribute_name):
            for saml2attributevalue in saml2attribute.iter(saml2attribute_iter):
                awsroles.append(saml2attributevalue.text)

    for awsrole in awsroles:
        chunks = awsrole.split(',')
        if 'saml-provider' in chunks[0]:
            newawsrole = chunks[1] + ',' + chunks[0]
            index = awsroles.index(awsrole)
            awsroles.insert(index, newawsrole)
            awsroles.remove(awsrole)

    if role is not None and principal is not None:
        role_arn = role
        principal_arn = principal

    elif len(awsroles) > 1:
        i = 0
        print('Please choose the role you would like to assume:')
        for awsrole in awsroles:
            print('[', i, ']: ', awsrole.split(',')[0])
            i += 1

        selectedroleindex = 2

        if profile == 'saml-prd':
            selectedroleindex = 8

        if int(selectedroleindex) > (len(awsroles) - 1):
            print('You selected an invalid role index, please try again')
            sys.exit(0)

        role_arn = awsroles[int(selectedroleindex)].split(',')[0]
        principal_arn = awsroles[int(selectedroleindex)].split(',')[1]

    else:
        role_arn = awsroles[0].split(',')[0]
        principal_arn = awsroles[0].split(',')[1]

    client = boto3.client('sts')
    token = client.assume_role_with_saml(RoleArn=role_arn,
                                         PrincipalArn=principal_arn,
                                         SAMLAssertion=assertion,
                                         DurationSeconds=sessionDurationSeconds)
    credentials = token['Credentials']

    awsFolder = os.path.join(home, '.aws')
    credsFile = os.path.join(awsFolder, 'credentials')
    configFile = os.path.join(awsFolder, 'config')
    configParser = configparser.RawConfigParser()

    if not os.path.exists(awsFolder):
        os.makedirs(awsFolder)

    if not os.path.exists(credsFile):
        f = open(credsFile, 'w+')

    if not os.path.exists(configFile):
        f = open(configFile, 'w+')

    configParser.read(configFile)
    if not configParser.has_section('default'):
        configParser.add_section('default')
        configParser.set('default', 'output', outputformat)
        configParser.set('default', 'region', region)
        with open(configFile, 'w+') as f:
            configParser.write(f)

    if writeToCredsFile:
        credsParser = configparser.RawConfigParser()
        credsParser.read(credsFile)
        if not credsParser.has_section(profile):
            credsParser.add_section(profile)

        credsParser.set(profile, 'output', outputformat)
        credsParser.set(profile, 'region', region)
        credsParser.set(profile, 'aws_access_key_id', credentials['AccessKeyId'])
        credsParser.set(profile, 'aws_secret_access_key', credentials['SecretAccessKey'])
        credsParser.set(profile, 'aws_session_token', credentials['SessionToken'])

        with open(credsFile, 'w+') as file:
            credsParser.write(file)

    if logging:
        print('Profile:', profile + ', Expiration:', credentials['Expiration'])

    return credentials


def aws_config():
    config = configparser.RawConfigParser()

    home = os.path.expanduser('~')
    credentials_file = home + '/.awsauth/config'
    config.read(credentials_file)

    username = config.get('dev', 'username')
    password = config.get('dev', 'password')
    idpentryurl = config.get('dev', 'idpentryurl')
    saml2attribute_root_iter = config.get('dev', 'saml2attribute_root_iter')
    saml2attribute_name = config.get('dev', 'saml2attribute_name')
    saml2attribute_iter = config.get('dev', 'saml2attribute_iter')

    getCreds(username=username,
             password=password,
             profile='saml',
             idpentryurl=idpentryurl,
             saml2attribute_root_iter=saml2attribute_root_iter,
             saml2attribute_name=saml2attribute_name,
             saml2attribute_iter=saml2attribute_iter)
    getCreds(username=username,
             password=password,
             profile='saml-prd',
             idpentryurl=idpentryurl,
             saml2attribute_root_iter=saml2attribute_root_iter,
             saml2attribute_name=saml2attribute_name,
             saml2attribute_iter=saml2attribute_iter)
