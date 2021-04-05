import os
import sys
import json
#import uuid
from random import choice
from datetime import datetime
import urllib.request
from pyqldb.driver.qldb_driver import QldbDriver

def generate_data( qldb_ledger, region='us-east-2' ):
    persons = None
    cases = None
    exhibits = None
    movements = None

    persons = get_data( 'Person' )
    cases = get_data( 'Case' )
    exhibits = get_data( 'Exhibit' )
    movements = get_data( 'Movement' )
    
    insert_person_data( qldb_ledger, region, persons )
    insert_case_data( qldb_ledger, region, persons, cases )
    insert_exhibit_data( qldb_ledger, region, cases, exhibits )
    insert_movement_data( qldb_ledger, region, persons, exhibits, movements )
    
def insert_person_data( qldb_ledger, region, persons ):
    for person in persons:
        person_id = insert_data( qldb_ledger, region, 'PERSON', person )
        person['id'] = person_id
        #print( 'Person', person )

def insert_case_data( qldb_ledger, region, persons, cases ):
    for case in cases:
        person = choice( persons )
        case['LeadInvestigator'] = person['id']
        case_id = insert_data( qldb_ledger, region, 'CASE', case )
        case['id'] = case_id
        #print( 'Case', case )

def insert_exhibit_data( qldb_ledger, region, cases, exhibits ):
    for exhibit in exhibits:
        case = choice( cases )
        exhibit['CaseId'] = case['id']
        exhibit_id = insert_data( qldb_ledger, region, 'EXHIBIT', exhibit )
        exhibit['id'] = exhibit_id
        #print( 'Exhibit', exhibit )

def insert_movement_data( qldb_ledger, region, persons, exhibits, movements ):
    for movement in movements:
        exhibit = choice( exhibits )
        person = choice( persons )
        movement['ExhibitId'] = exhibit['id']
        movement['IssuedTo'] = person['id']
        insert_data( qldb_ledger, region, 'MOVEMENT', movement )
        #print( 'Movement', movement )
        

def get_data( schema ):
    data = []

    try:    
      with open( f'{schema}.json') as f:
        data = json.load(f)
    except Exception as e:
        print( "Error Loading Data", e)
        
    return data
    
def create_qldb_driver(ledger_name, region_name, endpoint_url=None, boto3_session=None):
    qldb_driver = QldbDriver(ledger_name=ledger_name, region_name=region_name, endpoint_url=endpoint_url,
                            boto3_session=boto3_session)
    return qldb_driver    
    
def insert_data( qldb_ledger, region, record_type, record ):
    return_id = None
    
    with create_qldb_driver(qldb_ledger, region) as driver:
        if record_type == 'PERSON':
            return_id = driver.execute_lambda(lambda executor: insert_person( executor, record ) )
        elif record_type == 'CASE':
            return_id = driver.execute_lambda(lambda executor: insert_case( executor, record ) )
        elif record_type == 'EXHIBIT':
            return_id = driver.execute_lambda(lambda executor: insert_exhibit( executor, record ) )
        elif record_type == 'MOVEMENT':
            return_id = driver.execute_lambda(lambda executor: insert_movement( executor, record ) )
            
    return return_id            
    #return str( uuid.uuid4() )

def insert_person( transaction_executor, record ):
    
    query = "INSERT INTO Person ?"
    
    person_id = None
    
    try:
        person = {
            'PersonId': f'{record["PersonId"]}',
            'FirstName': f'{record["FirstName"]}',
            'LastName': f'{record["LastName"]}',
            'DOB': f'{record["DOB"]}',
            'PhoneNo': f'{record["PhoneNo"]}',
            'Email': f'{record["Email"]}',
            'GovId': f'{record["GovId"]}',
            'GovIdType': f'{record["GovIdType"]}',
            'Address': f'{record["Address"]}',
            'Role': f'{record["Role"]}'
        }

        cursor = transaction_executor.execute_statement(query, person)
        
        for doc in cursor:
            person_id = doc[ 'documentId' ]
            
    except Exception as e:
        print( 'Error Inserting Person Record', e )

    return person_id
    
def insert_case( transaction_executor, record ):
    
    query = "INSERT INTO CaseFile ?"
    
    case_id = None
    
    try:
        case = {
        'CaseTitle': f'{record["CaseTitle"]}',
        'CaseDate': f'{record["CaseDate"]}',
        'Description': f'{record["Description"]}',
        'LeadInvestigator': f'{record["LeadInvestigator"]}',
        'Location': f'{record["Location"]}',
        'IncidentType': f'{record["IncidentType"]}',
        'CaseClosed': record["CaseClosed"]
        }

        cursor = transaction_executor.execute_statement(query, case)
        
        for doc in cursor:
            case_id = doc[ 'documentId' ]
            
    except Exception as e:
        print( 'Error Inserting Case Record', e )

    return case_id
   
def insert_exhibit( transaction_executor, record ):
    query = "INSERT INTO Exhibit ?"
    
    exhibit_id = None
    
    try:
        exhibit = {
        'CaseId': f'{record["CaseId"]}',
        'Name': f'{record["Name"]}',
        'Description': f'{record["Description"]}',
        'DocumentType': f'{record["DocumentType"]}',
        'BucketURL': f'{record["BucketURL"]}',
        'Hash': f'{record["Hash"]}'
        }

        cursor = transaction_executor.execute_statement(query, exhibit)
        
        for doc in cursor:
            exhibit_id = doc[ 'documentId' ]
            
    except Exception as e:
        print( 'Error Inserting Exhibit Record', e )

    return exhibit_id
    
def insert_movement( transaction_executor, record ):
            
    query = "INSERT INTO Movement ?"
    
    movement_id = None
    
    try:
        issue_date = datetime.now().strftime( '%Y-%m-%d' )
        movement = {
            'ExhibitId': f'{record["ExhibitId"]}',
            'IssueDate': f'{issue_date}',
            'IssuedTo': f'{record["IssuedTo"]}'
        }  

        cursor = transaction_executor.execute_statement(query, movement)
        
        for doc in cursor:
            movement_id = doc[ 'documentId' ]
            
    except Exception as e:
        print( 'Error Inserting Exhibit Record', e )

    return movement_id

if __name__ == '__main__':
    
    if len( sys.argv ) < 2:
        print( 'Please provide ledger name. e.g. python DataGen.py QLDBImmersionDay')
    else:
        qldb_ledger = sys.argv[1]
        #print( qldb_ledger )
        generate_data( qldb_ledger )