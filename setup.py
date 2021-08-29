from requests import Session
import linkedin, json, time, re
from datetime import datetime

global session

def AuthSession():
    session = Session()
    session.headers = linkedin.headers
    creds = json.loads(open(f'{os.getcwd()}/creds.json', 'rb').read())
    session = linkedin.Auth(session, creds)
    variable = {'session_info': linkedin.get_session_cookies(session)}
    print(json.dumps(variable))

def StartSession():
    session = Session()
    session.headers = linkedin.headers
    linkedin.set_session_cookies(session, json.loads(open('creds.json').read())['session_info'])
    return session

def EmployeeSession(labeled='data engineer', number=200, country=1):
    black, total, wait = [], 0, 1.5
    log = []
    while total < number:
        # Employees
        employees = [a for a in linkedin.Employees(session, labeled, count=50, offset=total, country=country)['included'] if 'jobPostingId' in a.keys()]
        time.sleep(wait*4)
        if total in log: break
        log.append(total)
        print('Circulo:', total, labeled)
        for iditem, item in enumerate(employees):
            responses = []
            if item['jobPostingId'] in black: continue
            #Antiguos
            repeat = linkedin.DiccionarioSQL(f"SELECT ID FROM planta_empleo_posting WHERE JOB_POSTING_ID = '{item['jobPostingId']}'")
            if len(repeat)>0:
                time.sleep(wait)
                continue
            details = linkedin.JobDetails(session, item['jobPostingId'])
            job_format = linkedin.EasyApplicationJob(session, item['entityUrn'])
            company = [a for a in details['included'] if 'universalName' in a.keys()]
            empresa = company[0]['name'] if len(company)>0 else 'Confidencial'
            # Empresa repetida
            if empresa in ('Perficient', 'EPAM Anywhere', 'Mozilla', 'EPAM Systems', 'DoorDash', 'Optello', 'Jobot', 'Cypress HCM', 'Paper', 'Crossover for Work', 'Applicantz', 'Yalo'):
                time.sleep(wait)
                continue
            # Empresa
            try: 
                position = {
                    'COMPANY_NAME': company[0]['name'] if len(company)>0 else '',
                    'COMPANY_DESCRIPTION': (linkedin.deEmojify(company[0]['description']) if 'description' in company[0].keys() else '') if len(company)>0 else '',
                    'COMPANY_ID': (company[0]['universalName'] if 'universalName' in company[0].keys() else '') if len(company)>0 else '',
                    'COMPANY_URN': (company[0]['entityUrn'] if 'entityUrn' in company[0].keys() else '') if len(company)>0 else '', 
                    'COMPANY_URL': (company[0]['url'] if 'url' in company[0].keys() else '') if len(company)>0 else '', 
                    'JOB_POSTING_ID': item['jobPostingId'],
                    'JOB_STATE': item['jobState'],
                    'JOB_TITLE': item['title'],
                    'JOB_REMOTE': item['workRemoteAllowed'],
                    'JOB_LOCATION': item['formattedLocation'],
                    'JOB_EXPIRED': datetime.fromtimestamp(item['expireAt']/1000).strftime('%Y-%m-%d %H:%M:%S'),
                    'JOB_LISTED': datetime.fromtimestamp(item['listedAt']/1000).strftime('%Y-%m-%d %H:%M:%S'),
                    'JOB_EMPLOYMENT_STATUS': details['data']['employmentStatus'].replace('urn:li:fs_employmentStatus:', ''),
                    'JOB_OWNER_ENABLED': details['data']['ownerViewEnabled'],
                    'JOB_POSTING_URL': details['data']['jobPostingUrl'],
                    'JOB_APPLIES': details['data']['applies'],  
                    'JOB_ORIGINAL': datetime.fromtimestamp(details['data']['originalListedAt']/1000).strftime('%Y-%m-%d %H:%M:%S'),
                    'JOB_REMOTE_ALLOWED': details['data']['workRemoteAllowed'],
                    'JOB_EASY_APPLY': 'easyApplyUrl' in details['data']['applyMethod'].keys(),
                    'JOB_COUNTRY': details['data']['country'],
                    'JOB_EMPLOYMENT': details['data']['formattedEmploymentStatus'],
                    'JOB_REGION': details['data']['jobRegion'],
                    'JOB_VIEWS': details['data']['views'], 
                    'JOB_DESCRIPTION': linkedin.deEmojify(details['data']['description']['text'].replace('"', '').replace("'", '')),
                }
            except: print('error'); continue
            try: linkedin.InsertarTabla([position], 'planta_empleo_posting')
            except: black.append(item['jobPostingId']); continue
            # Elements
            error = False
            for element in job_format['elements']:
                # Question groups
                for group in  element['questionGroupings']:
                    # Form elements
                    for forms in group['formSection']['formElementGroups']:
                        # Question element
                        for simple in forms['formElements']:
                            question = {'formElementUrn': simple['urn']}
                            # Resumen CV
                            if simple['type'] in ('AMBRY_MEDIA'):
                                # Priting
                                linkedin.InsertarTabla([{
                                    'JOB': item['jobPostingId'],
                                    'JOB_TYPE': group['type'],
                                    'TYPE': simple['type'],
                                    'QUESTION': simple['title']['text'],
                                    'URN': simple['urn'],
                                }], 'planta_empleo_questions')
                                # Database
                                resume = linkedin.ResumeDetails(session, 1)['included'][0]
                                question['ambryMediaResponse'] = resume['entityUrn'].replace('urn:li:fs_resume:/', 'urn:li:ambryBlob:/')
                                question['fileData'] = {
                                    'attachmentClass': 'ui-attachment--pdf',
                                    'createdAt': resume['createdAt'],
                                    'documentName': resume['fileName'],
                                    'downloadUrl': resume['downloadUrl'],
                                    'lastUsedAt': resume['lastUsedAt'],
                                    'type': 'PDF',
                                }
                            # Pregunta texto/numero
                            elif simple['type'] in ('SINGLE_LINE_TEXT', 'MULTI_LINE_TEXT'):
                                # Priting
                                linkedin.InsertarTabla([{
                                    'JOB': item['jobPostingId'],
                                    'JOB_TYPE': group['type'],
                                    'TYPE': simple['type'],
                                    'QUESTION': simple['title']['text'],
                                    'URN': simple['urn'],
                                }], 'planta_empleo_questions')
                                # Database
                                max_line = int(simple['validCharacterCountRange']['start']) if 'validCharacterCountRange' in simple.keys() else 1000
                                line_text = simple['title']['text'].replace('"', '').replace("'", '')
                                try: respuesta = linkedin.DiccionarioSQL(f"""SELECT * FROM planta_empleo_responses WHERE QUESTION = "{line_text}" AND RESPONSE IS NOT NULL""")
                                except: respuesta = []
                                if len(respuesta) == 0: error = True; continue
                                respuesta = respuesta[0]
                                question['textResponse'] = respuesta['RESPONSE']
                            # Pregunta Combo, Radio, Checkbox
                            elif simple['type'] in ('DROPDOWN', 'RADIO', 'CHECKBOX'):
                                # Priting
                                line_text = simple['title']['text'].replace('"', '').replace("'", '')
                                options = [{'value': a['value']} if 'valueUrn' not in a.keys() else {'value': a['value'], 'valueUrn': a['valueUrn']} for a in simple['selectableOptions']]
                                max_line = int(simple['validCharacterCountRange']['start']) if 'validCharacterCountRange' in simple.keys() else 1000
                                for option in options:
                                    if not (simple['title']['text'] not in ['Código del país'] or (simple['title']['text'] in ['Código del país'] and option['value'] in ['51'])): continue
                                    linkedin.InsertarTabla([{
                                        'JOB': item['jobPostingId'],
                                        'JOB_TYPE': group['type'],
                                        'TYPE': simple['type'],
                                        'QUESTION': line_text,
                                        'URN': simple['urn'],
                                        'VALUE': option['value'],
                                        'VALUE_URN': option['valueUrn'] if 'valueUrn' in option.keys() else '',
                                    }], 'planta_empleo_questions')
                                # Database 
                                line_text = simple['title']['text'].replace('"', '').replace("'", '').replace("’", '')
                                try: respuesta = linkedin.DiccionarioSQL(f"""SELECT * FROM planta_empleo_responses WHERE TYPE = "{simple['type']}" AND QUESTION = "{line_text}" AND RESPONSE IS NOT NULL""")
                                except: respuesta = []
                                if len(respuesta) == 0: error = True; continue
                                respuesta = respuesta[0]
                                question['selectedValuesResponse'] = [{'value': respuesta['RESPONSE']} if simple['title']['text'] not in ['Código del país'] else {'value': '51', 'valueUrn': 'urn:li:country:pe'}]
                            # Pregunta Date Range
                            elif simple['type'] in ('DATE_RANGE'):
                                line_option = simple['type'], simple['title']['text'].replace('"', '').replace("'", '')
                                linkedin.InsertarTabla([{
                                    'JOB': item['jobPostingId'],
                                    'JOB_TYPE': group['type'],
                                    'TYPE': simple['type'],
                                    'QUESTION': line_option,
                                    'URN': simple['urn'],
                                }], 'planta_empleo_questions')
                            # Upload
                            responses.append(question)
            # Send employment
            if error:
                time.sleep(wait)
                print(f'Error 1: {iditem + total}', company[0]['name'] if len(company)>0 else 'Confidencial', '------', item['title'], item['jobPostingId'])
                continue
            send = linkedin.SubmitApplication(session, responses)
            if 'value' not in send.keys():
                time.sleep(wait)
                print(f'Error 2: {iditem + total}', company[0]['name'] if len(company)>0 else 'Confidencial', '------', item['title'], item['jobPostingId'])
                continue
            # Update
            send = {'APPLIED': str(send['value']['applied']), 'APPLIED_URN': send['value']['entityUrn'], 'APPLIED_AT': datetime.fromtimestamp(send['value']['appliedAt']/1000).strftime('%Y-%m-%d %H:%M:%S')}
            print(f'OK: {iditem + total}', company[0]['name'] if len(company)>0 else 'Confidencial', '------', item['title'], item['jobPostingId'])
            linkedin.Execute(f"UPDATE planta_empleo_posting SET APPLIED = '{send['APPLIED']}', APPLIED_URN = '{send['APPLIED_URN']}', APPLIED_AT = '{send['APPLIED_AT']}' WHERE JOB_POSTING_ID = '{item['jobPostingId']}'")
            time.sleep(wait)
            
        total += len(employees)

def LoopSession():
    while True:
        #Canada
        EmployeeSession('SQL', 50, 2)
        EmployeeSession('Data Engineer', 50, 2)
        EmployeeSession('ETL', 50, 2)
        EmployeeSession('Data Mining', 50, 2)
        EmployeeSession('Web Scraping', 50)
        EmployeeSession('RPA', 20, 2)
        EmployeeSession('Python', 50, 2)
        #Remote
        EmployeeSession('SQL')
        EmployeeSession('Data Engineer')
        EmployeeSession('ETL', 150)
        EmployeeSession('Data Mining', 150)
        EmployeeSession('RPA', 20)
        EmployeeSession('Python')

session = StartSession()
LoopSession()
