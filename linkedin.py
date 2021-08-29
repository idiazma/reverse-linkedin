from requests import Session, Response, Request
from urllib.parse import urlencode, quote
from bs4 import BeautifulSoup
import json, os, requests, mysql.connector, math

BASE_URL = 'www.linkedin.com'

HOST = '192.168.1.105'

DATABASE = 'productos'

USER = 'admin'

PASSWORD = 'admin'

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'es-419,es;q=0.9',
    'Origin': f'https://{BASE_URL}',
    'Sec-Fetch-User': '?1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'X-Li-User-Agent': 'LIAuthLibrary:3.2.4 com.linkedin.LinkedIn:8.8.1 iPhone:8.3',
    'X-Li-Lang': 'es_ES',
    'x-li-prefetch': '1',
    'x-restli-protocol-version': '2.0.0',
    'User-Agent': 'LinkedIn/8.8.1 CFNetwork/711.3.18 Darwin/14.0.0',
}

# Functions
def set_session_cookies(_session, cookies):
    for cookie in cookies:
        if 'httpOnly' in cookie:
            cookie['rest'] = {'httpOnly': cookie.pop('httpOnly')}
        if 'expiry' in cookie:
            cookie['expires'] = cookie.pop('expiry')
        if 'sameSite' in cookie:
            cookie.pop('sameSite')
        _session.cookies.set(**cookie)

def get_session_cookies(session, filters=[]):
    cookies = []
    if session:
        for cookie in session.cookies:
            if filters and cookie.name not in filters:
                continue
            cookie_dict = {'name': cookie.name, 'value': cookie.value}
            if cookie.domain:
                cookie_dict['domain'] = cookie.domain
            cookies.append(cookie_dict)
    return cookies

# LinkedIn
def Auth(_session, creds={}):
    url = f'https://{BASE_URL}'
    request = Request('GET', url=url, headers=headers)
    req = _session.prepare_request(request)
    response = _session.send(req)

    url = f'https://{BASE_URL}/litms/api/metadata/user'
    request = Request('GET', url=url, headers=headers)
    req = _session.prepare_request(request)
    response = _session.send(req)

    url = f'https://{BASE_URL}/uas/login-submit'
    request = Request('POST', url=url, data={
        'loginCsrfParam': _session.cookies.get_dict()['bcookie'].replace('v=2&', '').strip('"'),
        'session_key': creds['username'],
        'session_password': creds['password'],
    }, headers=headers)
    req = _session.prepare_request(request)
    response = _session.send(req)
    
    return _session

def Employees(_session, keywords='python', count=10, offset=0, country=1):
    filters = {
        'applyWithLinkedin': 'true',
        'geoUrn': 'urn:li:fs_geo:92000001' if country == 1 else 'urn:li:fs_geo:101174742',
        'locationFallback': 'Remoto' if country == 1 else 'Canadá',
        'sortBy': 'DD',
        'resultType': 'JOBS'
    }
    params = {
        'decorationId': 'com.linkedin.voyager.deco.jserp.WebJobSearchHitWithSalary-23',
        'count': count,
        'start': offset,
        'filters': f"""List({','.join([f'{b}->{filters[b]}' for b in filters.keys()])})""",
        'keywords': keywords,
        'q': 'jserpFilters',
        'origin': 'JOB_SEARCH_PAGE_LOCATION_AUTOCOMPLETE',
        'queryContext': 'List(primaryHitType->JOBS,spellCorrectionEnabled->true)',
    }
    url = f"https://{BASE_URL}/voyager/api/search/hits?{urlencode(params, safe='(),')}"
    request = Request('GET', url=url, headers={**headers,
        'Csrf-Token': _session.cookies.get_dict()['JSESSIONID'].strip('"'),
        'Accept': 'application/vnd.linkedin.normalized+json+2.1',
    })
    req = _session.prepare_request(request)
    response = _session.send(req)

    return json.loads(response.content)

def JobDetails(_session, job_id='python'):
    url = f'https://{BASE_URL}/voyager/api/jobs/jobPostings/{job_id}'
    request = Request('GET', url=url, params={
        'decorationId': 'com.linkedin.voyager.deco.jobs.web.shared.WebFullJobPosting-60',
        'topN': 1,
    }, headers={**headers,
        'Csrf-Token': _session.cookies.get_dict()['JSESSIONID'].strip('"'),
        'Accept': 'application/vnd.linkedin.normalized+json+2.1',
    })
    req = _session.prepare_request(request)
    response = _session.send(req)

    return json.loads(response.content)

def ResumeDetails(_session, count=5):
    url = f'https://{BASE_URL}/voyager/api/jobs/resumes'
    request = Request('GET', url=url, params={
        'decorationId': 'com.linkedin.voyager.deco.jobs.web.WebFullResume-5',
        'count': count,
        'q': 'member',
    }, headers={**headers,
        'Csrf-Token': _session.cookies.get_dict()['JSESSIONID'].strip('"'),
        'Accept': 'application/vnd.linkedin.normalized+json+2.1',
    })
    req = _session.prepare_request(request)
    response = _session.send(req)

    return json.loads(response.content)

def PostApplyPromo(_session, urn):
    url = f'https://{BASE_URL}/voyager/api/jobs/postApplyPromo'
    request = Request('GET', url=url, params={
        'decorationId': 'com.linkedin.voyager.deco.jobs.web.WebPostApplyPromoCard-14',
        'jobPosting': urn,
        'q': 'jobPosting',
        'screenContext': 'JOBS_DETAIL_STAND_OUT_CAROUSEL',
    }, headers={**headers,
        'Csrf-Token': _session.cookies.get_dict()['JSESSIONID'].strip('"'),
        'Accept': 'application/vnd.linkedin.normalized+json+2.1',
    })
    req = _session.prepare_request(request)
    response = _session.send(req)

    return json.loads(response.content)

def EasyApplicationJob(_session, job_id='python'):
    url = f'https://{BASE_URL}/voyager/api/jobs/easyApplyForms'
    request = Request('GET', url=url, params={
        'jobPostingUrn': job_id,
        'q': 'jobPosting',
    }, headers={**headers,
        'Csrf-Token': _session.cookies.get_dict()['JSESSIONID'].strip('"'),
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'x-requested-with': 'XMLHttpRequest'
    })
    req = _session.prepare_request(request)
    response = _session.send(req)

    return json.loads(response.content)

def SubmitApplication(_session, responses=[]):
    url = f'https://{BASE_URL}/voyager/api/jobs/easyApplyForms?action=submitApplication'
    request = Request('POST', url=url, data=str.encode(json.dumps({
        'followCompany': False,
        'referenceId': 'INITIAL_PAGE_LOAD::::84ff5aa8-0d48-44db-ac30-e7a0b5dba0db',
        'responses': responses,
        'trackingCode': 'TRK_INITIAL_PAGE_LOAD',
    })), headers={**headers,
        'Csrf-Token': _session.cookies.get_dict()['JSESSIONID'].strip('"'),
        'Accept': 'application/json, text/javascript, */*; q=0.01',
    })
    req = _session.prepare_request(request)
    response = _session.send(req)

    return json.loads(response.content)

# SQL
def DiccionarioSQL(Select):
    cnxn = mysql.connector.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, charset='latin1', auth_plugin='mysql_native_password')
    cursor = cnxn.cursor(buffered=True)
    cursor.execute(Select)
    cnxn.commit()
    Header, Respuesta = [column[0] for column in cursor.description], []
    for idx, Rows in enumerate(list(cursor.fetchall())) or []:
        Resultado = [x for x in Rows]
        Lista = {y: Resultado[idy] for idy, y in enumerate(Header)}
        Respuesta.append(Lista)
    cnxn.close()
    return Respuesta

def DiccionarioStore(store, values):
    cnxn = mysql.connector.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, charset='latin1', auth_plugin='mysql_native_password')
    cursor = cnxn.cursor(buffered=True)
    cursor.callproc(str(store), values)
    for result in cursor.stored_results():
        description = result.description
        fetch = result.fetchall()
    Header, Respuesta = [column[0] for column in description], []
    for idx, Rows in enumerate(list(fetch)) or []:
        Resultado = [x for x in Rows]
        Lista = {y: Resultado[idy] for idy, y in enumerate(Header)}
        Respuesta.append(Lista)
    cnxn.close()
    return Respuesta

def Execute(Update, Multi=False):
    cnxn = mysql.connector.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, auth_plugin='mysql_native_password')
    cursor = cnxn.cursor()
    cursor.execute(Update)
    cnxn.commit()
    cursor.close()
    cnxn.close()

def deEmojify(text):
    regrex_pattern = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F" 
        u"\U0001F300-\U0001F5FF" 
        u"\U0001F680-\U0001F6FF" 
        u"\U0001F1E0-\U0001F1FF" 
                           "]+", flags = re.UNICODE)
    return regrex_pattern.sub(r'', str(text))

import re
def InsertarTabla(Todo, Tabla = ''):
    if len(Todo)>0:
        Corte = 500
        SubA = DiccionarioSQL(f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_NAME = '{Tabla}'")
        SubA = [a for a in SubA if a['COLUMN_NAME'] in Todo[0].keys()]
        def NonLatin(text): 
            return deEmojify(text.replace("'", '').replace("’", '').replace("’", '').replace("“", '').replace("”", ''))
        Residuo, Paso, Pasos = len(Todo)%Corte, 0, math.floor(len(Todo)/Corte)
        while True:
            Siguiente = Paso*Corte + (Corte if Paso!=Pasos else Residuo)
            Grupo = Todo[Paso*Corte:Siguiente]; Paso+=1;
            if Grupo == []: break;
            def Formato(a, b):
                SubB = [c for c in SubA if c['COLUMN_NAME'] == b['COLUMN_NAME']]
                b = a[b['COLUMN_NAME']] 
                if b == None or b == 'None': return 'NULL'
                if len(SubB)==0: return "'" + str(b).replace("'", "") + "'" if str(b)!='' else 'NULL'
                if SubB[0]['DATA_TYPE'] not in ('varchar','char','datetime'): return "'" + str(b).replace("'", "").replace("\\", "") + "'" if str(b)!='' else 'NULL'
                if SubB[0]['DATA_TYPE'] in ('datetime'): return "'" + str(b).replace("'", "").replace("\\", "") + "'" if str(b)!='' and str(b)!='0000-00-00 00:00:00' else 'NULL'
                return "'" + NonLatin(str(str(b)[:int(SubB[0]['CHARACTER_MAXIMUM_LENGTH'])]).replace('"', "").replace("'", "").replace("\\", "")) + "'" if str(b)!='' else 'NULL'
            Execute("INSERT INTO {0}({1}) VALUES {2};".format(Tabla, ",".join([str(a['COLUMN_NAME']) for a in SubA]), ", ".join(["({0})".format(" ,".join([Formato(a, b) for b in SubA])) for a in Grupo])))
            if Paso == Pasos+1: break
    else: pass
