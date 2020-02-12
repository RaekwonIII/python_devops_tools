import logging
import sys

from bs4 import BeautifulSoup
from requests import Session, Response
from requests.adapters import BaseAdapter
from requests.structures import CaseInsensitiveDict
import urllib.parse as urlparse

from urllib3 import HTTPResponse

BASE_URL = 'stage.idruide.eu'
SIGNIN_URL_TEMPLATE = 'https://signin.{base_url}/auth/{campaign_token}'
TOKEN_EXCHANGE_URL_TEMPLATE = 'https://auth.{base_url}/auth/realms/idruide/protocol/openid-connect/token'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)


class SMMAdapter(BaseAdapter):

    def __init__(self):
        super(BaseAdapter, self).__init__()
        self.logger = logging.getLogger("SMMAdapter")

    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        response = Response()
        # Fallback to None if there's no status_code, for whatever reason.
        response.status_code = 200

        # Make headers case-insensitive.
        response.headers = CaseInsensitiveDict(getattr(request, 'headers', {}))
        self.logger.info('Request URL: %s', request.url)
        # Set encoding.
        parsed = urlparse.urlparse(request.url)
        # very hardcoded, but we know we have 'token' in the URL and we know it's only one...
        response._content = urlparse.parse_qs(parsed.query).get('token').pop()
        self.logger.info('Response token: %s', response.content)
        response.raw = HTTPResponse(body=request.url, status=200)
        response.reason = '???'

        if isinstance(request.url, bytes):
            response.url = request.url.decode('utf-8')
        else:
            response.url = request.url

        # Give the Response some context.
        response.request = request
        response.connection = self
        return response

    def close(self):
        pass


class DrawBridgeClient(object):

    def __init__(self, signin_url, token_exchange_url, logger=None):
        self.signin_url = signin_url
        self.token_exchange_url = token_exchange_url
        self.jwt_token = None
        self.access_token = None
        self.logger = logger

    @classmethod
    def from_signing_token_exchange_url(cls, signin_url, token_exchange_url):
        logger = logging.getLogger("DrawBridge Client")
        return cls(signin_url, token_exchange_url, logger)

    @classmethod
    def from_env_and_campaign(cls, url, campaign_token):
        signin_url = SIGNIN_URL_TEMPLATE.format(base_url=url, campaign_token=campaign_token)
        token_exchange_url = TOKEN_EXCHANGE_URL_TEMPLATE.format(base_url=url)
        logger = logging.getLogger("DrawBridge Client")
        return cls(signin_url, token_exchange_url, logger)

    def get_jwt_token(self, credentials):
        if not self.jwt_token:
            self.logger.info('No JWT token found, creating new one')
            with Session() as s:
                self.logger.info('Instantiating special adapter for SMM URL schema')
                s.mount("smm://", SMMAdapter())
                r = s.get(self.signin_url)

                soup = BeautifulSoup(r.text, 'html.parser')
                keys = [i.get('id') for i in soup.body.form.find_all('input') if i.get('type') != 'submit']
                payload = {k: credentials.get(k) for k in keys}

                self.logger.info('Login payload keys: %s', payload.keys())
                post_url = soup.body.form.get('action')
                r = s.post(post_url, data=payload)
                self.jwt_token = r.content
                self.logger.info('JWT token received: \n %s', self.jwt_token)

        return self.access_token

    def get_access_token(self):
        if not self.access_token:
            self.logger.info('No Access token found, creating new one')
            with Session() as s:
                payload = {
                    "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                    "client_id": "druidoo",
                    "subject_token": self.jwt_token,
                    "subject_issuer": "drawbridge"
                }
                r = s.post(self.token_exchange_url, data=payload)
                self.access_token = r.json().get('access_token')
                self.logger.info('Access token received: \n %s', self.access_token)

        return self.access_token
