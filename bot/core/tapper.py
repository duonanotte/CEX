import asyncio
import sys
import random
import aiohttp
import hashlib
import os
import json
import aiofiles
import datetime
import traceback
import pytz
import datetime

from datetime import time, timedelta
from aiohttp import ClientSession
from aiocfscrape import CloudflareScraper
from aiohttp_proxy import ProxyConnector
from better_proxy import Proxy
from time import time
from typing import Tuple
from urllib.parse import unquote, quote
from random import randint, uniform
from pyrogram import Client
from pyrogram.errors import Unauthorized, UserDeactivated, AuthKeyUnregistered, FloodWait
from pyrogram.raw import functions
from pyrogram.raw.functions.messages import RequestWebView
from time import time

from bot.config import settings
from bot.utils import logger
from bot.exceptions import InvalidSession
from bot.core.agents import generate_random_user_agent
from bot.config import settings
from bot.utils.connection_manager import connection_manager
from .headers import headers


import logging
logging.getLogger("pyrogram").setLevel(logging.ERROR)

api_profile = 'https://app.cexptap.com/api/v2/getUserInfo/'  # POST
api_convert = 'https://app.cexptap.com/api/v2/convert/'  # POST
api_claimBTC = 'https://app.cexptap.com/api/v2/claimCrypto/'  # POST
api_tap = 'https://app.cexptap.com/api/v2/claimMultiTaps'  # POST
api_data = 'https://app.cexptap.com/api/v2/getGameConfig'  # post
api_priceData = 'https://app.cexptap.com/api/v2/getConvertData'  # post
api_claimRef = 'https://app.cexptap.com/api/v2/claimFromChildren'  # post
api_checkref = 'https://app.cexptap.com/api/v2/getChildren'  # post
api_startTask = 'https://app.cexptap.com/api/v2/startTask'  # post
api_checkTask = 'https://app.cexptap.com/api/v2/checkTask'  # post
api_claimTask = 'https://app.cexptap.com/api/v2/claimTask'  # post
api_checkCompletedTask = 'https://app.cexptap.com/api/v2/getUserTasks' # post
api_getUserCard = 'https://app.cexptap.com/api/v2/getUserCards' #post
api_buyUpgrade = 'https://app.cexptap.com/api/v2/buyUpgrade' #post
api_getSpecialOffer = 'https://app.cexptap.com/api/v2/getUserSpecialOffer' # post
api_startSpecialOffer = 'https://app.cexptap.com/api/v2/startUserSpecialOffer' #post
api_checkSpecialOffer = 'https://app.cexptap.com/api/v2/checkUserSpecialOffer' #post
api_claimSpecialOffer = 'https://app.cexptap.com/api/v2/claimUserSpecialOffer' #post

with open('bot/config/proxies/session_proxy.json', 'r') as f:
    session_proxy_map = json.load(f)

def get_proxy_for_session(session_name):
    return session_proxy_map.get(session_name)

def generate_userhash(user_id: str) -> str:
    random_data = os.urandom(16)
    hash_input = user_id.encode('utf-8') + random_data

    userhash = hashlib.sha256(hash_input).hexdigest()

    return userhash

class Tapper:
    def __init__(self, tg_client: Client):
        self.tg_client = tg_client
        self.session_name = tg_client.name
        self.first_name = ''
        self.last_name = ''
        self.user_id = ''
        self.Total_Point_Earned = 0
        self.Total_Game_Played = 0
        self.btc_balance = 0
        self.coin_balance = 0
        self.task = None
        self.card = None
        self.startedTask = []
        self.skip = ['register_on_cex_io', 'boost_telegram', 'subscribe_crypto_garden_telegram', 'join_wcoin_tap_game']
        self.card1 = None
        self.potential_card = {}
        self.multi = 1000000
        self.headers = headers.copy()
        self.app_version = '0.19.3'
        self.proxy = None

        self.user_agents_dir = "user_agents"
        self.session_ug_dict = {}


    async def init(self):
        os.makedirs(self.user_agents_dir, exist_ok=True)
        await self.load_user_agents()
        user_agent, sec_ch_ua = await self.check_user_agent()
        self.headers = headers.copy()
        self.headers['User-Agent'] = user_agent
        self.headers['Sec-Ch-Ua'] = sec_ch_ua

        self.headers['X-Request-Userhash'] = generate_userhash(self.user_id)

    async def generate_random_user_agent(self):
        user_agent, sec_ch_ua = generate_random_user_agent(device_type='android', browser_type='webview')
        return user_agent, sec_ch_ua

    async def load_user_agents(self) -> None:
        try:
            os.makedirs(self.user_agents_dir, exist_ok=True)
            filename = f"{self.session_name}.json"
            file_path = os.path.join(self.user_agents_dir, filename)

            if not os.path.exists(file_path):
                logger.info(f"{self.session_name} | User agent file not found. A new one will be created when needed.")
                return

            try:
                async with aiofiles.open(file_path, 'r') as user_agent_file:
                    content = await user_agent_file.read()
                    if not content.strip():
                        logger.warning(f"{self.session_name} | User agent file '{filename}' is empty.")
                        return

                    data = json.loads(content)
                    if data['session_name'] != self.session_name:
                        logger.warning(f"{self.session_name} | Session name mismatch in file '{filename}'.")
                        return

                    self.session_ug_dict = {self.session_name: data}
            except json.JSONDecodeError:
                logger.warning(f"{self.session_name} | Invalid JSON in user agent file: {filename}")
            except Exception as e:
                logger.error(f"{self.session_name} | Error reading user agent file {filename}: {e}")
        except Exception as e:
            logger.error(f"{self.session_name} | Error loading user agents: {e}")

    async def save_user_agent(self) -> Tuple[str, str]:
        user_agent_str, sec_ch_ua = await self.generate_random_user_agent()

        new_session_data = {
            'session_name': self.session_name,
            'user_agent': user_agent_str,
            'sec_ch_ua': sec_ch_ua
        }

        file_path = os.path.join(self.user_agents_dir, f"{self.session_name}.json")
        try:
            async with aiofiles.open(file_path, 'w') as user_agent_file:
                await user_agent_file.write(json.dumps(new_session_data, indent=4, ensure_ascii=False))
        except Exception as e:
            logger.error(f"{self.session_name} | Error saving user agent data: {e}")

        self.session_ug_dict = {self.session_name: new_session_data}

        logger.info(f"{self.session_name} | User agent saved successfully: {user_agent_str}")

        return user_agent_str, sec_ch_ua

    async def check_user_agent(self) -> Tuple[str, str]:
        if self.session_name not in self.session_ug_dict:
            return await self.save_user_agent()

        session_data = self.session_ug_dict[self.session_name]
        if 'user_agent' not in session_data or 'sec_ch_ua' not in session_data:
            return await self.save_user_agent()

        return session_data['user_agent'], session_data['sec_ch_ua']

    async def check_proxy(self, http_client: aiohttp.ClientSession) -> bool:
        try:
            response = await http_client.get(url='https://ipinfo.io/json', timeout=aiohttp.ClientTimeout(total=5))
            data = await response.json()

            ip = data.get('ip')
            city = data.get('city')
            country = data.get('country')

            logger.info(
                f"{self.session_name} | Check proxy! Country: <cyan>{country}</cyan> | City: <light-yellow>{city}</light-yellow> | Proxy IP: {ip}")

            return True

        except Exception as error:
            logger.error(f"{self.session_name} | Proxy error: {error}")
            return False

    async def get_tg_web_data(self, proxy: str | None) -> str:
        #logger.info(f"Getting data for {self.session_name}")
        if proxy:
            proxy = Proxy.from_str(proxy)
            proxy_dict = dict(
                scheme=proxy.protocol,
                hostname=proxy.host,
                port=proxy.port,
                username=proxy.login,
                password=proxy.password
            )
        else:
            proxy_dict = None

        self.tg_client.proxy = proxy_dict

        try:
            if not self.tg_client.is_connected:
                try:
                    await self.tg_client.connect()

                except (Unauthorized, UserDeactivated, AuthKeyUnregistered):
                    raise InvalidSession(self.session_name)

            while True:
                try:
                    peer = await self.tg_client.resolve_peer('cexio_tap_bot')
                    break
                except FloodWait as fl:
                    fls = fl.value

                    logger.warning(f"{self.session_name} | FloodWait {fl}")
                    wait_time = random.randint(3600, 12800)
                    logger.info(f"{self.session_name} | Sleep {wait_time}s")

                    await asyncio.sleep(wait_time)

            web_view = await self.tg_client.invoke(RequestWebView(
                peer=peer,
                bot=peer,
                platform='android',
                from_bot_menu=False,
                url="https://cexp.cex.io/",
            ))

            auth_url = web_view.url
            tg_web_data = unquote(
                string=unquote(string=auth_url.split('tgWebAppData=')[1].split('&tgWebAppVersion')[0]))

            self.user_id = tg_web_data.split('"id":')[1].split(',"first_name"')[0]
            self.first_name = tg_web_data.split('"first_name":"')[1].split('","last_name"')[0]
            self.last_name = tg_web_data.split('"last_name":"')[1].split('","username"')[0]

            if self.tg_client.is_connected:
                await self.tg_client.disconnect()

            return unquote(string=auth_url.split('tgWebAppData=')[1].split('&tgWebAppVersion')[0])

        except InvalidSession as error:
            raise error

        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error during Authorization: "
                         f"{error}")
            await asyncio.sleep(delay=3)

    async def get_user_info(self, http_client: ClientSession, authToken):
        await self.ensure_http_client()
        data = {
            "devAuthData": int(self.user_id),
            "authData": str(authToken),
            "platform": "android",
            "data": {}
        }
        try:
            response = await http_client.post(api_profile, json=data)

            if response.status == 200:
                json_response = await response.json()
                data_response = json_response['data']

                self.coin_balance = float(data_response['balance_USD'])
                self.multi = 10**data_response['precision_BTC']
                self.btc_balance = int(data_response['balance_BTC']) / self.multi
                logger.info(
                    f"{self.session_name} | Balance: <green>{self.coin_balance:,.0f}</green> | BTC balance: <ly>{self.btc_balance:.2f}</ly>")
            else:
                logger.error(f"{self.session_name} | Error while getting user data. Response {response.status}. Try again after 30s")
                await asyncio.sleep(30)
        except Exception as e:
                logger.error(f"Error while getting user data: {e} .Try again after 30s")
                await self.init_http_client()
                raise
        return response


    async def tap(self, http_client: aiohttp.ClientSession, authToken, taps):
        time_unix = int((time()) * 1000)
        data = {
            "devAuthData": int(self.user_id),
            "authData": str(authToken),
            "platform": "android",
            "data": {
                "tapsEnergy": "1000",
                "tapsToClaim": str(taps),
                "tapsTs": time_unix
            }
        }
        # print(int((time()) * 1000) - time_unix)
        response = await http_client.post(api_tap, json=data)
        if response.status == 200:
            json_response = await response.json()
            data_response = json_response['data']
            self.coin_balance = data_response['balance_USD']
            logger.info(f"{self.session_name} | Tapped <cyan>{taps}</cyan> times | Coin balance: <green>{self.coin_balance:,.0f}</green>")
        else:

            json_response = await response.json()
            if "too slow" in json_response['data']['reason']:
                logger.error(f'{self.session_name} | <lr>Tap failed - please stop the code and open the bot in telegram then tap 1-2 times and run this code again. it should be worked!</lr>')
            else:
                print(json_response)
                logger.error(f'{self.session_name} | <lr>Tap failed - response code: {response.status}</lr>')
        return response

    async def claim_crypto(self, http_client: aiohttp.ClientSession, authToken):
        data = {
            "devAuthData": int(self.user_id),
            "authData": str(authToken),
            "platform": "android",
            "data": {}
        }
        response = await http_client.post(api_claimBTC, json=data)
        if response.status == 200:
            json_response = await response.json()
            data_response = json_response['data']["BTC"]
            try:
                self.multi = 10 ** int(data_response['precision_BTC'])
                self.btc_balance = int(data_response['balance_BTC']) / self.multi
            except:
                return None
            logger.info(
                f"{self.session_name} | Claimed <ly>{int(data_response['claimedAmount']) / self.multi:,.1f}</ly> BTC")
        else:
            logger.error(f"{self.session_name} | <lr>Claim BTC failed - response code: {response.status}</lr>")
        return response

    async def getConvertData(self, http_client: aiohttp.ClientSession, authToken):
        data = {
            "devAuthData": int(self.user_id),
            "authData": str(authToken),
            "platform": "android",
            "data": {}
        }
        response = await http_client.post(api_priceData, json=data)
        if response.status == 200:
            json_response = await response.json()
            data_response = json_response['convertData']['lastPrices']
            return data_response[-1]
        else:
            logger.error(f"{self.session_name} | Can convert | Error code: {response.status}")
            return response, None

    async def convertBTC(self, http_client: aiohttp.ClientSession, authToken):
        price = await self.getConvertData(http_client, authToken)
        if price:
            data = {
                "devAuthData": int(self.user_id),
                "authData": str(authToken),
                "platform": "android",
                "data": {
                    "fromCcy": "BTC",
                    "toCcy": "USD",
                    "price": str(price),
                    "fromAmount": str(self.btc_balance)
                }
            }
            response = await http_client.post(api_convert, json=data)
            if response.status == 200:
                json_response = await response.json()
                data_response = json_response['convert']
                self.coin_balance = data_response['balance_USD']
                logger.success(
                    f"{self.session_name} | Successfully convert <ly>{self.btc_balance:,.0f}</ly> BTC to <ly>{float(self.btc_balance)*float(price):,.0f}</ly> coin(s)")
            else:
                logger.error(f"{self.session_name} | <red>Error code {response.status} While trying to convert...</red>")
            return response

    async def checkref(self, http_client: aiohttp.ClientSession, authToken):
        data = {
            "devAuthData": int(self.user_id),
            "authData": str(authToken),
            "platform": "android",
            "data": {}
        }
        response = await http_client.post(api_checkref, json=data)
        if response.status == 200:
            json_response = await response.json()
            return json_response['data']['totalRewardsToClaim']
        else:
            return 0

    async def claim_pool(self, http_client: aiohttp.ClientSession, authToken):
        data = {
            "devAuthData": int(self.user_id),
            "authData": str(authToken),
            "platform": "android",
            "data": {}
        }
        response = await http_client.post(api_claimRef, json=data)
        if response.status == 200:
            json_response = await response.json()
            logger.success(
                f"{self.session_name} | Successfully claimed <ly>{int(json_response['data']['claimed_BTC']) / self.multi:.0f}</ly>")
        else:
            logger.error(f"{self.session_name} | <red>Error code {response.status} While trying to claim from pool</red>")

    async def fetch_data(self, http_client: aiohttp.ClientSession, authToken):
        data = {
            "devAuthData": int(self.user_id),
            "authData": str(authToken),
            "platform": "android",
            "data": {}
        }
        response = await http_client.post(api_data, json=data)
        if response.status == 200:
            json_response = await response.json(content_type=None)
            # print(json_response)
            try:
                self.task = json_response['tasksConfig']
            except:
                self.task = []

            self.card = json_response['upgradeCardsConfig']
        else:
            logger.error(f"{self.session_name} | <red>Error code {response.status} While trying to get data</red>")

    async def getUserTask(self, http_client: aiohttp.ClientSession, authToken):
        data = {
            "devAuthData": int(self.user_id),
            "authData": str(authToken),
            "platform": "android",
            "data": {}
        }
        response = await http_client.post(api_checkCompletedTask, json=data)
        if response.status == 200:
            json_response = await response.json()
            completed_task = []
            for task in json_response['tasks']:
                if json_response['tasks'][task]['state'] == "Claimed":
                    completed_task.append(task)
                elif json_response['tasks'][task]['state'] == "ReadyToCheck":
                    self.startedTask.append(task)
            return completed_task
        else:
            logger.error(f"{self.session_name} | <red>Error code {response.status} While trying to get completed task</red>")
            return response, None

    async def claimTask(self, http_client: aiohttp.ClientSession, authToken, taskId):
        data = {
            "devAuthData": int(self.user_id),
            "authData": str(authToken),
            "platform": "android",
            "data": {
                "taskId": taskId
            }
        }
        response = await http_client.post(api_claimTask, json=data)
        if response.status == 200:
            json_response = await response.json()
            logger.success(
                f"{self.session_name} | <green>Successfully claimed <yellow>{json_response['data']['claimedBalance']}</yellow> from {taskId}</green>")
            return True
        else:
            logger.error(f"{self.session_name} | <red>Failed to claim {taskId}. Response: {response.status}</red>")
            return False

    async def checkTask(self, http_client: aiohttp.ClientSession, authToken, taskId):
        data = {
            "devAuthData": int(self.user_id),
            "authData": str(authToken),
            "platform": "android",
            "data": {
                "taskId": taskId
            }
        }
        response = await http_client.post(api_checkTask, json=data)
        if response.status == 200:
            json_response = await response.json()
            return json_response['data']['state']
        else:
            logger.error(f"{self.session_name} | <red>Failed to check task {taskId}. Response: {response.status}</red>")
            return None

    async def startTask(self, http_client: aiohttp.ClientSession, authToken, taskId):
        data = {
            "devAuthData": int(self.user_id),
            "authData": str(authToken),
            "platform": "android",
            "data": {
                "taskId": taskId
            }
        }
        response = await http_client.post(api_startTask, json=data)
        if response.status == 200:
            logger.info(f"{self.session_name} | Successfully started task <cyan>{taskId}</cyan>")
        else:
            if response.status == 500:
                self.skip.append(taskId)
            logger.error(f"{self.session_name} | <red>Failed to start task {taskId}. Response: {response.status}</red>")

    async def getUserCard(self, http_client: aiohttp.ClientSession, authToken):
        data = {
            "devAuthData": int(self.user_id),
            "authData": str(authToken),
            "platform": "android",
            "data": {}
        }
        response = await http_client.post(api_getUserCard, json=data)
        if response.status == 200:
            json_response = await response.json()
            return json_response['cards']
        else:
           return response, None

    async def find_potential(self):
        self.potential_card.clear()
        for category in self.card:
            for card in category['upgrades']:
                if 'upgradeId' not in card or 'levels' not in card:
                    continue

                if card['upgradeId'] in self.card1:
                    card_lvl = self.card1[card['upgradeId']]['lvl']
                    if len(card['levels']) <= card_lvl:
                        continue
                    if len(card['levels']) > 0:
                        level_data = card['levels'][card_lvl]
                        if len(level_data) < 3 or level_data[2] == 0:
                            continue
                        potential = level_data[0] / level_data[2]
                        self.potential_card[potential] = {
                            "upgradeId": card['upgradeId'],
                            "cost": level_data[0],
                            "effect": level_data[2],
                            "categoryId": category['categoryId'],
                            "nextLevel": card_lvl + 1,
                            "effectCcy": "CEXP",
                            "ccy": "USD",
                            "dependency": card.get('dependency', {}),
                            "isAvailable": card.get('isAvailable', True)
                        }
                else:
                    if len(card['levels']) > 0:
                        level_data = card['levels'][0]
                        if len(level_data) < 3 or level_data[2] == 0:
                            continue
                        potential = level_data[0] / level_data[2]
                        self.potential_card[potential] = {
                            "upgradeId": card['upgradeId'],
                            "cost": level_data[0],
                            "effect": level_data[2],
                            "categoryId": category['categoryId'],
                            "nextLevel": 1,
                            "effectCcy": "CEXP",
                            "ccy": "USD",
                            "dependency": card.get('dependency', {}),
                            "isAvailable": card.get('isAvailable', True)
                        }

    def checkDependcy(self, dependency):
        if len(dependency) == 0:
            return True
        if dependency['upgradeId'] not in self.card1:
            return False
        if self.card1[dependency['upgradeId']]['lvl'] >= dependency['level']:
            return True
        return False

    async def buyUpgrade(self, http_client: aiohttp.ClientSession, authToken, Buydata):
        data = {
            "devAuthData": int(self.user_id),
            "authData": str(authToken),
            "platform": "android",
            "data": {
                "categoryId": Buydata['categoryId'],
                "ccy": Buydata['ccy'],
                "cost": Buydata['cost'],
                "effect": Buydata['effect'],
                "effectCcy": Buydata['effectCcy'],
                "nextLevel": Buydata['nextLevel'],
                "upgradeId": Buydata['upgradeId']
            }
        }
        try:
            response = await http_client.post(api_buyUpgrade, json=data)
            if response.status == 200:
                logger.success(
                    f"{self.session_name} | Successfully upgraded card <cyan>{Buydata['upgradeId']}</cyan> to level <cyan>{Buydata['nextLevel']}</cyan>")
                return True, 200
            else:
                logger.error(
                    f"{self.session_name} | <red>Error while upgrading card {Buydata['upgradeId']} to lvl {Buydata['nextLevel']}. Response code: {response.status}</red>")
                return False, response.status
        except Exception as e:
            logger.error(f"{self.session_name} | <red>Exception while upgrading card: {e}</red>")
            return False, 0

    async def init_http_client(self):
        if hasattr(self, 'http_client') and not self.http_client.closed:
            await self.http_client.close()
        self.http_client = CloudflareScraper(headers=self.headers, connector=self.proxy_conn)

    async def ensure_http_client(self):
        if not hasattr(self, 'http_client') or self.http_client.closed:
            await self.init_http_client()

    async def process_tasks(self, authToken):
        user_task = await self.getUserTask(self.http_client, authToken)
        if user_task:
            for task in self.task:
                if task['taskId'] in self.skip or task['taskId'] in user_task or task['type'] != "social":
                    continue
                if task['taskId'] in self.startedTask:
                    logger.info(f"{self.session_name} | Checking task {task['taskId']}")
                    task_status = await self.checkTask(self.http_client, authToken, task['taskId'])
                    if task_status == "ReadyToClaim":
                        delay = random.randint(10, 20)
                        await asyncio.sleep(delay)
                        await self.claimTask(self.http_client, authToken, task['taskId'])
                    else:
                        logger.info(f"{self.session_name} | Task {task['taskId']} is not ready to claim yet")
                else:
                    await self.startTask(self.http_client, authToken, task['taskId'])
                    delay = random.randint(10, 20)
                    await asyncio.sleep(delay)
        else:
            logger.info(f"{self.session_name} | Unable to get user tasks")

    async def process_upgrades(self, authToken):
        try:
            await self.get_user_info(self.http_client, authToken)
            await self.fetch_data(self.http_client, authToken)
            self.card1 = await self.getUserCard(self.http_client, authToken)

            if self.card1:
                await self.find_potential()

                sorted_potential_card = sorted(self.potential_card.values(), key=lambda x: x['cost'])

                for card_data in sorted_potential_card:

                    required_keys = ['upgradeId', 'cost', 'nextLevel', 'dependency', 'isAvailable']
                    if not all(key in card_data for key in required_keys):
                        logger.debug(f"{self.session_name} | Card data is missing required keys: {card_data}")
                        continue

                    if not card_data['isAvailable']:
                        logger.debug(
                            f"{self.session_name} | Card {card_data['upgradeId']} is not available for upgrade")
                        continue

                    if not self.checkDependcy(card_data['dependency']):
                        continue

                    card_cost = float(card_data['cost'])

                    if card_cost > round(float(self.coin_balance)):
                        continue

                    retry_count = 0
                    max_retries = 3
                    while retry_count < max_retries:
                        await asyncio.sleep(randint(5, 10))

                        current_card = next(
                            (c for c in self.card1.values() if c.get('upgradeId') == card_data['upgradeId']),
                            None)
                        if current_card and current_card.get('lvl', 0) >= card_data['nextLevel']:
                            logger.debug(
                                f"{self.session_name} | Card {card_data['upgradeId']} is already at or above level {card_data['nextLevel']}")
                            break

                        check, error_code = await self.buyUpgrade(self.http_client, authToken, card_data)
                        if check:
                            self.coin_balance = float(self.coin_balance) - card_cost

                            await self.get_user_info(self.http_client, authToken)
                            await self.fetch_data(self.http_client, authToken)
                            self.card1 = await self.getUserCard(self.http_client, authToken)
                            await self.find_potential()

                            sorted_potential_card = sorted(self.potential_card.values(), key=lambda x: x['cost'])
                            break
                        else:
                            retry_count += 1
                            if error_code == 500:
                                logger.debug(
                                    f"{self.session_name} | Server error (500) on attempt {retry_count}. Retrying...")
                                await asyncio.sleep(10 * retry_count)
                            else:
                                logger.debug(
                                    f"{self.session_name} | Upgrade failed with error code {error_code}. Moving to next card.")
                                break

                    if retry_count == max_retries:
                        logger.debug(
                            f"{self.session_name} | Failed to upgrade card after {max_retries} attempts. Moving to next card.")

                    await asyncio.sleep(10)
            else:
                logger.info(f"{self.session_name} | Unable to get user cards")
        except Exception as e:
            logger.error(f"{self.session_name} | Error in process_upgrades: {e}")
            logger.error(f"{self.session_name} | Traceback: {traceback.format_exc()}")

    async def run(self, proxy: str | None) -> None:
        self.proxy = proxy
        if settings.USE_RANDOM_DELAY_IN_RUN:
            random_delay = random.randint(settings.RANDOM_DELAY_IN_RUN[0], settings.RANDOM_DELAY_IN_RUN[1])
            logger.info(
                f"{self.session_name} | The Bot will go live in <y>{random_delay}s</y>")
            await asyncio.sleep(random_delay)

        await self.init()

        self.proxy_conn = ProxyConnector().from_url(proxy) if proxy else None
        http_client = CloudflareScraper(headers=self.headers, connector=self.proxy_conn)
        connection_manager.add(http_client)

        if settings.USE_PROXY:
            if not self.proxy:
                logger.error(f"{self.session_name} | Proxy is not set. Aborting operation.")
                return
            if not await self.check_proxy(http_client):
                logger.error(f"{self.session_name} | Proxy check failed. Aborting operation.")
                return

        authToken = ""

        self.access_token_created_time = 0
        self.token_live_time = randint(3500, 3600)

        while True:
                try:
                    if http_client.closed:
                        if self.proxy_conn:
                            if not self.proxy_conn.closed:
                                await self.proxy_conn.close()

                        self.proxy_conn = ProxyConnector().from_url(proxy) if proxy else None
                        http_client = CloudflareScraper(headers=self.headers, connector=self.proxy_conn)
                        connection_manager.add(http_client)

                    self.headers['X-Request-Userhash'] = generate_userhash(self.user_id)

                    if not self.tg_client.is_connected:
                        try:
                            await self.tg_client.connect()
                        except Exception as e:
                            logger.error(f"{self.session_name} | Failed to reconnect: {e}")
                            await asyncio.sleep(60)
                            continue

                    if time() - self.access_token_created_time >= self.token_live_time or authToken == "":
                        tg_web_data = await self.get_tg_web_data(proxy=proxy)
                        authToken = tg_web_data
                        self.access_token_created_time = time()
                        self.token_live_time = randint(3500, 3600)
                        await asyncio.sleep(delay=randint(10, 15))

                    await self.ensure_http_client()

                    await self.get_user_info(self.http_client, authToken)

                    await self.fetch_data(self.http_client, authToken)

                    self.card1 = await self.getUserCard(self.http_client, authToken)

                    if self.card is None or self.task is None:
                        await self.fetch_data(self.http_client, authToken)

                    await self.claim_crypto(self.http_client, authToken)

                    if settings.AUTO_CONVERT and self.btc_balance >= settings.MINIMUM_TO_CONVERT:
                        await self.convertBTC(self.http_client, authToken)

                    if settings.AUTO_TASK and self.task:
                        await self.process_tasks(authToken)

                    if settings.AUTO_BUY_UPGRADE:
                        await self.process_upgrades(authToken)


                except aiohttp.ClientConnectorError as error:
                    delay = random.randint(1800, 3600)
                    logger.error(f"{self.session_name} | Connection error: {error}. Retrying in {delay} seconds.")
                    logger.debug(f"Full error details: {traceback.format_exc()}")
                    await asyncio.sleep(delay)


                except aiohttp.ServerDisconnectedError as error:
                    delay = random.randint(900, 1800)
                    logger.error(f"{self.session_name} | Server disconnected: {error}. Retrying in {delay} seconds.")
                    logger.debug(f"Full error details: {traceback.format_exc()}")
                    await asyncio.sleep(delay)


                except aiohttp.ClientResponseError as error:
                    delay = random.randint(3600, 7200)
                    logger.error(
                        f"{self.session_name} | HTTP response error: {error}. Status: {error.status}. Retrying in {delay} seconds.")
                    logger.debug(f"Full error details: {traceback.format_exc()}")
                    await asyncio.sleep(delay)


                except aiohttp.ClientError as error:
                    delay = random.randint(3600, 7200)
                    logger.error(f"{self.session_name} | HTTP client error: {error}. Retrying in {delay} seconds.")
                    logger.debug(f"Full error details: {traceback.format_exc()}")
                    await asyncio.sleep(delay)


                except asyncio.TimeoutError:
                    delay = random.randint(7200, 14400)
                    logger.error(f"{self.session_name} | Request timed out. Retrying in {delay} seconds.")
                    logger.debug(f"Full error details: {traceback.format_exc()}")
                    await asyncio.sleep(delay)


                except InvalidSession as error:
                    logger.critical(f"{self.session_name} | Invalid Session: {error}. Manual intervention required.")
                    logger.debug(f"Full error details: {traceback.format_exc()}")
                    raise error


                except json.JSONDecodeError as error:
                    delay = random.randint(1800, 3600)
                    logger.error(f"{self.session_name} | JSON decode error: {error}. Retrying in {delay} seconds.")
                    logger.debug(f"Full error details: {traceback.format_exc()}")
                    await asyncio.sleep(delay)

                except KeyError as error:
                    delay = random.randint(1800, 3600)
                    logger.error(
                        f"{self.session_name} | Key error: {error}. Possible API response change. Retrying in {delay} seconds.")
                    logger.debug(f"Full error details: {traceback.format_exc()}")
                    await asyncio.sleep(delay)

                except Exception as error:
                    logger.error(f"{self.session_name} | Unknown error: {error}")
                    logger.error(f"{self.session_name} | Error traceback: {traceback.format_exc()}")
                    error_exception = uniform(3100, 12800)
                    hours = int(error_exception // 3600)
                    minutes = (int(error_exception % 3600)) // 60
                    logger.warning(
                        f"{self.session_name} | Sleep with exception error <yellow>{hours} hours</yellow> and <yellow>{minutes} minutes</yellow>")
                    await asyncio.sleep(error_exception)

                finally:
                    await http_client.close()
                    if self.proxy_conn:
                        if not self.proxy_conn.closed:
                            await self.proxy_conn.close()
                    connection_manager.remove(http_client)

                    sleep_hours = uniform(8, 16)
                    sleep_seconds = int(sleep_hours * 3600)
                    wake_time = datetime.datetime.now() + datetime.timedelta(seconds=sleep_seconds)
                    logger.info(f"{self.session_name} | Going to sleep for {sleep_hours:.2f} hours. Will wake up at <light-red>{wake_time.strftime('%H:%M:%S')}</light-red>")
                    await asyncio.sleep(sleep_seconds)
                    logger.info(f"{self.session_name} | ⏰ Woke up and resuming operations")


async def run_tapper(tg_client: Client):
    session_name = tg_client.name
    proxy = None
    if settings.USE_PROXY:
        proxy = get_proxy_for_session(session_name)
        if not proxy:
            logger.error(f"{session_name} | Proxy is not set for this session. Aborting operation.")
            return

    try:
        await Tapper(tg_client=tg_client).run(proxy=proxy)
    except InvalidSession:
        logger.error(f"{tg_client.name} | Invalid Session")
