from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import requests

from config import LIVE_ADAPTER_CONFIG


@dataclass
class LiveAdapterStatus:
    enabled: bool
    dry_run: bool
    testnet: bool
    api_key_present: bool
    api_secret_present: bool
    recv_window: int
    base_url: str


class BybitV5LiveAdapter:
    def __init__(self):
        self.enabled = LIVE_ADAPTER_CONFIG['enabled']
        self.dry_run = LIVE_ADAPTER_CONFIG['dry_run']
        self.testnet = LIVE_ADAPTER_CONFIG['testnet']
        self.api_key = os.getenv('BYBIT_API_KEY', '')
        self.api_secret = os.getenv('BYBIT_API_SECRET', '')
        self.recv_window = int(LIVE_ADAPTER_CONFIG['recv_window'])
        self.base_url = 'https://api-testnet.bybit.com' if self.testnet else 'https://api.bybit.com'
        self.session = requests.Session()

    def status(self) -> dict[str, Any]:
        return LiveAdapterStatus(
            enabled=self.enabled,
            dry_run=self.dry_run,
            testnet=self.testnet,
            api_key_present=bool(self.api_key),
            api_secret_present=bool(self.api_secret),
            recv_window=self.recv_window,
            base_url=self.base_url,
        ).__dict__

    def public_ticker(self, symbol: str, category: str = 'linear') -> dict[str, Any]:
        payload = self._public_request('GET', '/v5/market/tickers', {'category': category, 'symbol': symbol.upper()})
        items = payload.get('result', {}).get('list', [])
        if not items:
            raise RuntimeError(f'No ticker returned for {symbol}.')
        return items[0]

    def public_instrument_info(self, symbol: str, category: str = 'linear') -> dict[str, Any]:
        payload = self._public_request('GET', '/v5/market/instruments-info', {'category': category, 'symbol': symbol.upper()})
        items = payload.get('result', {}).get('list', [])
        if not items:
            raise RuntimeError(f'No instrument info returned for {symbol}.')
        return items[0]

    def get_wallet_balance(self, coin: str = 'USDT') -> dict[str, Any]:
        return self._private_request('GET', '/v5/account/wallet-balance', {'accountType': 'UNIFIED', 'coin': coin})

    def get_positions(self, category: str = 'linear', symbol: str | None = None, settle_coin: str | None = 'USDT') -> dict[str, Any]:
        params: dict[str, Any] = {'category': category}
        if symbol:
            params['symbol'] = symbol.upper()
        elif settle_coin:
            params['settleCoin'] = settle_coin
        return self._private_request('GET', '/v5/position/list', params)

    def place_order(
        self,
        *,
        symbol: str,
        side: str,
        qty: str,
        price: str | None = None,
        order_type: str = 'Limit',
        time_in_force: str | None = 'PostOnly',
        reduce_only: bool = False,
        take_profit: str | None = None,
        stop_loss: str | None = None,
        order_link_id: str | None = None,
        position_idx: int = 0,
        tpsl_mode: str | None = None,
        tp_order_type: str | None = None,
        sl_order_type: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            'category': 'linear',
            'symbol': symbol.upper(),
            'side': side,
            'orderType': order_type,
            'qty': qty,
            'reduceOnly': reduce_only,
            'positionIdx': position_idx,
            'orderLinkId': order_link_id or f'lab-{uuid.uuid4().hex[:18]}',
        }
        if time_in_force:
            payload['timeInForce'] = time_in_force
        if price is not None:
            payload['price'] = price
        if take_profit is not None:
            payload['takeProfit'] = take_profit
        if stop_loss is not None:
            payload['stopLoss'] = stop_loss
        if tpsl_mode is not None:
            payload['tpslMode'] = tpsl_mode
        if tp_order_type is not None:
            payload['tpOrderType'] = tp_order_type
        if sl_order_type is not None:
            payload['slOrderType'] = sl_order_type
        return self._private_request('POST', '/v5/order/create', payload)

    def cancel_all_orders(self, symbol: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {'category': 'linear', 'settleCoin': 'USDT'}
        if symbol:
            payload['symbol'] = symbol.upper()
        return self._private_request('POST', '/v5/order/cancel-all', payload)

    def _public_request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        if method.upper() != 'GET':
            raise ValueError('Only GET is supported for public requests in this adapter.')
        response = self.session.get(f'{self.base_url}{path}', params=payload, timeout=20)
        response.raise_for_status()
        payload_json = response.json()
        if payload_json.get('retCode') != 0:
            raise RuntimeError(payload_json.get('retMsg', 'Bybit public request failed'))
        return payload_json

    def _private_request(self, method: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled:
            return {
                'mode': 'disabled',
                'message': 'Live adapter is disabled. Set BYBIT_LIVE_ENABLED=true to allow signed requests.',
                'path': path,
                'payload': payload,
                'base_url': self.base_url,
            }
        if self.dry_run:
            return {'mode': 'dry-run', 'path': path, 'payload': payload, 'base_url': self.base_url}
        if not (self.api_key and self.api_secret):
            raise RuntimeError('Missing BYBIT_API_KEY / BYBIT_API_SECRET for live adapter.')

        ts = str(int(time.time() * 1000))
        headers = {
            'X-BAPI-API-KEY': self.api_key,
            'X-BAPI-TIMESTAMP': ts,
            'X-BAPI-RECV-WINDOW': str(self.recv_window),
        }

        if method.upper() == 'GET':
            query_string = urlencode({k: v for k, v in payload.items() if v is not None})
            sign_payload = f'{ts}{self.api_key}{self.recv_window}{query_string}'
            headers['X-BAPI-SIGN'] = hmac.new(self.api_secret.encode(), sign_payload.encode(), hashlib.sha256).hexdigest()
            response = self.session.get(f'{self.base_url}{path}', params=payload, headers=headers, timeout=30)
        elif method.upper() == 'POST':
            body = json.dumps(payload, separators=(',', ':'))
            sign_payload = f'{ts}{self.api_key}{self.recv_window}{body}'
            headers['X-BAPI-SIGN'] = hmac.new(self.api_secret.encode(), sign_payload.encode(), hashlib.sha256).hexdigest()
            headers['Content-Type'] = 'application/json'
            response = self.session.post(f'{self.base_url}{path}', data=body, headers=headers, timeout=30)
        else:
            raise ValueError(f'Unsupported method: {method}')
        response.raise_for_status()
        payload_json = response.json()
        if payload_json.get('retCode') != 0:
            raise RuntimeError(payload_json.get('retMsg', 'Bybit live adapter request failed'))
        return payload_json


live_adapter = BybitV5LiveAdapter()
