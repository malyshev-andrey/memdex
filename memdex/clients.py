from dataclasses import dataclass
from time import monotonic, sleep
import itertools
from functools import cache
from urllib.parse import urlencode

import requests
from tqdm.auto import tqdm


@dataclass
class VKGroupClient:
    access_token: str
    name: str
    api_version: str = '5.199'
    rate_limit: float = 3  # max requests per second
    _last_call_time: float = float('-inf')

    @cache
    def _make_request(self, url: str) -> requests.Response:
        delta = monotonic() - self._last_call_time
        if delta < 1 / self.rate_limit:
            sleep(1 / self.rate_limit - delta)
        self._last_call_time = monotonic()

        response = requests.get(url)
        response.raise_for_status()
        return response

    def _get_response(self, url: str, params: dict) -> dict:
        params = dict(
            access_token=self.access_token,
            v=self.api_version,
            **params
        )
        url = url.removesuffix('/')
        response = self._make_request(f'{url}?{urlencode(params)}')
        response = response.json()
        response = response['response']

        return response

    def _get_owner_id(self) -> str:
        result = self._get_response(
            'https://api.vk.com/method/groups.getById',
            params={'group_id': self.name}
        )

        result, = result['groups']  # one group expected
        result = str(-result['id'])

        return result

    def __post_init__(self):
        self.owner_id = self._get_owner_id()

    def __hash__(self):
        return hash((self.name, self.api_version))

    def _get_items(self, url: str, params: dict, *, unit: str|None = None) -> list[dict]:
        results = []
        total = self._get_response(url, params)['count']
        with tqdm(total=total, unit=unit, disable=unit is None) as progress_bar:
            for offset in itertools.count(step=params['count']):
                chunk = self._get_response(url, params | {'offset': offset})
                chunk = chunk['items']
                assert len(chunk) > 0

                results.extend(chunk)
                progress_bar.update(len(chunk))

                if len(results) >= total:
                    assert len(results) == total
                    break

        for item in results:
            for key in item:
                if key.endswith('_id') or key == 'id':
                    item[key] = str(item[key])

        return results

    def get_wall_photos(self, **kwargs) -> list[dict]:
        params = dict(
            owner_id=self.owner_id,
            album_id='wall',
            rev=0,
            count=1000
        )
        params.update(**kwargs)

        result = self._get_items(
            'https://api.vk.com/method/photos.get',
            params=params,
            unit='photo'
        )

        return result

    def get_posts(self, **kwargs) -> list[dict]:
        params = dict(
            domain=self.name,
            count=100
        )
        params.update(**kwargs)

        result = self._get_items(
            'https://api.vk.com/method/wall.get',
            params=params,
            unit='post'
        )

        return result
