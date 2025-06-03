import pandas as pd
from telethon.sync import TelegramClient
from telethon.tl.types import InputMessagesFilterPhotos
from tqdm.auto import tqdm

from .clients import VKGroupClient
from .config import config


def _vk_posts2photos_metadata(posts: list[dict]) -> pd.DataFrame:
    result = pd.DataFrame(posts)

    result = result[['id', 'attachments']].copy()
    result = result.rename(columns={'id': 'post_id'})

    result = result.explode('attachments')

    result = result[result['attachments'].str.get('type').eq('photo')]
    result['attachments'] = result['attachments'].str.get('photo')

    result['url'] = result['attachments'].str.get('orig_photo').str.get('url')
    result['id'] = result['attachments'].str.get('id').astype('str')
    result['date'] = result['attachments'].str.get('date')

    result = result.drop(columns='attachments')

    return result


def _vk_photos2metadata(photos: list[dict]) -> pd.DataFrame:
    result = pd.DataFrame(photos)

    result['url'] = result['orig_photo'].str.get('url')
    result = result[['date', 'post_id', 'id', 'url']].copy()

    return result


def _get_vk_group_photos_metadata(client: VKGroupClient) -> pd.DataFrame:
    photos_metadata = _vk_photos2metadata(client.get_wall_photos())

    posts_photos_metadata = _vk_posts2photos_metadata(client.get_posts())
    posts_photos_metadata = posts_photos_metadata[posts_photos_metadata['url'].notna()]
    posts_photos_metadata = posts_photos_metadata.rename(columns={'post_id': '_post_id'})

    result = posts_photos_metadata.merge(photos_metadata, how='outer', validate='many_to_one')

    result['post_id'] = result['_post_id'].combine_first(result['post_id'])
    result = result[result['post_id'].notna()]
    result = result.drop(columns='_post_id')

    assert result.notna().all().all()
    assert not result.duplicated(['post_id', 'id']).any()

    result = result.sort_values('id')
    result['id'] = result.groupby('post_id').cumcount() + 1

    return result


def get_vk_photos_metadata():
    result = []
    for group in config['vk']['groups']:
        client = VKGroupClient(
            access_token=config['vk']['token'],
            name=group,
            api_version=config['vk']['api_version']
        )
        metadata = _get_vk_group_photos_metadata(client)
        metadata['group'] = group
        result.append(metadata)
    result = pd.concat(result)
    return result


async def get_telegram_photos_metadata() -> pd.DataFrame:
    result = []
    api_id = config['telegram']['api_id']
    api_hash = config['telegram']['api_hash']
    async with TelegramClient('memdex_metadata', api_id, api_hash) as client:
        for channel in config['telegram']['channels']:
            total = (await client.get_messages(channel, 0, filter=InputMessagesFilterPhotos)).total
            messages = client.iter_messages(channel, filter=InputMessagesFilterPhotos)
            async for msg in tqdm(messages, total=total, unit='message'):
                result.append(dict(
                    group=channel,
                    date=int(msg.date.timestamp()),
                    post_id=msg.id,
                    id=1
                ))
    result = pd.DataFrame(result)
    return result
