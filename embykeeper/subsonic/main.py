from __future__ import annotations

import asyncio
from datetime import datetime, time
from pathlib import Path
import random
from typing import TYPE_CHECKING, Iterable, Tuple, Union
import json
from urllib.parse import urlparse

import httpx
from loguru import logger

from .api import Subsonic

from ..utils import next_random_datetime, show_exception

if TYPE_CHECKING:
    from loguru import Logger

logger = logger.bind(scheme="subsonic")


async def listen(client: Subsonic, loggeruser: Logger, time: Union[float, Iterable[float]]):
    """模拟连续播放音频直到达到指定总时长."""

    if isinstance(time, Iterable):
        total_time = random.uniform(*time)
    else:
        total_time = float(time)

    played_time = 0
    max_retries = 3
    current_retries = 0

    while played_time < total_time:
        try:
            songs = await client.get_random_songs()
            if not songs:
                loggeruser.warning("未能获取到任何歌曲.")
                return False
            song = random.choice(songs)
            song_id = song.get("id", None)
            if not song_id:
                loggeruser.warning("获取到歌曲信息不完整, 正在重试.")
                continue

            song_title = song.get("title", "未知歌曲")
            song_duration = float(song.get("duration", 60))
            remaining_time = total_time - played_time

            play_duration = min(remaining_time, song_duration) if song_duration > 0 else remaining_time

            loggeruser.info(f'开始播放 "{song_title}", 剩余时间 {remaining_time:.0f} 秒.')
            while current_retries < max_retries:
                try:
                    await client.scrobble(song_id, submission=False)
                    await asyncio.wait_for(client.stream_noreturn(song_id), timeout=play_duration)
                    played_time += play_duration
                    await client.scrobble(song_id, submission=True)
                    loggeruser.info(f'完成播放 "{song_title}", 已播放 {played_time:.0f} 秒.')
                    current_retries = 0
                    break
                except asyncio.TimeoutError:
                    # 正常超时，说明歌曲播放完成
                    played_time += play_duration
                    await client.scrobble(song_id, submission=True)
                    loggeruser.info(f'完成播放 "{song_title}", 已播放 {played_time:.0f} 秒.')
                    break
                except Exception as e:
                    current_retries += 1
                    if current_retries >= max_retries:
                        loggeruser.error(f"播放出错且达到最大重试次数, 停止播放.")
                        show_exception(e, regular=False)
                        return False
                    loggeruser.warning(f"播放出错 (重试 {current_retries}/{max_retries}), 正在重试.")
                    show_exception(e, regular=False)
                    await asyncio.sleep(1)
                    continue
        except httpx.HTTPError as e:
            current_retries += 1
            if current_retries >= max_retries:
                loggeruser.error(f"播放出错且达到最大重试次数, 停止播放.")
                show_exception(e, regular=False)
                return False
            loggeruser.warning(f"访问出错 (重试 {current_retries}/{max_retries}), 正在重试: {e}.")
            show_exception(e, regular=True)
            await asyncio.sleep(1)
            continue
    return True


async def login(config):
    """登录账号."""
    for a in config.get("subsonic", ()):
        server_url = a["url"]
        domain = urlparse(server_url).netloc

        proxy = None
        client = Subsonic(
            server=server_url,
            username=a["username"],
            password=a["password"],
            proxy=proxy or config.get("proxy", None),
            ua=a.get("ua", None),
            client=a.get("client", None),
            version=a.get("version", None),
        )
        info = await client.ping()
        if info.is_ok:
            loggeruser = logger.bind(server=domain, username=a["username"])
            loggeruser.info(
                f'成功连接至服务器 "{domain}" ({(info.type or "unknown").capitalize()} {info.version or "X.X"}).'
            )
            yield (
                client,
                loggeruser,
                a.get("time", [120, 240]),
            )
        else:
            logger.bind(log=True).error(f'服务器 "{domain}" 登陆错误, 请重新检查配置: {info.error_message}')
            continue


async def listener(config: dict, instant: bool = False):
    """入口函数 - 收听一个音频."""

    async def wrapper(
        sem: asyncio.Semaphore,
        client: Subsonic,
        loggeruser: Logger,
        time: float,
    ):
        async with sem:
            try:
                if not instant:
                    wait = random.uniform(180, 360)
                    loggeruser.info(f"播放音频前随机等待 {wait:.0f} 秒.")
                    await asyncio.sleep(wait)
                if isinstance(time, Iterable):
                    tm = max(time) * 4
                else:
                    tm = time * 4
                return await asyncio.wait_for(listen(client, loggeruser, time), max(tm, 600))
            except asyncio.TimeoutError:
                loggeruser.warning(f"一定时间内未完成播放, 保活失败.")
                return False

    logger.info("开始执行 Subsonic 保活.")
    tasks = []
    concurrent = int(config.get("listen_concurrent", 3))
    if not concurrent:
        concurrent = 100000
    sem = asyncio.Semaphore(concurrent)
    async for client, loggeruser, time in login(config):
        tasks.append(wrapper(sem, client, loggeruser, time))
    if not tasks:
        logger.info("没有指定相关的 Subsonic 服务器, 跳过保活.")
    results = await asyncio.gather(*tasks)
    fails = len(tasks) - sum(results)
    if fails:
        logger.error(f"保活失败 ({fails}/{len(tasks)}).")


async def listener_schedule(
    config: dict,
    start_time=time(11, 0),
    end_time=time(23, 0),
    days: Union[int, Tuple[int, int]] = 7,
    instant: bool = False,
):
    """计划任务 - 收听一个音频."""

    timestamp_file = Path(config["basedir"]) / "listener_schedule_next_timestamp"
    current_config = {
        "start_time": start_time.strftime("%H:%M"),
        "end_time": end_time.strftime("%H:%M"),
        "days": days if isinstance(days, int) else list(days),
    }

    while True:
        next_dt = None
        config_changed = False

        if timestamp_file.exists():
            try:
                stored_data = json.loads(timestamp_file.read_text())
                if not isinstance(stored_data, dict):
                    raise ValueError("invalid cache")
                stored_timestamp = stored_data["timestamp"]
                stored_config = stored_data["config"]

                if stored_config != current_config:
                    logger.info("计划任务配置已更改，将重新计算下次执行时间.")
                    config_changed = True
                else:
                    next_dt = datetime.fromtimestamp(stored_timestamp)
                    if next_dt > datetime.now():
                        logger.bind(log=True).info(f"从缓存中读取到下次保活时间: {next_dt.strftime('%m-%d %H:%M %p')}.")
            except (ValueError, OSError, json.JSONDecodeError) as e:
                logger.debug(f"读取存储的时间戳失败: {e}")
                config_changed = True

        if not next_dt or next_dt <= datetime.now() or config_changed:
            if isinstance(days, int):
                rand_days = days
            else:
                rand_days = random.randint(*days)
            next_dt = next_random_datetime(start_time, end_time, interval_days=rand_days)
            logger.bind(log=True).info(f"下一次保活将在 {next_dt.strftime('%m-%d %H:%M %p')} 进行.")

            try:
                save_data = {"timestamp": next_dt.timestamp(), "config": current_config}
                timestamp_file.write_text(json.dumps(save_data))
            except OSError as e:
                logger.debug(f"存储时间戳失败: {e}")

        await asyncio.sleep((next_dt - datetime.now()).total_seconds())
        try:
            timestamp_file.unlink(missing_ok=True)
        except OSError as e:
            logger.debug(f"删除时间戳文件失败: {e}")
        await listener(config, instant=instant)
