from __future__ import annotations

import asyncio
from datetime import datetime, time
from functools import lru_cache
import inspect
import pkgutil
import random
import re
from typing import List, Type
from importlib import import_module
from pathlib import Path
import json

from loguru import logger

from ..utils import next_random_datetime
from . import __name__ as __product__
from .link import Link
from .tele import ClientsSession

from .bots._base import BaseBotCheckin, CheckinResult

logger = logger.bind(scheme="telegram")


def get_spec(type: str):
    """服务模块路径解析."""
    if type == "checkiner":
        sub = "bots"
        suffix = "checkin"
    elif type == "monitor":
        sub = "monitor"
        suffix = "monitor"
    elif type == "messager":
        sub = "messager"
        suffix = "messager"
    else:
        raise ValueError(f"{type} is not a valid service.")
    return sub, suffix


@lru_cache
def get_names(type: str, allow_ignore=False) -> List[str]:
    """列出服务中所有可用站点."""
    sub, _ = get_spec(type)
    results = []
    typemodule = import_module(f"{__product__}.{sub}")
    for _, mn, _ in pkgutil.iter_modules(typemodule.__path__):
        module = import_module(f"{__product__}.{sub}.{mn}")
        if not allow_ignore:
            if not getattr(module, "__ignore__", False):
                results.append(mn)
        else:
            if (not mn.startswith("_")) and (not mn.startswith("test")):
                results.append(mn)
    return results


def get_cls(type: str, names: List[str] = None) -> List[Type]:
    """获得服务特定站点的所有类."""
    sub, suffix = get_spec(type)
    if names == None:
        names = get_names(type)

    exclude_names = set(name[1:] for name in names if name.startswith("-"))
    include_names = set(name[1:] for name in names if name.startswith("+"))
    names = set(name for name in names if not name.startswith("-") and not name.startswith("+"))

    if not names and (exclude_names or include_names):
        names = set(get_names(type))

    if "all" in names:
        names = set(get_names(type, allow_ignore=True))

    if type == "checkiner":
        if "sgk" in names:
            sgk_names = set(n for n in get_names(type, allow_ignore=True) if n.endswith("sgk"))
            names.update(sgk_names)
            names.remove("sgk")

        if "sgk" in exclude_names:
            sgk_names = set(n for n in names if n.endswith("sgk"))
            names -= sgk_names
            exclude_names.remove("sgk")

        if "sgk" in include_names:
            sgk_names = set(n for n in get_names(type, allow_ignore=True) if n.endswith("sgk"))
            include_names.update(sgk_names)
            include_names.remove("sgk")

    # 应用排除项
    names = names - exclude_names
    # 添加附加项
    names = list(names | include_names)

    results = []
    for name in names:
        match = re.match(r"templ_(\w+)<(\w+)>", name)
        if match:
            try:
                module = import_module(f"{__product__}.{sub}._templ_{match.group(1).lower()}")
                func = getattr(module, "use", None)
                if not func:
                    logger.warning(f'您配置的 "{type}" 不支持模板 "{match.group(1)}".')
                    continue
                results.append(func(bot_username=match.group(2), name=f"@{match.group(2)}"))
            except ImportError:
                all_names = get_names(type)
                logger.warning(f'您配置的 "{type}" 不支持站点 "{name}", 请从以下站点中选择:')
                logger.warning(", ".join(all_names))
        else:
            try:
                module = import_module(f"{__product__}.{sub}.{name.lower()}")
                for cn, cls in inspect.getmembers(module, inspect.isclass):
                    if (name.replace("_", "").replace("_old", "") + suffix).lower() == cn.lower():
                        results.append(cls)
            except ImportError:
                all_names = get_names(type)
                logger.warning(f'您配置的 "{type}" 不支持站点 "{name}", 请从以下站点中选择:')
                logger.warning(", ".join(all_names))
    return results


def extract(clss: List[Type]) -> List[Type]:
    """对于嵌套类, 展开所有子类."""
    extracted = []
    for cls in clss:
        ncs = [c for c in cls.__dict__.values() if inspect.isclass(c)]
        if ncs:
            extracted.extend(ncs)
        else:
            extracted.append(cls)
    return extracted


async def _checkin_task(checkiner: BaseBotCheckin, sem, wait=0):
    """签到器壳, 用于随机等待开始."""
    if wait > 0:
        checkiner.log.debug(f"随机启动等待: 将等待 {wait:.2f} 分钟以启动.")
    await asyncio.sleep(wait * 60)
    async with sem:
        result = await checkiner._start()
        await asyncio.sleep(random.uniform(5, 10))
        return result


async def _gather_task(tasks, username):
    return username, await asyncio.gather(*tasks)


async def checkiner(config: dict, instant=False):
    """签到器入口函数."""
    logger.debug("正在启动每日签到模块, 请等待登录.")
    async with ClientsSession.from_config(config, checkin=(True, True)) as clients:
        coros = []
        async for tg in clients:
            log = logger.bind(scheme="telechecker", username=tg.me.name)
            logger.info("已连接到 Telegram, 签到器正在初始化.")
            service_config = config.get("telegram", [])[tg._config_index].get("service", {})
            if not service_config:
                service_config: dict = config.get("service", {})
            names = service_config.get("checkiner", None)
            clses = extract(get_cls("checkiner", names=names))
            if not clses:
                log.warning("没有任何有效签到站点, 签到将跳过.")
                continue
            if not await Link(tg).auth("checkiner", log_func=log.error):
                continue
            sem = asyncio.Semaphore(int(config.get("concurrent", 1)))
            checkiners: List[BaseBotCheckin] = [
                cls(
                    tg,
                    retries=config.get("retries", 4),
                    timeout=config.get("timeout", 240),
                    nofail=config.get("nofail", True),
                    basedir=config.get("basedir", None),
                    proxy=config.get("proxy", None),
                    config=config.get("checkiner", {}).get(cls.__module__.rsplit(".", 1)[-1], {}),
                    instant=instant,
                )
                for cls in clses
            ]
            tasks = []
            names = []
            for c in checkiners:
                names.append(c.name)
                wait = 0 if instant else random.uniform(0, int(config.get("random", 60)))
                task = asyncio.create_task(_checkin_task(c, sem, wait))
                tasks.append(task)
            coros.append(asyncio.ensure_future(_gather_task(tasks, username=tg.me.name)))
            if names:
                log.debug(f'已启用签到器: {", ".join(names)}')
        while coros:
            done, coros = await asyncio.wait(coros, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                try:
                    username, results = await t
                except asyncio.CancelledError:
                    continue
                log = logger.bind(scheme="telechecker", username=username)
                failed = []
                ignored = []
                successful = []
                checked = []
                for n, s in results:
                    if s == CheckinResult.IGNORE:
                        ignored.append(n)
                    elif s == CheckinResult.SUCCESS:
                        successful.append(n)
                    elif s == CheckinResult.CHECKED:
                        checked.append(n)
                    else:
                        failed.append(n)
                spec = f"共{len(successful) + len(checked) + len(failed) + len(ignored)}个"
                if successful:
                    spec += f", {len(successful)}成功"
                if checked:
                    spec += f", {len(checked)}已签到而跳过"
                if failed:
                    spec += f", {len(failed)}失败"
                if ignored:
                    spec += f", {len(ignored)}跳过"
                if failed:
                    if successful:
                        msg = "签到部分失败"
                    else:
                        msg = "签到失败"
                    log.bind(log=True).error(f"{msg} ({spec}): {', '.join([f for f in failed])}")
                else:
                    log.bind(log=True).info(f"签到成功 ({spec}).")


async def checkiner_schedule(
    config: dict,
    start_time: time = None,
    end_time: time = None,
    days: int = 1,
    instant: bool = False,
):
    """签到器计划任务."""

    timestamp_file = Path(config["basedir"]) / "checkiner_schedule_next_timestamp"
    current_config = {
        "start_time": start_time.strftime("%H:%M") if start_time else None,
        "end_time": end_time.strftime("%H:%M") if end_time else None,
        "days": days,
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
                    logger.bind(scheme="telechecker").info("计划任务配置已更改，将重新计算下次执行时间.")
                    config_changed = True
                else:
                    next_dt = datetime.fromtimestamp(stored_timestamp)
                    if next_dt > datetime.now():
                        logger.bind(scheme="telechecker").bind(log=True).info(
                            f"从缓存中读取到下次签到时间: {next_dt.strftime('%m-%d %H:%M %p')}."
                        )
            except (ValueError, OSError, json.JSONDecodeError) as e:
                logger.debug(f"读取存储的时间戳失败: {e}")
                config_changed = True

        if not next_dt or next_dt <= datetime.now() or config_changed:
            next_dt = next_random_datetime(start_time, end_time, interval_days=days)
            logger.bind(scheme="telechecker").bind(log=True).info(
                f"下一次签到将在 {next_dt.strftime('%m-%d %H:%M %p')} 进行."
            )
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

        await checkiner(config, instant=instant)


async def monitorer(config: dict):
    """监控器入口函数."""
    logger.debug("正在启动消息监控模块.")
    jobs = []
    async with ClientsSession.from_config(config, monitor=(True, False)) as clients:
        async for tg in clients:
            log = logger.bind(scheme="telemonitor", username=tg.me.name)
            logger.info("已连接到 Telegram, 监控器正在初始化.")
            service_config = config.get("telegram", [])[tg._config_index].get("service", {})
            if not service_config:
                service_config: dict = config.get("service", {})
            names = service_config.get("monitor", None)
            clses = extract(get_cls("monitor", names=names))
            if not clses:
                log.warning("没有任何有效监控站点, 监控将跳过.")
            if not await Link(tg).auth("monitorer", log_func=log.error):
                continue
            names = []
            for cls in clses:
                cls_config = config.get("monitor", {}).get(cls.__module__.rsplit(".", 1)[-1], {})
                jobs.append(
                    asyncio.create_task(
                        cls(
                            tg,
                            nofail=config.get("nofail", True),
                            basedir=config.get("basedir", None),
                            proxy=config.get("proxy", None),
                            config=cls_config,
                        )._start()
                    )
                )
                names.append(cls.name)
            if names:
                log.debug(f'已启用监控器: {", ".join(names)}')
        await asyncio.gather(*jobs)


async def messager(config: dict):
    """自动水群入口函数."""
    logger.debug("正在启动自动水群模块.")
    messagers = []
    async with ClientsSession.from_config(config, send=(True, False)) as clients:
        async for tg in clients:
            log = logger.bind(scheme="telemessager", username=tg.me.name)
            logger.info("已连接到 Telegram, 自动水群正在初始化.")
            service_config = config.get("telegram", [])[tg._config_index].get("service", {})
            if not service_config:
                service_config: dict = config.get("service", {})
            names = service_config.get("messager", None)
            clses = extract(get_cls("messager", names=names))
            if not clses:
                log.warning("没有任何有效自动水群站点, 自动水群将跳过.")
            if not await Link(tg).auth("messager", log_func=log.error):
                continue
            for cls in clses:
                cls_config = config.get("messager", {}).get(cls.__module__.rsplit(".", 1)[-1], {})
                messagers.append(
                    cls(
                        {"api_id": tg.api_id, "api_hash": tg.api_hash, "phone": tg.phone_number},
                        me=tg.me,
                        nofail=config.get("nofail", True),
                        proxy=config.get("proxy", None),
                        basedir=config.get("basedir", None),
                        config=cls_config,
                    )
                )
    await asyncio.gather(*[m._start() for m in messagers])
