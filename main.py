import asyncio
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# (removed: sqlalchemy.text not needed for raw sqlite path)
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.core.star.filter.permission import PermissionType
from astrbot.api.provider import LLMResponse
from astrbot.core.provider.entities import TokenUsage
from astrbot.core.agent.run_context import ContextWrapper
from astrbot.core.astr_agent_context import AstrAgentContext
from astrbot.core.star.filter.event_message_type import EventMessageType
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.core.config.astrbot_config import AstrBotConfig
from astrbot.core.utils.astrbot_path import get_astrbot_data_path


@register(
    "astrbot_plugin_session_usage_stats",
    "OpenAI",
    "统计全部模型的调用次数、Token 消耗和趋势排行",
    "1.0.0",
    "",
)
class SessionUsageStatsPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.enable_auto_scan = bool(config.get("enable_auto_scan", False))
        self.auto_scan_interval_minutes = int(config.get("auto_scan_interval_minutes", 5) or 5)
        self.scan_batch_size = int(config.get("scan_batch_size", 500) or 500)
        self.enabled_platforms = list(config.get("enabled_platforms", ["webchat", "aiocqhttp"]) or ["webchat", "aiocqhttp"])
        self.include_threads = bool(config.get("include_threads", False))
        self.enable_event_capture = bool(config.get("enable_event_capture", False))
        self.event_capture_platforms = list(config.get("event_capture_platforms", ["aiocqhttp"]) or ["aiocqhttp"])
        self._event_lock = asyncio.Lock()
        self._model_call_lock = asyncio.Lock()
        self._model_call_buffer: dict[tuple[str, str, str, str, str, str], list[int]] = {}
        self._model_call_flush_task: asyncio.Task | None = None
        self._wrapped_provider_ids: set[int] = set()
        self._provider_call_context = {}
        # 最近一次成功聊天调用的真实 Provider/模型。
        # AstrBot fallback 时 context.get_using_provider() 仍可能指向默认第一个 Provider，
        # 因此 on_agent_done 需要优先参考这里由 Provider wrapper 捕获到的实际成功项。
        self._recent_chat_call_context: dict[str, Any] = {}
        if bool(config.get("include_threads", False)) and "webchat_thread" not in self.enabled_platforms:
            self.enabled_platforms.append("webchat_thread")
        # 事件捕获平台也必须进入有效统计平台集合。
        # 否则 enabled_platforms=[webchat] 且 event_capture_platforms=[aiocqhttp] 时，
        # QQ 的 on_agent_done 会在 _resolve_event_session 被提前过滤掉。
        self._effective_platforms = set(map(str, self.enabled_platforms)) | set(map(str, self.event_capture_platforms))

        self._scan_lock = asyncio.Lock()
        self._last_query_scan_ts = 0.0
        self._auto_task: asyncio.Task | None = None
        self._stopping = False

        try:
            self.data_dir = Path(StarTools.get_data_dir())
        except Exception:
            self.data_dir = Path(get_astrbot_data_path()) / "plugin_data" / "astrbot_plugin_session_usage_stats"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "usage_stats.db"
        self._init_sqlite()

    async def initialize(self):
        if self.enable_auto_scan:
            self._start_auto_scan_task()
        self._wrap_model_call_providers()
        self._model_call_flush_task = asyncio.create_task(self._model_call_flush_loop())
        self._register_page_apis()

    def _register_page_apis(self):
        self.context.register_web_api(
            "/astrbot_plugin_session_usage_stats/page/stats",
            self._api_stats,
            ["GET"],
            "模型用量数据查询"
        )
        self.context.register_web_api(
            "/astrbot_plugin_session_usage_stats/page/trend",
            self._api_trend,
            ["GET"],
            "模型用量趋势查询"
        )
        self.context.register_web_api(
            "/astrbot_plugin_session_usage_stats/page/model_stats",
            self._api_model_stats,
            ["GET"],
            "模型调用统计查询"
        )
        self.context.register_web_api(
            "/astrbot_plugin_session_usage_stats/page/clear",
            self._api_clear,
            ["POST"],
            "清空所有统计数据"
        )

    def _api_error_payload(self, action: str, e: Exception, **extra):
        logger.error(f"[session_usage_stats] 页面接口 {action} 失败: {e}", exc_info=True)
        payload = {"ok": False, "error": str(e) or e.__class__.__name__, **extra}
        return payload

    def _empty_api_window_payload(self, bucket_type: str, action: str, e: Exception):
        start, end, window_label, _cst = self._rolling_window(bucket_type)
        return self._api_error_payload(
            action,
            e,
            bucket_type=bucket_type,
            bucket_key=window_label,
            window_label=window_label,
            start_at=start.isoformat(timespec="seconds"),
            end_at=end.isoformat(timespec="seconds"),
            rows=[],
        )

    def _resolve_main_db_path(self) -> str:
        import os as _os
        import sqlite3 as _sqlite3

        def _normalize_sqlite_path(path: Any) -> str:
            text = str(path or "").strip()
            for prefix in ("sqlite+aiosqlite:///", "sqlite:///"):
                if text.startswith(prefix):
                    text = text.replace(prefix, "", 1)
                    break
            return text

        def _history_db_score(path: str) -> int:
            """Return >=0 for a usable history db, larger is preferred; -1 means invalid."""
            path = _normalize_sqlite_path(path)
            if not path or not _os.path.exists(path) or _os.path.getsize(path) <= 0:
                return -1
            try:
                conn = _sqlite3.connect(path, timeout=5)
                try:
                    row = conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='platform_message_history'"
                    ).fetchone()
                    if not row:
                        return -1
                    count = conn.execute("SELECT COUNT(*) FROM platform_message_history").fetchone()[0] or 0
                    score = int(count)
                    name = _os.path.basename(path)
                    if name == "data_v4.db":
                        score += 1_000_000
                    elif name.startswith("data_v"):
                        score += 500_000
                    return score
                finally:
                    conn.close()
            except Exception:
                return -1

        candidates: list[str] = []
        try:
            db = self.context.get_db()
            candidates.extend([
                getattr(db, "db_path", None),
                getattr(db, "path", None),
                getattr(db, "DATABASE_URL", ""),
            ])
        except Exception:
            pass

        data_root = str(get_astrbot_data_path())
        candidates.extend([
            "/AstrBot/data/data_v4.db",
            "/AstrBot/data/data_v3.db",
            "/AstrBot/data/data.db",
            "/AstrBot/data/astrbot.db",
            str(Path(data_root) / "data_v4.db"),
            str(Path(data_root) / "data_v3.db"),
            str(Path(data_root) / "data.db"),
            str(Path(data_root) / "astrbot.db"),
        ])

        # 兜底：扫描 AstrBot 数据目录下的 sqlite/db 文件，选择包含非空 platform_message_history 的主库。
        try:
            for root, _dirs, files in _os.walk(data_root):
                # 插件自己的数据目录通常不是主历史库，避免误选 favour.db 等空表。
                if "/plugin_data/" in root.replace("\\", "/"):
                    continue
                for fn in files:
                    if fn.endswith((".db", ".sqlite", ".sqlite3")):
                        candidates.append(_os.path.join(root, fn))
        except Exception:
            pass

        best_path = ""
        best_score = -1
        seen: set[str] = set()
        for cand in candidates:
            path = _normalize_sqlite_path(cand)
            if not path or path in seen:
                continue
            seen.add(path)
            score = _history_db_score(path)
            if score > best_score:
                best_score = score
                best_path = path
        return best_path if best_score >= 0 else ""

    def _rolling_window(self, bucket_type: str):
        from zoneinfo import ZoneInfo
        cst = ZoneInfo("Asia/Shanghai")
        now = datetime.now(cst)
        if bucket_type == "week":
            start = now - timedelta(days=7)
            label = "过去 7 天"
        elif bucket_type == "month":
            start = now - timedelta(days=30)
            label = "过去 30 天"
        else:
            start = now - timedelta(hours=24)
            label = "过去 24 小时"
        clear_at = self._get_clear_at(cst)
        if clear_at and clear_at > start:
            start = clear_at
        return start, now, label, cst

    def _get_clear_at(self, cst=None):
        """最近一次点击清空数据的时间；网页滚动窗口读取主历史库时要越过它。"""
        try:
            from zoneinfo import ZoneInfo
            cst = cst or ZoneInfo("Asia/Shanghai")
            conn = sqlite3.connect(self.db_path, timeout=10)
            try:
                row = conn.execute(
                    "SELECT clear_at FROM scan_state WHERE state_key=?",
                    ("global",),
                ).fetchone()
            finally:
                conn.close()
            if not row or not row[0]:
                return None
            text = str(row[0]).strip()
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(cst)
        except Exception:
            return None

    def _iter_history_bot_rows(self, start, end, cst):
        import os as _os
        import sqlite3 as _sq
        db_path = self._resolve_main_db_path()
        if not db_path or not _os.path.exists(str(db_path)):
            return []
        conn = _sq.connect(str(db_path), timeout=5)
        try:
            # platform_message_history.created_at 存储为 UTC 朴素时间；窗口用 CST 表达，查询前转回 UTC。
            start_utc = start.astimezone(timezone.utc).replace(tzinfo=None)
            end_utc = end.astimezone(timezone.utc).replace(tzinfo=None)
            rows = conn.execute(
                """
                SELECT platform_id, user_id, content, created_at
                FROM platform_message_history
                WHERE created_at >= ? AND created_at < ?
                ORDER BY created_at ASC
                """,
                (start_utc.isoformat(sep=" "), end_utc.isoformat(sep=" ")),
            ).fetchall()
        finally:
            conn.close()

        out = []
        for platform_id, session_id, content_raw, created_at in rows:
            if platform_id not in self.enabled_platforms:
                continue
            try:
                content = json.loads(content_raw) if isinstance(content_raw, str) else content_raw
            except Exception:
                continue
            if not isinstance(content, dict):
                continue
            if content.get("type") != "bot" or self._should_skip_history_content(content):
                continue
            dt = self._normalize_datetime(created_at)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt_cst = dt.astimezone(cst)
            if not (start <= dt_cst < end):
                continue
            input_tokens, output_tokens, total_tokens = self._extract_token_usage(content)
            out.append((platform_id, session_id, dt_cst, input_tokens, output_tokens, total_tokens))
        return out

    def _query_stored_bucket_rows(
        self,
        bucket_type: str,
        platform_id: str | None = None,
        session_id: str | None = None,
        bucket_keys: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """读取插件自身统计库聚合。

        页面口径是滚动窗口。实时捕获库过去只按自然 day/week/month 聚合，跨 0 点会像“今日”一样被截断；
        新数据会额外写入 hour 桶，这里优先按滚动窗口覆盖到的 hour 桶汇总。
        """
        keys = [str(k) for k in (bucket_keys or []) if str(k)]
        if keys:
            query_bucket_type = "hour"
            placeholders = ",".join("?" for _ in keys)
            sql = f"""
                SELECT platform_id, session_id,
                       SUM(round_count), SUM(user_message_count), SUM(bot_message_count),
                       SUM(input_tokens), SUM(output_tokens), SUM(total_tokens)
                FROM usage_stats
                WHERE bucket_type=? AND bucket_key IN ({placeholders})
            """
            params: list[Any] = [query_bucket_type, *keys]
        else:
            query_bucket_type = bucket_type
            bucket_key = self._current_bucket_key(bucket_type)
            sql = """
                SELECT platform_id, session_id,
                       SUM(round_count), SUM(user_message_count), SUM(bot_message_count),
                       SUM(input_tokens), SUM(output_tokens), SUM(total_tokens)
                FROM usage_stats
                WHERE bucket_type=? AND bucket_key=?
            """
            params = [query_bucket_type, bucket_key]
        if platform_id is not None:
            sql += " AND platform_id=?"
            params.append(str(platform_id))
        if session_id is not None:
            sql += " AND session_id=?"
            params.append(str(session_id))
        sql += " GROUP BY platform_id, session_id"

        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()

        out: list[dict[str, Any]] = []
        for row in rows:
            out.append({
                "platform_id": str(row[0]),
                "session_id": str(row[1]),
                "round_count": int(row[2] or 0),
                "user_message_count": int(row[3] or 0),
                "bot_message_count": int(row[4] or 0),
                "input_tokens": int(row[5] or 0),
                "output_tokens": int(row[6] or 0),
                "total_tokens": int(row[7] or 0),
            })
        return out

    def _query_stored_hour_rows_for_trend(self, hour_keys: list[str]) -> list[dict[str, Any]]:
        keys = [str(k) for k in (hour_keys or []) if str(k)]
        if not keys:
            return []
        placeholders = ",".join("?" for _ in keys)
        sql = f"""
            SELECT platform_id, session_id, bucket_key,
                   SUM(round_count), SUM(user_message_count), SUM(bot_message_count),
                   SUM(input_tokens), SUM(output_tokens), SUM(total_tokens)
            FROM usage_stats
            WHERE bucket_type='hour' AND bucket_key IN ({placeholders})
            GROUP BY platform_id, session_id, bucket_key
        """
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            rows = conn.execute(sql, keys).fetchall()
        finally:
            conn.close()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append({
                "platform_id": str(row[0]),
                "session_id": str(row[1]),
                "bucket_key": str(row[2]),
                "round_count": int(row[3] or 0),
                "user_message_count": int(row[4] or 0),
                "bot_message_count": int(row[5] or 0),
                "input_tokens": int(row[6] or 0),
                "output_tokens": int(row[7] or 0),
                "total_tokens": int(row[8] or 0),
            })
        return out

    def _query_rolling_usage(self, bucket_type: str, platform_id: str | None = None, session_id: str | None = None):
        start, end, window_label, cst = self._rolling_window(bucket_type)
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        hour_keys = self._rolling_hour_bucket_keys(start, end, cst)

        # 第一权威源：插件自身 usage_stats 的 hour 桶。
        # scan_incremental 会在每次查询前（_scan_before_query）把所有主库新消息同步进 hour 桶，
        # 所以这里读到的数据已经包含截止当前的全部历史+实时消耗。
        # 即使主历史库的消息被清空，统计结果也不会丢失。
        for row in self._query_stored_bucket_rows(bucket_type, platform_id, session_id, hour_keys):
            key = (str(row["platform_id"]), str(row["session_id"]))
            grouped[key] = row

        # 兜底：主历史库的精确扫描。
        # 仅作为插件刚启用且尚未进行首次扫描、或历史消息未被任何路径捕获的极端情况下的补偿。
        # 如果 hour 桶里已经有了该会话的数据，则跳过主历史库的累加，防止数据重复计算（双重计入）。
        for pid, sid, _dt_cst, input_tokens, output_tokens, total_tokens in self._iter_history_bot_rows(start, end, cst):
            pid = str(pid)
            sid = str(sid)
            if platform_id is not None and pid != str(platform_id):
                continue
            if session_id is not None and sid != str(session_id):
                continue
            key = (pid, sid)
            if key in grouped:
                continue
            item = grouped.setdefault(key, {
                "platform_id": pid,
                "session_id": sid,
                "round_count": 0,
                "user_message_count": 0,
                "bot_message_count": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
            })
            item["round_count"] += 1
            item["user_message_count"] += 1
            item["bot_message_count"] += 1
            item["input_tokens"] += input_tokens
            item["output_tokens"] += output_tokens
            item["total_tokens"] += total_tokens

        return sorted(grouped.values(), key=lambda x: x["total_tokens"], reverse=True), window_label, start, end

    async def _api_stats(self):
        from quart import jsonify, request as qreq
        bucket_type = qreq.args.get("bucket_type", "day")
        if bucket_type not in ("day", "week", "month"):
            bucket_type = "day"
        try:
            # 页面查询前先做一次轻量增量补扫，避免面板数据滞后一轮自动扫描。
            await self._scan_before_query()
            data, window_label, start, end = self._query_rolling_usage(bucket_type)
            return jsonify({
                "ok": True,
                "bucket_type": bucket_type,
                "bucket_key": window_label,
                "window_label": window_label,
                "start_at": start.isoformat(timespec="seconds"),
                "end_at": end.isoformat(timespec="seconds"),
                "rows": data,
            })
        except Exception as e:
            # 重载插件或数据库短暂锁定时，返回可渲染的空数据，避免前端直接收到 500。
            return jsonify(self._empty_api_window_payload(bucket_type, "stats", e))

    async def _api_trend(self):
        from quart import jsonify, request as qreq

        bucket_type = qreq.args.get("bucket_type", "day")
        if bucket_type not in ("day", "week", "month"):
            bucket_type = "day"
        try:
            # 趋势图也要查询前补扫，否则最近数据可能不是最新。
            await self._scan_before_query()

            start, end, window_label, cst = self._rolling_window(bucket_type)
            if bucket_type == "day":
                granularity = "hour"
                step = timedelta(hours=1)
                count = 24
                labels = [(start + step * i).strftime("%m-%d %H:00") for i in range(count)]
                def key_of(dt):
                    idx = int((dt - start).total_seconds() // 3600)
                    return labels[idx] if 0 <= idx < count else None
            elif bucket_type == "week":
                granularity = "day"
                step = timedelta(days=1)
                count = 7
                labels = [(start + step * i).strftime("%Y-%m-%d") for i in range(count)]
                def key_of(dt):
                    idx = int((dt - start).total_seconds() // 86400)
                    return labels[idx] if 0 <= idx < count else None
            else:
                granularity = "day"
                step = timedelta(days=1)
                count = 30
                labels = [(start + step * i).strftime("%Y-%m-%d") for i in range(count)]
                def key_of(dt):
                    idx = int((dt - start).total_seconds() // 86400)
                    return labels[idx] if 0 <= idx < count else None

            empty = {
                k: {
                    "bucket_key": k,
                    "round_count": 0,
                    "user_message_count": 0,
                    "bot_message_count": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_tokens": 0,
                    "session_count": 0,
                }
                for k in labels
            }
            sessions_by_key: dict[str, set[str]] = {k: set() for k in labels}

            # 趋势图优先使用插件自身 usage_stats 的 hour 桶作为第一权威源驱动。
            # 这样即使主历史库的消息被清理，趋势图也不会丢数。
            # 我们记录已经填入的会话小时 key，防止兜底主历史库扫描时重复计入（双重计入）。
            filled_keys: set[tuple[str, str, str]] = set()

            for row in self._query_stored_hour_rows_for_trend(self._rolling_hour_bucket_keys(start, end, cst)):
                try:
                    hour_dt = datetime.strptime(row["bucket_key"], "%Y-%m-%d %H").replace(tzinfo=cst)
                except Exception:
                    continue
                key = key_of(hour_dt)
                if key not in empty:
                    continue
                item = empty[key]
                item["round_count"] += int(row["round_count"] or 0)
                item["user_message_count"] += int(row["user_message_count"] or 0)
                item["bot_message_count"] += int(row["bot_message_count"] or 0)
                item["input_tokens"] += int(row["input_tokens"] or 0)
                item["output_tokens"] += int(row["output_tokens"] or 0)
                item["total_tokens"] += int(row["total_tokens"] or 0)
                sessions_by_key[key].add(f"{row['platform_id']}:{row['session_id']}")
                filled_keys.add((str(row["platform_id"]), str(row["session_id"]), row["bucket_key"]))

            # 兜底：主历史库的精确扫描，仅用于补偿未及时进入 hour 桶的边缘场景。
            # 若对应的会话在对应的小时已经存在于实时库中，则跳过，防止双重计入。
            for platform_id, session_id, dt_cst, input_tokens, output_tokens, total_tokens in self._iter_history_bot_rows(start, end, cst):
                key = key_of(dt_cst)
                if key not in empty:
                    continue
                
                # 构造 hour 格式的 key 用于去重比对
                hour_str = dt_cst.strftime("%Y-%m-%d %H")
                if (str(platform_id), str(session_id), hour_str) in filled_keys:
                    continue

                item = empty[key]
                item["round_count"] += 1
                item["user_message_count"] += 1
                item["bot_message_count"] += 1
                item["input_tokens"] += input_tokens
                item["output_tokens"] += output_tokens
                item["total_tokens"] += total_tokens
                sessions_by_key[key].add(f"{platform_id}:{session_id}")

            data = []
            for key in labels:
                item = empty[key]
                item["session_count"] = len(sessions_by_key[key])
                data.append(item)
            return jsonify({
                "ok": True,
                "bucket_type": bucket_type,
                "bucket_key": window_label,
                "window_label": window_label,
                "granularity": granularity,
                "start_at": start.isoformat(timespec="seconds"),
                "end_at": end.isoformat(timespec="seconds"),
                "rows": data,
            })
        except Exception as e:
            payload = self._empty_api_window_payload(bucket_type, "trend", e)
            payload["granularity"] = "hour" if bucket_type == "day" else "day"
            return jsonify(payload)

    async def _api_model_stats(self):
        from quart import jsonify, request as qreq
        bucket_type = qreq.args.get("bucket_type", "day")
        scope = qreq.args.get("scope", "chat")
        if bucket_type not in ("day", "week", "month"):
            bucket_type = "day"
        if scope not in ("chat", "all"):
            scope = "chat"
        try:
            # 模型占比只读取 model_usage_stats/model_call_stats。
            # 历史补扫会扫描主消息库，切换“对话模型/全部模型”时没有必要执行，避免界面偶发卡顿。
            try:
                self._wrap_model_call_providers()
            except Exception as e:
                logger.debug(f"[session_usage_stats] 查询模型统计前补包 Provider 失败: {e}", exc_info=True)
            if scope == "all":
                await self._flush_model_call_buffer()
            rows, window_label, start, end = self._query_model_usage(bucket_type, scope)
            return jsonify({
                "ok": True,
                "bucket_type": bucket_type,
                "bucket_key": window_label,
                "window_label": window_label,
                "start_at": start.isoformat(timespec="seconds"),
                "end_at": end.isoformat(timespec="seconds"),
                "scope": scope,
                "rows": rows,
            })
        except Exception as e:
            return jsonify(self._empty_api_window_payload(bucket_type, "model_stats", e))

    def _query_model_usage(self, bucket_type: str, scope: str = "chat"):
        """查询模型维度统计。

        scope=chat 读取原来的对话完成统计；scope=all 读取 provider 调用层统计。
        全部模型模式不在前端暴露模型用途，只按模型名与提供商聚合。
        """
        start, end, window_label, cst = self._rolling_window(bucket_type)
        hour_keys = self._rolling_hour_bucket_keys(start, end, cst)
        placeholders = ",".join("?" for _ in hour_keys) or "?"
        params = [*hour_keys] if hour_keys else ["__none__"]
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            if scope == "all":
                # “全部模型”的聊天完成调用也以 AstrBot provider_stats 为权威来源，
                # 这样能和“对话模型”保持同一套 provider_id/provider_model 归因，避免 wrapper 把
                # Agent 链路里的中间/后台调用记成其它 Provider。
                rows = []
                try:
                    main_db_path = self._resolve_main_db_path()
                    if main_db_path:
                        main_conn = sqlite3.connect(str(main_db_path), timeout=5)
                        try:
                            start_utc = start.astimezone(timezone.utc).replace(tzinfo=None)
                            end_utc = end.astimezone(timezone.utc).replace(tzinfo=None)
                            rows = main_conn.execute(
                                """
                                SELECT provider_model, provider_id,
                                       COUNT(*) AS call_count,
                                       SUM(COALESCE(token_input_other,0) + COALESCE(token_input_cached,0)) AS input_tokens,
                                       SUM(COALESCE(token_output,0)) AS output_tokens,
                                       SUM(COALESCE(token_input_other,0) + COALESCE(token_input_cached,0) + COALESCE(token_output,0)) AS total_tokens
                                FROM provider_stats
                                WHERE status='completed'
                                  AND created_at >= ? AND created_at < ?
                                GROUP BY provider_model, provider_id
                                """,
                                (start_utc.isoformat(sep=" "), end_utc.isoformat(sep=" ")),
                            ).fetchall()
                        finally:
                            main_conn.close()
                except Exception as e:
                    logger.debug(f"[session_usage_stats] 查询 provider_stats 全部模型聊天部分失败，回退 model_call_stats: {e}", exc_info=True)
                    rows = []

                # 继续把 wrapper 捕获到的后台调用合并进“全部模型”。
                # background(system/__background__) 里混有两类调用：
                # 1) 标准 Agent 对话的 Provider 调用：通常已进入 AstrBot provider_stats；
                # 2) 其它插件直连 Provider 的调用：有些不会进入 provider_stats，只能靠 wrapper 捕获。
                # 因此不能简单全放或全挡。这里采用“差额合并”：
                # - provider_stats 先作为权威基线；
                # - 同模型同 Provider 的 background 有 Token 调用，扣掉 provider_stats 已覆盖的部分；
                # - 只把剩余差额视为插件直连调用并入全部模型；
                # - 0 Token 的 embedding/rerank 等调用仍直接并入次数。
                background_token_rows = conn.execute(
                    f"""
                    SELECT model_name, provider_name,
                           SUM(call_count) AS call_count,
                           SUM(input_tokens) AS input_tokens,
                           SUM(output_tokens) AS output_tokens,
                           SUM(total_tokens) AS total_tokens
                    FROM model_call_stats
                    WHERE bucket_type='hour' AND bucket_key IN ({placeholders})
                      AND platform_id='system' AND session_id='__background__'
                      AND total_tokens>0
                    GROUP BY model_name, provider_name
                    """,
                    params,
                ).fetchall()
                background_zero_rows = conn.execute(
                    f"""
                    SELECT model_name, provider_name,
                           SUM(call_count) AS call_count,
                           SUM(input_tokens) AS input_tokens,
                           SUM(output_tokens) AS output_tokens,
                           SUM(total_tokens) AS total_tokens
                    FROM model_call_stats
                    WHERE bucket_type='hour' AND bucket_key IN ({placeholders})
                      AND total_tokens=0
                    GROUP BY model_name, provider_name
                    """,
                    params,
                ).fetchall()

                base_rows = list(rows or [])
                covered: dict[tuple[str, str], list[int]] = {}
                for r in base_rows:
                    key = (str(r[0] or "unknown"), str(r[1] or "unknown"))
                    vals = covered.setdefault(key, [0, 0, 0, 0])
                    vals[0] += int(r[2] or 0)
                    vals[1] += int(r[3] or 0)
                    vals[2] += int(r[4] or 0)
                    vals[3] += int(r[5] or 0)

                plugin_direct_rows = []
                for r in list(background_token_rows or []):
                    key = (str(r[0] or "unknown"), str(r[1] or "unknown"))
                    bg = [int(r[2] or 0), int(r[3] or 0), int(r[4] or 0), int(r[5] or 0)]
                    cov = covered.get(key, [0, 0, 0, 0])
                    delta = [max(0, bg[i] - cov[i]) for i in range(4)]
                    if delta[0] > 0 or delta[3] > 0:
                        plugin_direct_rows.append((key[0], key[1], delta[0], delta[1], delta[2], delta[3]))

                rows = base_rows + plugin_direct_rows + list(background_zero_rows or [])

                # 若主库不可用且没有可合并后台调用，再退回旧 model_call_stats，保证页面不空白。
                if not rows:
                    rows = conn.execute(
                        f"""
                        SELECT model_name, provider_name,
                               SUM(call_count) AS call_count,
                               SUM(input_tokens) AS input_tokens,
                               SUM(output_tokens) AS output_tokens,
                               SUM(total_tokens) AS total_tokens
                        FROM model_call_stats
                        WHERE bucket_type='hour' AND bucket_key IN ({placeholders})
                        GROUP BY model_name, provider_name
                        ORDER BY total_tokens DESC, call_count DESC
                        """,
                        params,
                    ).fetchall()
                rows = sorted(rows, key=lambda r: (int(r[4] or 0), int(r[2] or 0)), reverse=True)
            else:
                # “对话模型”优先读取 AstrBot 主库 provider_stats。
                # 这是 AstrBot 每轮 Agent 完成后写入的权威记录，能正确反映 webchat 会话级模型、fallback、路由后的实际完成 Provider。
                rows = []
                try:
                    main_db_path = self._resolve_main_db_path()
                    if main_db_path:
                        main_conn = sqlite3.connect(str(main_db_path), timeout=5)
                        try:
                            start_utc = start.astimezone(timezone.utc).replace(tzinfo=None)
                            end_utc = end.astimezone(timezone.utc).replace(tzinfo=None)
                            rows = main_conn.execute(
                                """
                                SELECT provider_model, provider_id,
                                       COUNT(*) AS call_count,
                                       SUM(COALESCE(token_input_other,0) + COALESCE(token_input_cached,0)) AS input_tokens,
                                       SUM(COALESCE(token_output,0)) AS output_tokens,
                                       SUM(COALESCE(token_input_other,0) + COALESCE(token_input_cached,0) + COALESCE(token_output,0)) AS total_tokens
                                FROM provider_stats
                                WHERE status='completed'
                                  AND created_at >= ? AND created_at < ?
                                GROUP BY provider_model, provider_id
                                ORDER BY total_tokens DESC, call_count DESC
                                """,
                                (start_utc.isoformat(sep=" "), end_utc.isoformat(sep=" ")),
                            ).fetchall()
                        finally:
                            main_conn.close()
                except Exception as e:
                    logger.debug(f"[session_usage_stats] 查询 provider_stats 对话模型失败，回退 model_usage_stats: {e}", exc_info=True)
                    rows = []
                if not rows:
                    rows = conn.execute(
                        f"""
                        SELECT model_name, provider_name,
                               SUM(call_count) AS call_count,
                               SUM(input_tokens) AS input_tokens,
                               SUM(output_tokens) AS output_tokens,
                               SUM(total_tokens) AS total_tokens
                        FROM model_usage_stats
                        WHERE bucket_type='hour' AND bucket_key IN ({placeholders})
                        GROUP BY model_name, provider_name
                        ORDER BY total_tokens DESC, call_count DESC
                        """,
                        params,
                    ).fetchall()
        finally:
            conn.close()

        # Final display-level merge.
        #
        # scope=all intentionally combines multiple sources:
        #   1) AstrBot provider_stats as the authoritative chat-call baseline;
        #   2) wrapper-captured background/direct provider calls as a delta;
        #   3) 0-token embedding/rerank calls.
        # Some provider ids are configured as "provider/model" and are normalized for
        # display below. Without a second merge after normalization, the same visible
        # model/provider pair can appear twice in the dashboard, e.g.
        # "kimi-k2.6 · 小水管无限制" split into provider_stats baseline and wrapper delta.
        merged: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            model_display = str(row[0] or "unknown")
            provider_display = self._normalize_provider_display_name(str(row[1] or "unknown"), model_display)
            key = (model_display, provider_display)
            item = merged.setdefault(key, {
                "model_name": model_display,
                "provider_name": provider_display,
                "call_count": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
            })
            item["call_count"] += int(row[2] or 0)
            item["input_tokens"] += int(row[3] or 0)
            item["output_tokens"] += int(row[4] or 0)
            item["total_tokens"] += int(row[5] or 0)

        out = sorted(
            merged.values(),
            key=lambda r: (int(r.get("total_tokens") or 0), int(r.get("call_count") or 0)),
            reverse=True,
        )
        return out, window_label, start, end

    def _get_main_history_max_id(self) -> int:
        """获取 AstrBot 主历史表当前最大消息 ID，用于清空后重置扫描游标。"""
        import sqlite3 as _sqlite3

        db_path = self._resolve_main_db_path()
        if not db_path:
            return 0

        conn = _sqlite3.connect(str(db_path), timeout=5)
        try:
            row = conn.execute("SELECT MAX(id) FROM platform_message_history").fetchone()
            return int(row[0] or 0) if row else 0
        finally:
            conn.close()

    async def _api_clear(self):
        from quart import jsonify
        import sqlite3 as _sq

        try:
            # 清空时必须把扫描游标推进到主历史库当前末尾。
            # 否则下一次查询前补扫/自动扫描会从 0 重新扫描旧消息，表现为“清空后数据又回来了”。
            async with self._scan_lock:
                max_history_id = self._get_main_history_max_id()
                conn = _sq.connect(self.db_path, timeout=10)
                try:
                    conn.execute("PRAGMA busy_timeout=10000")
                    conn.execute("DELETE FROM usage_stats")
                    conn.execute("DELETE FROM model_usage_stats")
                    conn.execute("DELETE FROM model_call_stats")
                    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
                    conn.execute(
                        """
                        INSERT INTO scan_state(state_key, last_message_id, last_scan_at, clear_at)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(state_key) DO UPDATE SET
                            last_message_id=excluded.last_message_id,
                            last_scan_at=excluded.last_scan_at,
                            clear_at=excluded.clear_at
                        """,
                        ("global", int(max_history_id), now, now),
                    )
                    conn.commit()
                finally:
                    conn.close()
            return jsonify({"ok": True, "last_message_id": max_history_id})
        except Exception as e:
            return jsonify(self._api_error_payload("clear", e, last_message_id=None))

    async def terminate(self):
        self._stopping = True
        if self._model_call_flush_task and not self._model_call_flush_task.done():
            self._model_call_flush_task.cancel()
            try:
                await self._model_call_flush_task
            except asyncio.CancelledError:
                pass
            except BaseException as e:
                logger.warning(f"[session_usage_stats] 模型调用 flush 任务停止异常: {e}", exc_info=True)
        try:
            await self._flush_model_call_buffer()
        except Exception as e:
            logger.warning(f"[session_usage_stats] 停止前 flush 模型调用统计失败: {e}", exc_info=True)
        if self._auto_task and not self._auto_task.done():
            self._auto_task.cancel()
            try:
                await self._auto_task
            except asyncio.CancelledError:
                pass
            except BaseException as e:
                logger.warning(f"[session_usage_stats] 自动扫描任务停止异常: {e}", exc_info=True)

    async def _model_call_flush_loop(self):
        while not self._stopping:
            try:
                await asyncio.sleep(5)
                await self._flush_model_call_buffer()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[session_usage_stats] flush 模型调用统计失败: {e}", exc_info=True)

    def _provider_display_name(self, provider: Any) -> tuple[str, str]:
        cfg = getattr(provider, "provider_config", {}) or {}
        if not isinstance(cfg, dict):
            cfg = {}

        model_name = (
            getattr(provider, "model", None)
            or getattr(provider, "model_name", None)
            or cfg.get("embedding_model")
            or cfg.get("rerank_model")
            or cfg.get("nvidia_rerank_model")
            or cfg.get("model")
            or cfg.get("model_name")
            or cfg.get("model_id")
            or "unknown"
        )
        provider_name = (
            cfg.get("id")
            or cfg.get("type")
            or getattr(provider, "provider_type", None)
            or provider.__class__.__name__
            or "unknown"
        )
        return str(model_name or "unknown")[:160], str(provider_name or "unknown")[:120]

    def _remember_successful_chat_provider(self, provider: Any, resp: Any = None, args: Any = None, kwargs: dict | None = None):
        """Record the actual provider/model that produced a successful chat response.

        This does not write statistics directly. on_agent_done remains the single writer for
        chat call counts, while this context fixes fallback attribution.
        """
        try:
            model_name, provider_name = self._provider_display_name(provider)
            if kwargs:
                model_name = str(kwargs.get("model") or model_name or "unknown")
            # raw_completion.model 在代理/中转服务中可能是上游实际路由模型，
            # 不一定等于 AstrBot 配置中用户选择的模型名。
            # 因此优先使用 kwargs/provider_config 中的配置模型名；只有配置取不到时才用 raw model 兜底。
            raw = getattr(resp, "raw_completion", None) if resp is not None else None
            raw_model = self._pick_model_from_raw_completion(raw)
            if raw_model and (not model_name or model_name == "unknown"):
                model_name = raw_model
            in_tok, out_tok = self._extract_llm_response_tokens(resp)
            self._recent_chat_call_context = {
                "model_name": str(model_name or "unknown")[:160],
                "provider_name": str(provider_name or "unknown")[:120],
                "input_tokens": int(in_tok or 0),
                "output_tokens": int(out_tok or 0),
                "ts": datetime.now().timestamp(),
            }
        except Exception as e:
            logger.debug(f"[session_usage_stats] 记录实际聊天 Provider 上下文失败: {e}", exc_info=True)

    @staticmethod
    def _pick_model_from_raw_completion(raw: Any) -> str:
        if raw is None:
            return ""
        try:
            if isinstance(raw, dict):
                for key in ("model", "model_name", "model_id", "deployment", "engine"):
                    val = raw.get(key)
                    if val and not isinstance(val, (dict, list, tuple, set)):
                        return str(val).strip()
            for key in ("model", "model_name", "model_id", "deployment", "engine"):
                val = getattr(raw, key, None)
                if val and not isinstance(val, (dict, list, tuple, set)):
                    return str(val).strip()
        except Exception:
            return ""
        return ""

    @staticmethod
    def _is_generic_provider_name(name: str) -> bool:
        return str(name or "").strip().lower() in {
            "openai", "gemini", "google", "genai", "anthropic", "unknown", ""
        }

    @staticmethod
    def _normalize_provider_display_name(provider_name: str, model_name: str = "") -> str:
        """Make provider display stable for the dashboard.

        Some provider ids are configured as "provider/model". The page already renders
        "model · provider"; keeping the model suffix in provider_name makes it look like
        the same model appears twice, e.g. "Gemini · 公益Gemini/Gemini".
        """
        provider = str(provider_name or "unknown").strip() or "unknown"
        model = str(model_name or "").strip()
        if "/" in provider and model:
            left, right = provider.rsplit("/", 1)
            if right.strip().lower() == model.lower():
                provider = left.strip() or provider
        return provider[:120]

    def _provider_name_from_config_model(self, model_name: str, current_provider_name: str = "") -> str:
        """Try to convert a generic SDK provider name (openai/gemini/...) to AstrBot provider id.

        OpenAI-compatible providers often return raw_completion.model='gpt-5.5' while the SDK module
        only tells us 'openai'. The UI should display the AstrBot provider id, not the generic SDK type.
        """
        model_name = str(model_name or "").strip()
        if not model_name:
            return ""
        try:
            pm = getattr(self.context, "provider_manager", None)
            candidates = []
            if pm:
                for prov in list(getattr(pm, "provider_insts", []) or []):
                    cfg = getattr(prov, "provider_config", {}) or {}
                    if not isinstance(cfg, dict):
                        cfg = {}
                    cfg_model = str(
                        cfg.get("model") or cfg.get("model_name") or cfg.get("model_id")
                        or getattr(prov, "model", None) or getattr(prov, "model_name", None) or ""
                    ).strip()
                    cfg_id = str(cfg.get("id") or "").strip()
                    if cfg_model == model_name and cfg_id:
                        candidates.append(cfg_id)
            # 唯一匹配时可直接采用。
            if len(candidates) == 1:
                return candidates[0]
            # 多个 provider 使用同一 model 时，优先保持当前 Provider 名（如果它就是候选项）。
            if current_provider_name and current_provider_name in candidates:
                return current_provider_name
            # 再按全局默认/回退顺序选一个，避免落成 openai 这种泛称。
            try:
                cfg = getattr(getattr(self.context, "provider_manager", None), "provider_settings", {}) or {}
                order = []
                default_id = cfg.get("default_provider_id")
                if default_id:
                    order.append(str(default_id))
                order.extend([str(x) for x in (cfg.get("fallback_chat_models") or [])])
                for pid in order:
                    if pid in candidates:
                        return pid
            except Exception:
                pass
            if candidates:
                return candidates[0]
        except Exception as e:
            logger.debug(f"[session_usage_stats] 从配置反查 Provider 名失败: {e}", exc_info=True)
        return ""

    def _record_model_call_now(
        self,
        model_name: str,
        provider_name: str,
        input_tokens: int = 0,
        output_tokens: int = 0,
        platform_id: str = "system",
        session_id: str = "__background__",
    ):
        total_tokens = int(input_tokens or 0) + int(output_tokens or 0)
        created_at = datetime.now()
        for bucket_type, bucket_key in self._build_bucket_keys(created_at).items():
            key = (
                str(platform_id or "system"),
                str(session_id or "__background__"),
                str(model_name or "unknown"),
                str(provider_name or "unknown"),
                str(bucket_type),
                str(bucket_key),
            )
            vals = self._model_call_buffer.setdefault(key, [0, 0, 0, 0])
            vals[0] += 1
            vals[1] += int(input_tokens or 0)
            vals[2] += int(output_tokens or 0)
            vals[3] += total_tokens

    async def _flush_model_call_buffer(self):
        try:
            self._wrap_model_call_providers()
        except Exception as e:
            logger.debug(f"[session_usage_stats] flush 前补包 Provider 失败: {e}", exc_info=True)
        async with self._model_call_lock:
            if not self._model_call_buffer:
                return
            items = list(self._model_call_buffer.items())
            self._model_call_buffer.clear()
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            conn.execute("PRAGMA busy_timeout=10000")
            now = datetime.now().isoformat(timespec="seconds")
            conn.executemany(
                """
                INSERT INTO model_call_stats(
                    platform_id, session_id, model_name, provider_name, bucket_type, bucket_key,
                    call_count, input_tokens, output_tokens, total_tokens, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform_id, session_id, model_name, provider_name, bucket_type, bucket_key)
                DO UPDATE SET
                    call_count = call_count + excluded.call_count,
                    input_tokens = input_tokens + excluded.input_tokens,
                    output_tokens = output_tokens + excluded.output_tokens,
                    total_tokens = total_tokens + excluded.total_tokens,
                    updated_at = excluded.updated_at
                """,
                [(*key, int(vals[0]), int(vals[1]), int(vals[2]), int(vals[3]), now) for key, vals in items],
            )
            conn.commit()
        except Exception:
            async with self._model_call_lock:
                for key, vals in items:
                    cur = self._model_call_buffer.setdefault(key, [0, 0, 0, 0])
                    cur[0] += vals[0]
                    cur[1] += vals[1]
                    cur[2] += vals[2]
                    cur[3] += vals[3]
            raise
        finally:
            conn.close()

    @staticmethod
    def _extract_llm_response_tokens(resp: Any) -> tuple[int, int]:
        if resp is None:
            return 0, 0
        try:
            return SessionUsageStatsPlugin._extract_event_token_usage(resp)
        except Exception:
            return 0, 0

    def _wrap_model_call_providers(self) -> dict[str, int]:
        """Wrap currently loaded providers.

        AstrBot versions / deployments may load providers after plugin initialize().
        Therefore this method is intentionally cheap and can be called repeatedly
        before querying / flushing model usage; already wrapped providers are skipped.
        """
        pm = getattr(self.context, "provider_manager", None)
        stats = {"seen": 0, "wrapped": 0}
        if not pm:
            return stats
        providers = []
        for attr in (
            "provider_insts",
            "embedding_provider_insts",
            "rerank_provider_insts",
            "providers",
            "llm_providers",
            "embedding_providers",
            "rerank_providers",
        ):
            value = getattr(pm, attr, None)
            if isinstance(value, dict):
                providers.extend(list(value.values()))
            elif value:
                try:
                    providers.extend(list(value))
                except TypeError:
                    providers.append(value)
        # De-duplicate while preserving order.
        dedup = []
        seen_ids = set()
        for provider in providers:
            if not provider or id(provider) in seen_ids:
                continue
            seen_ids.add(id(provider))
            dedup.append(provider)
        stats["seen"] = len(dedup)
        before = len(self._wrapped_provider_ids)
        for provider in dedup:
            self._wrap_single_provider(provider)
        stats["wrapped"] = max(0, len(self._wrapped_provider_ids) - before)
        return stats

    def _wrap_single_provider(self, provider: Any):
        if not provider or id(provider) in self._wrapped_provider_ids:
            return
        self._wrapped_provider_ids.add(id(provider))
        model_name, provider_name = self._provider_display_name(provider)

        def wrap_async_method(method_name: str, recorder):
            current = getattr(provider, method_name, None)
            if not callable(current):
                return
            # 热重载后 provider 实例可能还挂着旧插件实例创建的 wrapper。
            # 这里剥回原始方法并重新包到当前实例，避免统计继续写到旧实例的缓冲区。
            original = getattr(current, "__session_usage_original__", current)
            if not callable(original):
                return
            async def wrapped(*args, **kwargs):
                result = await original(*args, **kwargs)
                try:
                    recorder(result, args, kwargs)
                except Exception as e:
                    logger.debug(f"[session_usage_stats] 记录模型调用失败 {method_name}: {e}", exc_info=True)
                return result
            wrapped.__session_usage_wrapped__ = True
            wrapped.__session_usage_original__ = original
            wrapped.__session_usage_owner__ = id(self)
            setattr(provider, method_name, wrapped)

        def record_chat(resp, args, kwargs):
            # Provider 层聊天调用直接进入“全部模型”。
            # 这能覆盖插件从 AstrBot 模型配置中取 Provider 后直接 text_chat/text_chat_stream 的调用；
            # 标准对话的“对话模型”仍由 on_agent_done 写入 model_usage_stats。
            self._remember_successful_chat_provider(provider, resp, args, kwargs)
            recent = getattr(self, "_recent_chat_call_context", {}) or {}
            call_model = str(recent.get("model_name") or kwargs.get("model") or model_name or "unknown")
            call_provider = str(recent.get("provider_name") or provider_name or "unknown")
            in_tok = int(recent.get("input_tokens") or 0)
            out_tok = int(recent.get("output_tokens") or 0)
            self._record_model_call_now(call_model, call_provider, in_tok, out_tok)

        def record_plain_call(_resp, args, kwargs):
            # Embedding / rerank 在 AstrBot provider 封装后通常拿不到官方 usage。
            # 为避免 Token 图混入估算值，这类调用只统计次数，不写入 Token。
            self._record_model_call_now(model_name, provider_name, 0, 0)

        def wrap_stream_method(method_name: str):
            current = getattr(provider, method_name, None)
            if not callable(current):
                return
            original = getattr(current, "__session_usage_original__", current)
            if not callable(original):
                return
            async def wrapped(*args, **kwargs):
                last_resp = None
                async for item in original(*args, **kwargs):
                    last_resp = item
                    yield item
                try:
                    record_chat(last_resp, args, kwargs)
                except Exception as e:
                    logger.debug(f"[session_usage_stats] 记录流式模型调用失败 {method_name}: {e}", exc_info=True)
            wrapped.__session_usage_wrapped__ = True
            wrapped.__session_usage_original__ = original
            wrapped.__session_usage_owner__ = id(self)
            setattr(provider, method_name, wrapped)

        # 聊天 wrapper 直接写“全部模型”，同时记录实际成功 Provider/模型上下文；
        # on_agent_done 只写“对话模型”，避免标准对话重复计数。
        wrap_async_method("text_chat", record_chat)
        wrap_stream_method("text_chat_stream")
        wrap_async_method("get_embedding", record_plain_call)
        wrap_async_method("get_embeddings", record_plain_call)
        wrap_async_method("get_embeddings_batch", record_plain_call)
        wrap_async_method("rerank", record_plain_call)

    def _init_sqlite(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scan_state (
                    state_key TEXT PRIMARY KEY,
                    last_message_id INTEGER NOT NULL DEFAULT 0,
                    last_scan_at TEXT
                )
                """
            )
            try:
                cols = {r[1] for r in conn.execute("PRAGMA table_info(scan_state)").fetchall()}
                if "clear_at" not in cols:
                    conn.execute("ALTER TABLE scan_state ADD COLUMN clear_at TEXT")
            except Exception:
                logger.warning("[session_usage_stats] scan_state clear_at 迁移失败", exc_info=True)

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS usage_stats (
                    platform_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    bucket_type TEXT NOT NULL,
                    bucket_key TEXT NOT NULL,
                    round_count INTEGER NOT NULL DEFAULT 0,
                    user_message_count INTEGER NOT NULL DEFAULT 0,
                    bot_message_count INTEGER NOT NULL DEFAULT 0,
                    input_tokens INTEGER NOT NULL DEFAULT 0,
                    output_tokens INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT,
                    PRIMARY KEY (platform_id, session_id, bucket_type, bucket_key)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS model_usage_stats (
                    platform_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    model_name TEXT NOT NULL,
                    provider_name TEXT NOT NULL DEFAULT 'unknown',
                    bucket_type TEXT NOT NULL,
                    bucket_key TEXT NOT NULL,
                    call_count INTEGER NOT NULL DEFAULT 0,
                    input_tokens INTEGER NOT NULL DEFAULT 0,
                    output_tokens INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT,
                    PRIMARY KEY (platform_id, session_id, model_name, provider_name, bucket_type, bucket_key)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS model_call_stats (
                    platform_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    model_name TEXT NOT NULL,
                    provider_name TEXT NOT NULL DEFAULT 'unknown',
                    bucket_type TEXT NOT NULL,
                    bucket_key TEXT NOT NULL,
                    call_count INTEGER NOT NULL DEFAULT 0,
                    input_tokens INTEGER NOT NULL DEFAULT 0,
                    output_tokens INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT,
                    PRIMARY KEY (platform_id, session_id, model_name, provider_name, bucket_type, bucket_key)
                )
                """
            )
            # 模型占比接口频繁按 hour bucket 聚合，补索引能减少切换 scope 时的 SQLite 扫描成本。
            conn.execute("CREATE INDEX IF NOT EXISTS idx_model_usage_bucket ON model_usage_stats(bucket_type, bucket_key, model_name, provider_name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_model_call_bucket ON model_call_stats(bucket_type, bucket_key, model_name, provider_name)")
            conn.commit()
        finally:
            conn.close()

    def _get_last_message_id(self) -> int:
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            row = conn.execute(
                "SELECT last_message_id FROM scan_state WHERE state_key = ?",
                ("global",),
            ).fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()

    def _set_last_message_id(self, message_id: int):
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            now = datetime.now().isoformat(timespec="seconds")
            conn.execute(
                """
                INSERT INTO scan_state(state_key, last_message_id, last_scan_at, clear_at)
                VALUES (?, ?, ?, NULL)
                ON CONFLICT(state_key) DO UPDATE SET
                    last_message_id=excluded.last_message_id,
                    last_scan_at=excluded.last_scan_at
                """,
                ("global", int(message_id), now),
            )
            conn.commit()
        finally:
            conn.close()

    def _upsert_usage_row(
        self,
        platform_id: str,
        session_id: str,
        bucket_type: str,
        bucket_key: str,
        round_inc: int,
        user_inc: int,
        bot_inc: int,
        input_inc: int,
        output_inc: int,
        total_inc: int,
    ):
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            now = datetime.now().isoformat(timespec="seconds")
            conn.execute(
                """
                INSERT INTO usage_stats(
                    platform_id, session_id, bucket_type, bucket_key,
                    round_count, user_message_count, bot_message_count,
                    input_tokens, output_tokens, total_tokens, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform_id, session_id, bucket_type, bucket_key)
                DO UPDATE SET
                    round_count = round_count + excluded.round_count,
                    user_message_count = user_message_count + excluded.user_message_count,
                    bot_message_count = bot_message_count + excluded.bot_message_count,
                    input_tokens = input_tokens + excluded.input_tokens,
                    output_tokens = output_tokens + excluded.output_tokens,
                    total_tokens = total_tokens + excluded.total_tokens,
                    updated_at = excluded.updated_at
                """,
                (
                    platform_id,
                    session_id,
                    bucket_type,
                    bucket_key,
                    round_inc,
                    user_inc,
                    bot_inc,
                    input_inc,
                    output_inc,
                    total_inc,
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def _upsert_model_usage_row(
        self,
        platform_id: str,
        session_id: str,
        model_name: str,
        provider_name: str,
        bucket_type: str,
        bucket_key: str,
        call_inc: int,
        input_inc: int,
        output_inc: int,
        total_inc: int,
    ):
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            now = datetime.now().isoformat(timespec="seconds")
            conn.execute(
                """
                INSERT INTO model_usage_stats(
                    platform_id, session_id, model_name, provider_name, bucket_type, bucket_key,
                    call_count, input_tokens, output_tokens, total_tokens, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform_id, session_id, model_name, provider_name, bucket_type, bucket_key)
                DO UPDATE SET
                    call_count = call_count + excluded.call_count,
                    input_tokens = input_tokens + excluded.input_tokens,
                    output_tokens = output_tokens + excluded.output_tokens,
                    total_tokens = total_tokens + excluded.total_tokens,
                    updated_at = excluded.updated_at
                """,
                (
                    str(platform_id),
                    str(session_id),
                    str(model_name or "unknown"),
                    str(provider_name or "unknown"),
                    str(bucket_type),
                    str(bucket_key),
                    int(call_inc),
                    int(input_inc),
                    int(output_inc),
                    int(total_inc),
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _safe_attr(obj: Any, name: str) -> Any:
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj.get(name)
        try:
            return getattr(obj, name, None)
        except Exception:
            return None

    def _provider_stats_model_info(self, event: AstrMessageEvent | None = None, max_age_seconds: int = 600) -> tuple[str, str]:
        """从 AstrBot 主库 provider_stats 读取本轮实际完成回复使用的 Provider/模型。

        provider_stats 是 AstrBot 在 Agent 完成后写入的权威记录，包含 provider_id/provider_model。
        对 webchat 会话级模型、fallback、路由插件等场景，比 context.get_using_provider() 更可靠。
        """
        if not event:
            return "", ""
        try:
            umo = str(getattr(event, "unified_msg_origin", None) or "").strip()
            if not umo:
                return "", ""
            db_path = self._resolve_main_db_path()
            if not db_path:
                return "", ""
            conn = sqlite3.connect(str(db_path), timeout=5)
            try:
                row = conn.execute(
                    """
                    SELECT provider_model, provider_id, end_time, created_at
                    FROM provider_stats
                    WHERE umo=? AND status='completed'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (umo,),
                ).fetchone()
            finally:
                conn.close()
            if not row:
                return "", ""
            provider_model, provider_id, end_time, _created_at = row
            try:
                if end_time and abs(datetime.now().timestamp() - float(end_time)) > max_age_seconds:
                    return "", ""
            except Exception:
                pass
            return str(provider_model or "").strip()[:160], str(provider_id or "").strip()[:120]
        except Exception as e:
            logger.debug(f"[session_usage_stats] 从 provider_stats 提取模型信息失败: {e}", exc_info=True)
            return "", ""

    def _extract_event_model_info(self, response: LLMResponse, run_context: Any = None, event: AstrMessageEvent | None = None) -> tuple[str, str]:
        """提取本轮实际成功的模型/Provider。

        优先级：
        1. Provider wrapper 捕获到的最近成功聊天调用（可正确识别 fallback 后的实际 Provider）；
        2. response/raw_completion/run_context 中显式携带的模型信息；
        3. 当前会话默认 Provider（兜底，不再作为首选）。
        """
        model_keys = (
            "model", "model_name", "model_id", "llm_model", "deployment",
            "deployment_name", "engine"
        )
        provider_keys = ("provider", "provider_name", "provider_id", "provider_type", "platform", "type")

        def pick(obj: Any, keys: tuple[str, ...]) -> str:
            queue = [obj]
            seen = set()
            while queue:
                cur = queue.pop(0)
                if cur is None:
                    continue
                ident = id(cur)
                if ident in seen:
                    continue
                seen.add(ident)
                for k in keys:
                    v = self._safe_attr(cur, k)
                    if v and not isinstance(v, (dict, list, tuple, set)):
                        text = str(v).strip()
                        if text:
                            return text
                for child_key in (
                    "raw_completion", "metadata", "meta", "raw", "extra", "response",
                    "provider", "llm", "curr_llm", "using_provider", "context", "request", "req"
                ):
                    child = self._safe_attr(cur, child_key)
                    if child is not None and not isinstance(child, (str, int, float, bool)):
                        queue.append(child)
            return ""

        model_name = ""
        provider_name = ""

        # 0) 最权威：AstrBot provider_stats 中本轮完成回复实际使用的 Provider/模型。
        try:
            ps_model, ps_provider = self._provider_stats_model_info(event)
            if ps_model:
                model_name = ps_model
            if ps_provider:
                provider_name = ps_provider
        except Exception:
            pass

        # 1) fallback 修正：wrapper 捕获到的实际成功 Provider。
        # 若 provider_stats 已经给出结果，则不再让 wrapper 最近调用覆盖它。
        try:
            recent = getattr(self, "_recent_chat_call_context", {}) or {}
            ts = float(recent.get("ts") or 0)
            # on_agent_done 紧跟模型调用完成；30 秒窗口足够覆盖工具链收尾，又避免误用很久前的调用。
            if (not model_name or not provider_name) and ts and datetime.now().timestamp() - ts <= 30:
                if not model_name:
                    model_name = str(recent.get("model_name") or "").strip()
                if not provider_name:
                    provider_name = str(recent.get("provider_name") or "").strip()
        except Exception:
            pass

        # 2) response/run_context 显式字段优先于默认 Provider。
        # raw_completion.model 只作兜底：代理服务可能返回真实上游模型别名，
        # 若直接覆盖会把 AstrBot 配置模型显示成另一个模型。
        raw = getattr(response, "raw_completion", None)
        model_name = model_name or pick(response, model_keys) or pick(run_context, model_keys)
        if not model_name or model_name == "unknown":
            model_name = self._pick_model_from_raw_completion(raw) or model_name
        provider_name = provider_name or pick(response, provider_keys) or pick(run_context, provider_keys)

        if not provider_name:
            try:
                mod = getattr(raw.__class__, "__module__", "") if raw else ""
                if "openai" in mod:
                    provider_name = "openai"
                elif "google" in mod or "genai" in mod:
                    provider_name = "gemini"
                elif "anthropic" in mod:
                    provider_name = "anthropic"
            except Exception:
                pass

        # 3) openai/gemini 这类只是 SDK/协议类型，不是 AstrBot 配置里的 Provider 名。
        # 若拿到了泛称，则尝试用 model_name 反查 AstrBot provider id。
        if model_name and self._is_generic_provider_name(provider_name):
            resolved_provider = self._provider_name_from_config_model(model_name, provider_name)
            if resolved_provider:
                provider_name = resolved_provider

        # 4) 最后才用当前默认 Provider 兜底。它在 fallback 场景下可能是第一个失败模型，不能提前使用。
        if not model_name or self._is_generic_provider_name(provider_name):
            try:
                umo = getattr(event, "unified_msg_origin", None) if event else None
                provider = self.context.get_using_provider(umo=umo)
                if provider:
                    fallback_model, fallback_provider = self._provider_display_name(provider)
                    if not model_name:
                        getter = getattr(provider, "get_model", None)
                        model_name = str((getter() if callable(getter) else fallback_model) or "").strip()
                    if self._is_generic_provider_name(provider_name):
                        provider_name = fallback_provider
            except Exception:
                pass

        if model_name == provider_name and provider_name != "unknown":
            provider_name = "unknown"
        return str(model_name or "unknown")[:160], str(provider_name or "unknown")[:120]

    def _build_bucket_keys(self, created_at: Any) -> dict[str, str]:
        dt = self._normalize_datetime(created_at)
        iso_year, iso_week, _ = dt.isocalendar()
        return {
            "hour": dt.strftime("%Y-%m-%d %H"),
            "day": dt.strftime("%Y-%m-%d"),
            "week": f"{iso_year}-W{iso_week:02d}",
            "month": dt.strftime("%Y-%m"),
        }

    def _rolling_hour_bucket_keys(self, start: datetime, end: datetime, cst=None) -> list[str]:
        """返回覆盖滚动窗口的小时桶 key，用于实时捕获库精确汇总过去24小时/7天/30天。"""
        from zoneinfo import ZoneInfo
        cst = cst or ZoneInfo("Asia/Shanghai")
        cur = start.astimezone(cst).replace(minute=0, second=0, microsecond=0)
        end_hour = end.astimezone(cst).replace(minute=0, second=0, microsecond=0)
        keys: list[str] = []
        while cur <= end_hour:
            keys.append(cur.strftime("%Y-%m-%d %H"))
            cur += timedelta(hours=1)
        return keys

    def _legacy_bucket_keys_for_window(self, bucket_type: str, start: datetime, end: datetime, cst=None) -> list[str]:
        """旧版统计没有 hour 桶时的兼容查询 key。

        day 退回窗口覆盖到的自然日；week/month 退回窗口覆盖到的自然周/月。
        这只是显示兜底，新数据仍应以 hour 桶作为滚动窗口的准确口径。
        """
        from zoneinfo import ZoneInfo
        cst = cst or ZoneInfo("Asia/Shanghai")
        cur = start.astimezone(cst).replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = end.astimezone(cst).replace(hour=0, minute=0, second=0, microsecond=0)
        keys: list[str] = []
        seen = set()
        while cur <= end_day:
            if bucket_type == "month":
                key = cur.strftime("%Y-%m")
            elif bucket_type == "week":
                iso_year, iso_week, _ = cur.isocalendar()
                key = f"{iso_year}-W{iso_week:02d}"
            else:
                key = cur.strftime("%Y-%m-%d")
            if key not in seen:
                seen.add(key)
                keys.append(key)
            cur += timedelta(days=1)
        return keys

    def _normalize_datetime(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if value is None:
            return datetime.now()
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value)
        text = str(value).strip()
        text = text.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text)
        except Exception:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    return datetime.strptime(text, fmt)
                except Exception:
                    pass
        return datetime.now()

    def _extract_token_usage(self, content: dict[str, Any]) -> tuple[int, int, int]:
        agent_stats = content.get("agent_stats") or {}
        token_usage = agent_stats.get("token_usage") or {}
        input_cached = int(token_usage.get("input_cached", 0) or 0)
        input_other = int(token_usage.get("input_other", 0) or 0)
        output = int(token_usage.get("output", 0) or 0)
        total = int(token_usage.get("total", input_cached + input_other + output) or 0)
        return input_cached + input_other, output, total

    @staticmethod
    def _extract_plain_text_from_history_content(content: dict[str, Any]) -> str:
        """从 platform_message_history.content 中抽出纯文本，用于过滤插件自用命令/输出。"""
        parts: list[str] = []
        message = content.get("message") or []
        if isinstance(message, list):
            for seg in message:
                if not isinstance(seg, dict):
                    continue
                if seg.get("type") == "plain":
                    parts.append(str(seg.get("text") or ""))
                elif seg.get("type") == "text":
                    data = seg.get("data") or {}
                    if isinstance(data, dict):
                        parts.append(str(data.get("text") or ""))
        elif isinstance(message, str):
            parts.append(message)
        return "".join(parts).strip()

    @staticmethod
    def _is_stats_command_text(text: str) -> bool:
        """判断是否为本插件自己的统计命令，避免统计行为污染统计数据。"""
        normalized = (text or "").strip()
        while normalized.startswith("/"):
            normalized = normalized[1:].lstrip()
        return normalized.startswith("会话统计")

    @staticmethod
    def _is_stats_result_text(text: str) -> bool:
        """判断是否为本插件自己的统计输出，避免历史扫描把统计页也算作 bot 回复。"""
        normalized = (text or "").strip()
        return normalized.startswith("会话统计·") or normalized.startswith("会话统计模式") or normalized.startswith("补扫完成")

    def _should_skip_history_content(self, content: dict[str, Any]) -> bool:
        text = self._extract_plain_text_from_history_content(content)
        msg_type = content.get("type")
        if msg_type == "user" and self._is_stats_command_text(text):
            return True
        if msg_type == "bot" and self._is_stats_result_text(text):
            return True
        return False

    async def _fetch_new_records(self, last_message_id: int) -> list[Any]:
        import sqlite3 as _sqlite3
        db_path = self._resolve_main_db_path()
        if not db_path:
            # 主历史库只用于补扫历史消息；实时统计已由 on_agent_done 写入。
            # 某些部署不使用 SQLite 主历史库，自动扫描应静默跳过，避免周期性刷 WARN。
            logger.debug("[session_usage_stats] 未找到可用 platform_message_history 主库，本轮历史补扫跳过")
            return []

        conn = _sqlite3.connect(str(db_path), timeout=5)
        try:
            cur = conn.execute(
                "SELECT id, platform_id, user_id, content, created_at "
                "FROM platform_message_history "
                "WHERE id > ? "
                "ORDER BY id ASC LIMIT ?",
                (int(last_message_id), int(self.scan_batch_size)),
            )
            return cur.fetchall()
        finally:
            conn.close()

    async def scan_incremental(self, reason: str = "manual") -> dict[str, int]:
        async with self._scan_lock:
            processed = 0
            touched_sessions: set[str] = set()
            last_message_id = self._get_last_message_id()
            max_seen_id = last_message_id

            while True:
                rows = await self._fetch_new_records(last_message_id=max_seen_id)
                if not rows:
                    break

                for row in rows:
                    row_id, platform_id, session_id, content_raw, created_at = row
                    max_seen_id = max(max_seen_id, int(row_id))
                    if platform_id not in self.enabled_platforms:
                        continue

                    content = content_raw
                    if isinstance(content, str):
                        try:
                            content = json.loads(content)
                        except Exception:
                            continue
                    if not isinstance(content, dict):
                        continue

                    msg_type = content.get("type")
                    if msg_type not in {"user", "bot"}:
                        continue
                    if self._should_skip_history_content(content):
                        continue

                    # 统计口径统一为“完成回复轮数”：
                    # 历史库中 user 记录可能因为连发、改错字、未触发回复等原因多于 bot 记录。
                    # 若逐条累计 user，会让私聊/群聊都出现 user_message_count 远大于 bot_message_count。
                    # 因此只以带 agent_stats 的 bot 记录作为一轮完成对话的锚点，按 bot 记录写入 user+bot+round。
                    if msg_type != "bot":
                        continue

                    input_tokens, output_tokens, total_tokens = self._extract_token_usage(content)
                    bucket_keys = self._build_bucket_keys(created_at)

                    for bucket_type, bucket_key in bucket_keys.items():
                        self._upsert_usage_row(
                            platform_id=platform_id,
                            session_id=session_id,
                            bucket_type=bucket_type,
                            bucket_key=bucket_key,
                            round_inc=1,
                            user_inc=1,
                            bot_inc=1,
                            input_inc=input_tokens,
                            output_inc=output_tokens,
                            total_inc=total_tokens,
                        )

                    processed += 1
                    touched_sessions.add(f"{platform_id}:{session_id}")

                self._set_last_message_id(max_seen_id)

                if len(rows) < self.scan_batch_size:
                    break

                await asyncio.sleep(0)

            logger.info(
                f"[session_usage_stats] 增量扫描完成 reason={reason}, processed={processed}, touched_sessions={len(touched_sessions)}, last_message_id={max_seen_id}"
            )
            return {
                "processed": processed,
                "touched_sessions": len(touched_sessions),
                "last_message_id": max_seen_id,
            }

    def _query_usage(self, platform_id: str, session_id: str, bucket_type: str, bucket_key: str) -> dict[str, int]:
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            row = conn.execute(
                """
                SELECT round_count, user_message_count, bot_message_count,
                       input_tokens, output_tokens, total_tokens
                FROM usage_stats
                WHERE platform_id=? AND session_id=? AND bucket_type=? AND bucket_key=?
                """,
                (platform_id, session_id, bucket_type, bucket_key),
            ).fetchone()
            if not row:
                return {
                    "round_count": 0,
                    "user_message_count": 0,
                    "bot_message_count": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_tokens": 0,
                }
            return {
                "round_count": int(row[0]),
                "user_message_count": int(row[1]),
                "bot_message_count": int(row[2]),
                "input_tokens": int(row[3]),
                "output_tokens": int(row[4]),
                "total_tokens": int(row[5]),
            }
        finally:
            conn.close()

    def _current_bucket_key(self, bucket_type: str) -> str:
        now = datetime.now()
        if bucket_type == "day":
            return now.strftime("%Y-%m-%d")
        if bucket_type == "week":
            iso_year, iso_week, _ = now.isocalendar()
            return f"{iso_year}-W{iso_week:02d}"
        if bucket_type == "month":
            return now.strftime("%Y-%m")
        raise ValueError(f"未知 bucket_type: {bucket_type}")

    def _start_auto_scan_task(self):
        if self._stopping:
            return
        if self._auto_task and not self._auto_task.done():
            return
        self._auto_task = asyncio.create_task(self._auto_scan_loop())

    async def _auto_scan_loop(self):
        while not self._stopping:
            try:
                await self.scan_incremental(reason="auto")
            except asyncio.CancelledError:
                raise
            except sqlite3.OperationalError as e:
                logger.warning(f"[session_usage_stats] 自动扫描跳过: {e}")
            except Exception as e:
                logger.warning(f"[session_usage_stats] 自动扫描失败: {e}", exc_info=True)
            try:
                await asyncio.sleep(max(1, int(self.auto_scan_interval_minutes or 5)) * 60)
            except asyncio.CancelledError:
                raise

    async def _scan_before_query(self):
        if self._stopping:
            return {"processed": 0, "touched_sessions": 0, "last_message_id": self._get_last_message_id()}
        # 2 秒内已有查询触发过扫描则直接复用，避免同一波 4 个并行请求排队空扫。
        now = time.monotonic()
        if now - self._last_query_scan_ts < 2.0:
            return {"processed": 0, "touched_sessions": 0, "last_message_id": self._get_last_message_id()}
        try:
            result = await self.scan_incremental(reason="query")
            self._last_query_scan_ts = time.monotonic()
            return result
        except Exception as e:
            # 重载、清空、主库忙碌或主库路径暂时无法解析时，页面查询不应直接炸成 500；
            # 只跳过本次补扫，并继续使用插件统计库中已有数据渲染面板。
            logger.warning(f"[session_usage_stats] 查询前补扫跳过: {e}", exc_info=True)
            try:
                last_id = self._get_last_message_id()
            except Exception:
                last_id = 0
            return {"processed": 0, "touched_sessions": 0, "last_message_id": last_id}

    def _get_event_platform_name(self, event: AstrMessageEvent) -> str:
        meta = getattr(event, "platform_meta", None) or getattr(event, "_platform_meta", None)
        if meta is not None:
            name = getattr(meta, "name", None)
            if name:
                return str(name)
        getter = getattr(event, "get_platform_name", None)
        if callable(getter):
            try:
                return str(getter())
            except Exception:
                pass
        return ""

    def _is_event_capture_platform(self, event: AstrMessageEvent, platform_id: str) -> bool:
        platform_name = self._get_event_platform_name(event)
        candidates = {str(platform_id), str(platform_name)}
        return any(p in candidates for p in self.event_capture_platforms)

    def _detect_platform_and_session(self, event: AstrMessageEvent) -> tuple[str, str]:
        """尽量稳定地解析平台与会话。

        注意：某些事件适配器里 event.platform_id 可能不是平台名，而是账号/昵称一类的展示值。
        统计维度必须优先使用真实平台名，否则会出现“平台=用户昵称”的脏数据。
        """
        platform_name = self._get_event_platform_name(event)
        raw_platform_id = getattr(event, "platform_id", None)
        if not raw_platform_id:
            getter = getattr(event, "get_platform_id", None)
            if callable(getter):
                try:
                    raw_platform_id = getter()
                except Exception:
                    raw_platform_id = None

        # 优先使用标准平台名；只有拿不到平台名时才退回 raw platform_id。
        platform_id = str(platform_name or raw_platform_id or "unknown")

        raw_session_id = getattr(event, "session_id", None)
        session_id = raw_session_id

        if session_id:
            session_id = str(session_id)
            if "!" in session_id:
                session_id = session_id.split("!")[-1]

        if not session_id:
            session_id = getattr(event, "unified_msg_origin", None)

        if not session_id:
            sender_getter = getattr(event, "get_sender_id", None)
            session_id = sender_getter() if callable(sender_getter) else "unknown"

        # QQ/aiocqhttp 平台兜底：群聊按 group_id 聚合，私聊按 sender_id 聚合。
        if (not session_id or session_id == "unknown") and "aiocqhttp" in platform_id:
            sender_getter = getattr(event, "get_sender_id", None)
            group_getter = getattr(event, "get_group_id", None)
            sender_id = getattr(event, "sender_id", None) or (sender_getter() if callable(sender_getter) else None)
            group_id = getattr(event, "group_id", None) or (group_getter() if callable(group_getter) else None)
            if group_id:
                session_id = f"group:{group_id}"
            elif sender_id:
                session_id = f"user:{sender_id}"

        return str(platform_id), str(session_id)

    @staticmethod
    def _event_is_at_or_wake(event: AstrMessageEvent) -> bool:
        """兼容属性/方法两种形态，避免把方法对象本身误判为 True。"""
        value = getattr(event, "is_at_or_wake_command", False)
        if callable(value):
            try:
                value = value()
            except Exception:
                value = False
        return bool(value)

    def _render_stats_text(self, title: str, platform_id: str, session_id: str, bucket_type: str, bucket_key: str, data: dict[str, int]) -> str:
        return (
            f"{title}\n"
            f"平台：{platform_id}\n"
            f"会话：{session_id}\n"
            f"统计周期：{bucket_key}\n"
            f"对话轮数：{data['round_count']}\n"
            f"用户消息数：{data['user_message_count']}\n"
            f"机器人消息数：{data['bot_message_count']}\n"
            f"输入 Tokens：{data['input_tokens']}\n"
            f"输出 Tokens：{data['output_tokens']}\n"
            f"总 Tokens：{data['total_tokens']}"
        )

    async def _resolve_event_session(self, event: AstrMessageEvent) -> tuple[str, str, datetime] | None:
        """解析 on_agent_done 事件所属会话。

        enabled_platforms 内的平台都允许记录 model_usage_stats。
        webchat 的主历史表 bot 记录通常只有 agent_stats/token_usage，没有模型名，
        因此“对话模型”必须从 on_agent_done 里补；但 webchat 的轮数仍由历史扫描写入，避免双计。
        """
        platform_id, session_id = self._detect_platform_and_session(event)
        if platform_id not in getattr(self, "_effective_platforms", set(self.enabled_platforms) | set(self.event_capture_platforms)):
            return None
        if not session_id or session_id == "unknown":
            return None
        try:
            created_at = event.message_obj.timestamp if event.message_obj and getattr(event.message_obj, "timestamp", None) else datetime.now()
            if isinstance(created_at, (int, float)):
                created_at = datetime.fromtimestamp(created_at)
        except Exception:
            created_at = datetime.now()
        if not isinstance(created_at, datetime):
            created_at = datetime.now()
        return str(platform_id), str(session_id), created_at

    @filter.event_message_type(EventMessageType.PRIVATE_MESSAGE | EventMessageType.GROUP_MESSAGE)
    async def capture_user_message(self, event: AstrMessageEvent):
        # 实时事件捕获不在“收到用户消息”阶段直接入库。
        # 原因：群聊里普通消息/被拦截消息/未完成 LLM 的消息都可能经过这里，
        # 会导致 user_message_count 远大于 bot_message_count。
        # 统一改为在 on_agent_done 阶段按“完成一轮回复”写入 user+bot+round。
        return

    @filter.on_agent_done()
    async def capture_bot_response(
        self,
        event: AstrMessageEvent,
        run_context: "ContextWrapper[AstrAgentContext]",
        response: LLMResponse,
    ):
        if not self.enable_event_capture:
            return
        if not response or getattr(response, "role", None) != "assistant":
            return
        parsed = await self._resolve_event_session(event)
        if not parsed:
            return
        platform_id, session_id, created_at = parsed
        input_tokens, output_tokens = self._extract_event_token_usage(response)
        total_tokens = input_tokens + output_tokens
        model_name, provider_name = self._extract_event_model_info(response, run_context, event)
        async with self._event_lock:
            for bucket_type, bucket_key in self._build_bucket_keys(created_at).items():
                # on_agent_done 是“完成一轮回复”的最可靠锚点：
                # - aiocqhttp 等平台可能不写主历史表，需要这里实时入库；
                # - webchat 在清空后/部分环境下也可能没有及时落入 platform_message_history；
                # - 页面滚动查询会优先使用主历史表，并只在历史表没覆盖的会话上合并 usage_stats，避免展示双计。
                self._upsert_usage_row(
                    platform_id=platform_id,
                    session_id=session_id,
                    bucket_type=bucket_type,
                    bucket_key=bucket_key,
                    round_inc=1,
                    user_inc=1,
                    bot_inc=1,
                    input_inc=input_tokens,
                    output_inc=output_tokens,
                    total_inc=total_tokens,
                )
                # model_usage_stats 必须在 on_agent_done 记录；主历史表缺少模型名/Provider 名。
                self._upsert_model_usage_row(
                    platform_id=platform_id,
                    session_id=session_id,
                    model_name=model_name,
                    provider_name=provider_name,
                    bucket_type=bucket_type,
                    bucket_key=bucket_key,
                    call_inc=1,
                    input_inc=input_tokens,
                    output_inc=output_tokens,
                    total_inc=total_tokens,
                )
        # “全部模型”的聊天调用由 Provider wrapper 记录。
        # on_agent_done 只负责会话轮数和“对话模型”，避免标准对话在全部模型中重复计数。

    @staticmethod
    def _extract_event_token_usage(response: LLMResponse) -> tuple[int, int]:
        """从 LLMResponse.usage 中抽出 (input_tokens, output_tokens)，兼容 dict / TokenUsage / None。"""
        usage = getattr(response, "usage", None)
        input_tokens = 0
        output_tokens = 0
        if usage is None:
            return 0, 0
        if isinstance(usage, TokenUsage):
            try:
                input_tokens = int(getattr(usage, "input", 0) or 0)
            except Exception:
                input_tokens = 0
            try:
                output_tokens = int(getattr(usage, "output", 0) or 0)
            except Exception:
                output_tokens = 0
            return input_tokens, output_tokens
        if isinstance(usage, dict):
            for k in ("input_tokens", "prompt_tokens", "input"):
                if k in usage:
                    try:
                        input_tokens = int(usage.get(k) or 0)
                        break
                    except Exception:
                        pass
            for k in ("output_tokens", "completion_tokens", "output"):
                if k in usage:
                    try:
                        output_tokens = int(usage.get(k) or 0)
                        break
                    except Exception:
                        pass
            return input_tokens, output_tokens
        # 其他类型：尽力抽取
        for k in ("input_tokens", "prompt_tokens"):
            if hasattr(usage, k):
                try:
                    input_tokens = int(getattr(usage, k) or 0)
                    break
                except Exception:
                    pass
        for k in ("output_tokens", "completion_tokens"):
            if hasattr(usage, k):
                try:
                    output_tokens = int(getattr(usage, k) or 0)
                    break
                except Exception:
                    pass
        return input_tokens, output_tokens

    @filter.command("会话统计")
    async def session_usage_stats(self, event: AstrMessageEvent):
        args = event.message_str.strip().split()
        sub = args[1].strip() if len(args) >= 2 else "今日"

        if sub in {"今日", "本周", "本月"}:
            await self._scan_before_query()
            platform_id, session_id = self._detect_platform_and_session(event)
            mapping = {"今日": "day", "本周": "week", "本月": "month"}
            label_mapping = {"今日": "过去 24 小时", "本周": "过去 7 天", "本月": "过去 30 天"}
            bucket_type = mapping[sub]
            rows, window_label, _start, _end = self._query_rolling_usage(bucket_type, platform_id, session_id)
            data = rows[0] if rows else {
                "round_count": 0,
                "user_message_count": 0,
                "bot_message_count": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
            }
            yield event.plain_result(
                self._render_stats_text(
                    title=f"会话统计·{label_mapping[sub]}",
                    platform_id=platform_id,
                    session_id=session_id,
                    bucket_type=bucket_type,
                    bucket_key=window_label,
                    data=data,
                )
            )
            return

        if sub == "模式":
            text = (
                "会话统计模式\n"
                f"自动扫描：" + ("已开启" if self.enable_auto_scan else "已关闭") + "\n"
                f"自动扫描间隔：{self.auto_scan_interval_minutes} 分钟\n"
                f"单次扫描上限：{self.scan_batch_size}\n"
                f"启用平台（扫描）：{', '.join(self.enabled_platforms)}\n"
                f"事件捕获开关：{'开启' if self.enable_event_capture else '关闭'}\n"
                f"事件捕获平台：{', '.join(self.event_capture_platforms)}"
            )
            yield event.plain_result(text)
            return

        if sub in {"补扫", "重扫"}:
            result = await self.scan_incremental(reason="manual_command")
            yield event.plain_result(
                "补扫完成\n"
                f"处理消息数：{result['processed']}\n"
                f"涉及会话数：{result['touched_sessions']}\n"
                f"最新游标：{result['last_message_id']}"
            )
            return

        if sub == "全部":
            yield event.plain_result("权限不足，该指令仅管理员可用")
            return

        yield event.plain_result("用法：会话统计 今日(过去24小时) / 本周(过去7天) / 本月(过去30天) / 模式 / 补扫 / 全部（管理员）")

    @filter.permission_type(PermissionType.ADMIN)
    @filter.command("会话统计诊断")
    async def session_usage_stats_diag(self, event: AstrMessageEvent):
        stats = self._wrap_model_call_providers()
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            total_calls = conn.execute("SELECT COALESCE(SUM(call_count),0) FROM model_call_stats").fetchone()[0] or 0
            model_rows = conn.execute("SELECT COUNT(*) FROM model_call_stats").fetchone()[0] or 0
            recent = conn.execute(
                "SELECT model_name, provider_name, SUM(call_count), SUM(total_tokens) "
                "FROM model_call_stats GROUP BY model_name, provider_name "
                "ORDER BY MAX(updated_at) DESC LIMIT 5"
            ).fetchall()
        finally:
            conn.close()
        lines = [
            "模型用量统计诊断",
            f"Provider 已发现：{stats.get('seen', 0)}",
            f"本次新包装：{stats.get('wrapped', 0)}",
            f"已包装总数：{len(self._wrapped_provider_ids)}",
            f"模型调用记录行：{model_rows}",
            f"累计调用次数：{int(total_calls)}",
        ]
        if recent:
            lines.append("最近模型：")
            for m, p, c, t in recent:
                lines.append(f"- {m} / {p}：{int(c or 0)} 次，{int(t or 0)} Token")
        else:
            lines.append("最近模型：暂无。请在插件重载后实际触发一次模型调用，再刷新面板。")
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(PermissionType.ADMIN)
    @filter.command("会话统计全部")
    async def session_usage_stats_all(self, event: AstrMessageEvent):
        args = event.message_str.strip().split()
        sub = args[1].strip() if len(args) >= 2 else "今日"
        if sub not in {"今日", "本周", "本月"}:
            yield event.plain_result("用法：会话统计全部 今日(过去24小时) / 本周(过去7天) / 本月(过去30天)")
            return

        await self._scan_before_query()
        mapping = {"今日": "day", "本周": "week", "本月": "month"}
        label_mapping = {"今日": "过去 24 小时", "本周": "过去 7 天", "本月": "过去 30 天"}
        bucket_type = mapping[sub]
        data, window_label, _start, _end = self._query_rolling_usage(bucket_type)

        if not data:
            yield event.plain_result(f"会话统计全部·{label_mapping[sub]}\n暂无数据")
            return

        from collections import defaultdict
        groups = defaultdict(list)
        for r in data:
            groups[r["platform_id"]].append(r)

        lines = [f"会话统计全部·{label_mapping[sub]}（{window_label}）"]
        grand_rounds = grand_tokens = 0
        for pid, prows in sorted(groups.items()):
            p_rounds = sum(r["round_count"] for r in prows)
            p_tokens = sum(r["total_tokens"] for r in prows)
            grand_rounds += p_rounds
            grand_tokens += p_tokens
            lines.append(f"\n【{pid}】{len(prows)} 个会话  轮:{p_rounds}  tokens:{p_tokens}")
            for r in prows:
                lines.append(f"  {r['session_id']}  轮:{r['round_count']} 用:{r['user_message_count']} bot:{r['bot_message_count']} tok:{r['total_tokens']}")
        lines.append(f"\n--- 合计  轮:{grand_rounds}  tokens:{grand_tokens}")
        yield event.plain_result("\n".join(lines))
