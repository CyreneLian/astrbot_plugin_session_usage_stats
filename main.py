"""
AstrBot 模型用量统计插件 v2.01

功能描述：
- 统计全部模型的调用次数、Token 消耗和趋势排行
- 提供美观的可视化模型用量统计仪表盘、趋势排行、各会话对话轮数等
- 支持低开销增量扫描与自动清理

作者: 往昔的涟漪
版本: 2.01
日期: 2026-06-26
"""

import asyncio
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

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

# 本地模块导入
from .models.config import SessionUsageStatsConfig
from .core.database import DatabaseManager
from .core.scanner import MessageScanner
from .core.api import ApiHandler

@register(
    "astrbot_plugin_session_usage_stats",
    "OpenAI",
    "统计全部模型的调用次数、Token 消耗和趋势排行",
    "2.01",
    "",
)
class SessionUsageStatsPlugin(Star):
    """模型用量统计插件主类"""
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        # 1. 初始化配置模型
        self.plugin_config = SessionUsageStatsConfig.from_dict(config or {})
        try:
            self.plugin_config.validate()
        except ValueError as e:
            logger.error(f"[session_usage_stats] 配置校验失败: {e}")

        # 保持对旧属性的兼容，以便 scanner 等逻辑直接读取
        self.enable_auto_scan = self.plugin_config.enable_auto_scan
        self.auto_scan_interval_minutes = self.plugin_config.auto_scan_interval_minutes
        self.scan_batch_size = self.plugin_config.scan_batch_size
        self.enabled_platforms = self.plugin_config.enabled_platforms
        self.include_threads = self.plugin_config.include_threads
        self.enable_event_capture = self.plugin_config.enable_event_capture
        self.event_capture_platforms = self.plugin_config.event_capture_platforms
        self.auto_cleanup_enabled = self.plugin_config.auto_cleanup_enabled
        self.auto_cleanup_retention_days = self.plugin_config.auto_cleanup_retention_days
        self.auto_cleanup_vacuum = self.plugin_config.auto_cleanup_vacuum

        self._event_lock = asyncio.Lock()
        self._model_call_lock = asyncio.Lock()
        self._model_call_buffer: Dict[Tuple[str, str, str, str, str, str], List[int]] = {}
        self._model_call_flush_task: Optional[asyncio.Task] = None
        self._wrapped_provider_ids: set[int] = set()
        self._provider_call_context = {}
        self._last_cleanup_at = 0.0
        self._recent_chat_call_context: Dict[str, Any] = {}

        if self.include_threads and "webchat_thread" not in self.enabled_platforms:
            self.enabled_platforms.append("webchat_thread")
        self._effective_platforms = set(map(str, self.enabled_platforms)) | set(map(str, self.event_capture_platforms))

        self._scan_lock = asyncio.Lock()
        self._last_query_scan_ts = 0.0
        self._stopping = False

        # 初始化数据目录与数据库管理器
        try:
            self.data_dir = Path(StarTools.get_data_dir())
        except Exception:
            self.data_dir = Path(get_astrbot_data_path()) / "plugin_data" / "astrbot_plugin_session_usage_stats"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "usage_stats.db"

        # 2. 初始化服务层
        self.db = DatabaseManager(self.db_path)
        self.scanner = MessageScanner(self.db, self.plugin_config, self)
        self.api_handler = ApiHandler(self)

    async def initialize(self):
        """插件激活初始化"""
        if self.auto_cleanup_enabled:
            await self._cleanup_old_data(reason="startup")
        
        # 启动自动扫描服务
        self.scanner.start()
        
        self._wrap_model_call_providers()
        self._model_call_flush_task = asyncio.create_task(self._model_call_flush_loop())
        
        # 注册 API 路由
        self.api_handler.register_apis()

    async def terminate(self):
        """插件卸载资源清理"""
        self._stopping = True
        
        # 停止自动扫描服务
        await self.scanner.terminate()

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
        
        # 关闭数据库管理器（释放线程池）
        self.db.close()

    def _register_page_apis(self):
        """兼容旧版路由注册占位"""
        pass

    def _api_stats(self): pass
    def _api_trend(self): pass
    def _api_model_stats(self): pass
    def _api_clear(self): pass

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
                    cnt = conn.execute("SELECT COUNT(*) FROM platform_message_history").fetchone()[0]
                    return int(cnt or 0)
                finally:
                    conn.close()
            except Exception:
                return -1

        best_path = ""
        best_score = -1
        try:
            cfg = self.context.astrbot_config
            if cfg:
                db_url = cfg.get_plat_config().get("database", {}).get("url")
                if db_url:
                    path = _normalize_sqlite_path(db_url)
                    score = _history_db_score(path)
                    if score >= 0:
                        return path
        except Exception:
            pass

        parent = Path(get_astrbot_data_path())
        candidates = [
            parent / "data_v4.db",
            parent / "data.db",
            parent / "data" / "data_v4.db",
            parent / "data" / "data.db",
        ]
        for c in candidates:
            path = str(c.resolve())
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
        try:
            from zoneinfo import ZoneInfo
            cst = cst or ZoneInfo("Asia/Shanghai")
            # 通过 db 实例读取
            text = self.db.get_clear_at()
            if not text:
                return None
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

        for row in self._query_stored_bucket_rows(bucket_type, platform_id, session_id, hour_keys):
            key = (str(row["platform_id"]), str(row["session_id"]))
            grouped[key] = row

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

    def _query_model_usage(self, bucket_type: str, scope: str = "chat"):
        start, end, window_label, cst = self._rolling_window(bucket_type)
        hour_keys = self._rolling_hour_bucket_keys(start, end, cst)
        placeholders = ",".join("?" for _ in hour_keys) or "?"
        params = [*hour_keys] if hour_keys else ["__none__"]
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            if scope == "all":
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

                for r in background_token_rows:
                    m, p = str(r[0] or "unknown"), str(r[1] or "unknown")
                    key = (m, p)
                    c_call, c_in, c_out, c_tot = int(r[2] or 0), int(r[3] or 0), int(r[4] or 0), int(r[5] or 0)
                    if key in covered:
                        base_vals = covered[key]
                        c_call = max(0, c_call - base_vals[0])
                        c_in = max(0, c_in - base_vals[1])
                        c_out = max(0, c_out - base_vals[2])
                        c_tot = max(0, c_tot - base_vals[3])
                    if c_tot > 0 or c_call > 0:
                        base_rows.append((m, p, c_call, c_in, c_out, c_tot))

                for r in background_zero_rows:
                    m, p = str(r[0] or "unknown"), str(r[1] or "unknown")
                    c_call = int(r[2] or 0)
                    base_rows.append((m, p, c_call, 0, 0, 0))

                rows = base_rows
            else:
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

    async def _cleanup_old_data(self, reason: str = "manual") -> dict[str, int]:
        if not self.auto_cleanup_enabled:
            return {"deleted_usage": 0, "deleted_model_usage": 0, "deleted_model_call": 0, "vacuumed": 0}
        now_ts = time.monotonic()
        if reason != "manual" and now_ts - self._last_cleanup_at < 3600:
            return {"deleted_usage": 0, "deleted_model_usage": 0, "deleted_model_call": 0, "vacuumed": 0}
        
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(self.auto_cleanup_retention_days or 30)))
        cutoff_iso = cutoff.isoformat(timespec="seconds")
        
        # 通过 db 实例异步清理
        result = await self.db.run_async(
            self.db.cleanup_old_data, cutoff_iso, self.auto_cleanup_vacuum
        )
        self._last_cleanup_at = now_ts
        logger.info(
            f"[session_usage_stats] 自动清理完成 reason={reason}, cutoff={cutoff_iso}, "
            f"usage={result['deleted_usage']}, model_usage={result['deleted_model_usage']}, model_call={result['deleted_model_call']}, vacuum={result['vacuumed']}"
        )
        return result

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
        platform_name = self._get_event_platform_name(event)
        raw_platform_id = getattr(event, "platform_id", None)
        if not raw_platform_id:
            getter = getattr(event, "get_platform_id", None)
            if callable(getter):
                try:
                    raw_platform_id = getter()
                except Exception:
                    raw_platform_id = None
        platform_id = str(platform_name or raw_platform_id or "unknown")
        raw_session_id = getattr(event, "session_id", None)
        session_id = raw_session_id
        if session_id:
            session_id = str(session_id)
            if "!" in session_id:
                session_id = session_id.split("!")[-1]
        if not session_id:
            session_id = getattr(event, "unified_msg_origin", None)
        return platform_id, session_id

    def _event_is_at_or_wake(self, event: AstrMessageEvent) -> bool:
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
        platform_id, session_id = self._detect_platform_and_session(event)
        if platform_id not in getattr(self, "_effective_platforms", set(self.enabled_platforms) | set(self.event_capture_platforms)):
            return None
        if not session_id or session_id == "unknown":
            return None
        try:
            created_at = event.message_obj.timestamp if event.message_obj and getattr(event.message_obj, "timestamp", None) else datetime.now().timestamp()
            if isinstance(created_at, (int, float)):
                created_at = datetime.fromtimestamp(created_at, timezone.utc)
        except Exception:
            created_at = datetime.now(timezone.utc)
        if not isinstance(created_at, datetime):
            created_at = datetime.now(timezone.utc)
        return str(platform_id), str(session_id), created_at

    @filter.event_message_type(EventMessageType.PRIVATE_MESSAGE | EventMessageType.GROUP_MESSAGE)
    async def capture_user_message(self, event: AstrMessageEvent):
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
            # 批量异步写入，防止阻塞
            def run_upsert():
                for bucket_type, bucket_key in self._build_bucket_keys(created_at).items():
                    self.db.upsert_usage_row(
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
                    self.db.upsert_model_usage_row(
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
            await self.db.run_async(run_upsert)

    def _safe_attr(self, obj: Any, name: str) -> Any:
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj.get(name)
        try:
            return getattr(obj, name, None)
        except Exception:
            return None

    def _provider_stats_model_info(self, event: AstrMessageEvent | None = None, max_age_seconds: int = 600) -> tuple[str, str]:
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

        try:
            ps_model, ps_provider = self._provider_stats_model_info(event)
            if ps_model:
                model_name = ps_model
            if ps_provider:
                provider_name = ps_provider
        except Exception:
            pass

        try:
            recent = getattr(self, "_recent_chat_call_context", {}) or {}
            ts = float(recent.get("ts") or 0)
            if (not model_name or not provider_name) and ts and datetime.now().timestamp() - ts <= 30:
                if not model_name:
                    model_name = str(recent.get("model_name") or "").strip()
                if not provider_name:
                    provider_name = str(recent.get("provider_name") or "").strip()
        except Exception:
            pass

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

        if model_name and self._is_generic_provider_name(provider_name):
            resolved_provider = self._provider_name_from_config_model(model_name, provider_name)
            if resolved_provider:
                provider_name = resolved_provider

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

    def _provider_display_name(self, provider: Any) -> tuple[str, str]:
        pname = "unknown"
        mname = "unknown"
        try:
            pname = str(getattr(provider, "provider_name", None) or getattr(provider, "provider_id", None) or "unknown")
            mname = str(getattr(provider, "model", None) or getattr(provider, "model_name", None) or "unknown")
            if pname == "unknown":
                cfg = getattr(provider, "provider_config", {}) or {}
                if isinstance(cfg, dict):
                    pname = str(cfg.get("id") or cfg.get("provider_name") or "unknown")
        except Exception:
            pass
        return mname, pname

    def _remember_successful_chat_provider(self, provider: Any, resp: Any = None, args: Any = None, kwargs: dict | None = None):
        try:
            mname, pname = self._provider_display_name(provider)
            kwargs = kwargs or {}
            model_val = str(kwargs.get("model") or mname or "unknown").strip()
            prov_val = str(pname or "unknown").strip()
            in_tokens = 0
            out_tokens = 0
            if resp:
                in_tokens, out_tokens = self._extract_llm_response_tokens(resp)
            self._recent_chat_call_context = {
                "ts": datetime.now().timestamp(),
                "model_name": model_val,
                "provider_name": prov_val,
                "input_tokens": in_tokens,
                "output_tokens": out_tokens,
            }
        except Exception as e:
            logger.debug(f"[session_usage_stats] 记录最近成功 Provider 失败: {e}", exc_info=True)

    def _pick_model_from_raw_completion(self, raw: Any) -> str:
        if not raw:
            return ""
        try:
            if isinstance(raw, dict):
                return str(raw.get("model") or "")
            return str(getattr(raw, "model", None) or "")
        except Exception:
            return ""

    def _is_generic_provider_name(self, name: str) -> bool:
        return str(name).strip().lower() in ("openai", "gemini", "anthropic", "google", "unknown", "")

    def _normalize_provider_display_name(self, provider_name: str, model_name: str = "") -> str:
        name = str(provider_name).strip()
        if self._is_generic_provider_name(name) and model_name:
            resolved = self._provider_name_from_config_model(model_name, name)
            if resolved:
                name = resolved
        
        # 移除提供商名称中重复的后缀（例如：Gemini/gemini-3.1-pro-preview -> Gemini）
        if "/" in name:
            parts = name.split("/")
            if parts[0].lower() in model_name.lower() or model_name.lower() in parts[1].lower():
                 name = parts[0].strip()
            else:
                 name = parts[0].strip()

        if name.lower() == "openai":
            name = "OpenAI"
        elif name.lower() == "gemini":
            name = "Gemini"
        return name

    def _provider_name_from_config_model(self, model_name: str, current_provider_name: str = "") -> str:
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
            if len(candidates) == 1:
                return candidates[0]
            if current_provider_name and current_provider_name in candidates:
                return current_provider_name
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
        
        # 异步批量写入 model_call_stats
        def do_flush():
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
            finally:
                conn.close()

        try:
            await self.db.run_async(do_flush)
        except Exception:
            async with self._model_call_lock:
                for key, vals in items:
                    cur = self._model_call_buffer.setdefault(key, [0, 0, 0, 0])
                    cur[0] += vals[0]
                    cur[1] += vals[1]
                    cur[2] += vals[2]
                    cur[3] += vals[3]
            raise

    @staticmethod
    def _extract_llm_response_tokens(resp: Any) -> tuple[int, int]:
        if resp is None:
            return 0, 0
        try:
            return SessionUsageStatsPlugin._extract_event_token_usage(resp)
        except Exception:
            return 0, 0

    def _wrap_model_call_providers(self) -> dict[str, int]:
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
            self._remember_successful_chat_provider(provider, resp, args, kwargs)
            recent = getattr(self, "_recent_chat_call_context", {}) or {}
            call_model = str(recent.get("model_name") or kwargs.get("model") or model_name or "unknown")
            call_provider = str(recent.get("provider_name") or provider_name or "unknown")
            in_tok = int(recent.get("input_tokens") or 0)
            out_tok = int(recent.get("output_tokens") or 0)
            self._record_model_call_now(call_model, call_provider, in_tok, out_tok)

        def record_plain_call(_resp, args, kwargs):
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

        wrap_async_method("text_chat", record_chat)
        wrap_stream_method("text_chat_stream")
        wrap_async_method("get_embedding", record_plain_call)
        wrap_async_method("get_embeddings", record_plain_call)
        wrap_async_method("get_embeddings_batch", record_plain_call)
        wrap_async_method("rerank", record_plain_call)

    def _init_sqlite(self):
        """主入口不再自己初始化 SQLite，直接交给 db 模块"""
        pass

    async def _model_call_flush_loop(self):
        await asyncio.sleep(5)
        while not self._stopping:
            try:
                await self._flush_model_call_buffer()
            except Exception as e:
                logger.error(f"[session_usage_stats] 自动 flush 模型调用统计失败: {e}", exc_info=True)
            for _ in range(30):
                if self._stopping:
                    break
                await asyncio.sleep(1)

    def _build_bucket_keys(self, created_at: Any) -> dict[str, str]:
        dt = self._normalize_datetime(created_at)
        from zoneinfo import ZoneInfo
        cst = ZoneInfo("Asia/Shanghai")
        dt = dt.astimezone(cst)
        iso_year, iso_week, _ = dt.isocalendar()
        return {
            "hour": dt.strftime("%Y-%m-%d %H"),
            "day": dt.strftime("%Y-%m-%d"),
            "week": f"{iso_year}-W{iso_week:02d}",
            "month": dt.strftime("%Y-%m-%d")[:7],
        }

    def _rolling_hour_bucket_keys(self, start: datetime, end: datetime, cst=None) -> list[str]:
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
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        if value is None:
            return datetime.now(timezone.utc)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, timezone.utc)
        text = str(value).strip()
        text = text.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(text, fmt)
                    return dt.replace(tzinfo=timezone.utc)
                except Exception:
                    pass
        return datetime.now(timezone.utc)

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
        normalized = (text or "").strip()
        while normalized.startswith("/"):
            normalized = normalized[1:].lstrip()
        return normalized.startswith("会话统计")

    @staticmethod
    def _is_stats_result_text(text: str) -> bool:
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

    async def scan_incremental(self, reason: str = "manual") -> dict[str, int]:
        """将增量扫描委托给 scanner 服务"""
        return await self.scanner.scan_incremental(reason)

    async def _scan_before_query(self):
        now_ts = time.monotonic()
        if now_ts - self._last_query_scan_ts < 2:
            return
        self._last_query_scan_ts = now_ts
        try:
            await self.scan_incremental(reason="before_query")
        except Exception as e:
            logger.warning(f"[session_usage_stats] 查询前补扫跳过: {e}", exc_info=True)

    @staticmethod
    def _extract_event_token_usage(response: LLMResponse) -> tuple[int, int]:
        input_tokens = 0
        output_tokens = 0
        try:
            usage = getattr(response, "token_usage", None)
            if usage:
                input_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
                output_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
            else:
                raw = getattr(response, "raw_completion", None)
                if isinstance(raw, dict):
                    usage_dict = raw.get("usage") or {}
                    input_tokens = int(usage_dict.get("prompt_tokens", 0) or 0)
                    output_tokens = int(usage_dict.get("completion_tokens", 0) or 0)
                elif raw:
                    usage_obj = getattr(raw, "usage", None)
                    if usage_obj:
                        input_tokens = int(getattr(usage_obj, "prompt_tokens", 0) or 0)
                        output_tokens = int(getattr(usage_obj, "completion_tokens", 0) or 0)
        except Exception:
            pass
        return input_tokens, output_tokens

    @filter.permission_type(PermissionType.ADMIN)
    @filter.command("会话统计模式")
    async def session_usage_stats_mode(self, event: AstrMessageEvent):
        mode_text = "实时事件捕获" if self.enable_event_capture else "定时增量扫描"
        lines = [
            "会话统计当前模式",
            f"运行模式：{mode_text}",
            f"自动清理：{'开启' if self.auto_cleanup_enabled else '关闭'}，保留天数：{self.auto_cleanup_retention_days} 天",
            f"自动扫描：{'开启' if self.enable_auto_scan else '关闭'}，周期：{self.auto_scan_interval_minutes} 分钟",
            f"有效统计平台：{', '.join(self.enabled_platforms)}",
            f"事件捕获平台：{', '.join(self.event_capture_platforms)}",
        ]
        yield event.plain_result("\n".join(lines))

    @filter.command("会话统计")
    async def session_usage_stats(self, event: AstrMessageEvent):
        args = event.message_str.strip().split()
        sub = args[1].strip() if len(args) >= 2 else "今日"
        if sub in {"今日", "本周", "本月"}:
            await self._scan_before_query()
            mapping = {"今日": "day", "本周": "week", "本月": "month"}
            label_mapping = {"今日": "过去 24 小时", "本周": "过去 7 天", "本月": "过去 30 天"}
            bucket_type = mapping[sub]
            platform_id, session_id = self._detect_platform_and_session(event)
            if not session_id or session_id == "unknown":
                yield event.plain_result("无法获取当前会话标识")
                return
            
            # 使用线程池执行滚动查询
            data, window_label, _start, _end = await self.db.run_async(
                self._query_rolling_usage, bucket_type, platform_id, session_id
            )
            
            if not data:
                yield event.plain_result(
                    f"会话统计·{label_mapping[sub]}\n"
                    f"平台：{platform_id}\n"
                    f"会话：{session_id}\n"
                    f"统计周期：{window_label}\n"
                    "暂无数据，请稍后刷新"
                )
                return
            yield event.plain_result(
                self._render_stats_text(
                    f"会话统计·{label_mapping[sub]}",
                    platform_id,
                    session_id,
                    bucket_type,
                    window_label,
                    data[0],
                )
            )
            return

        if sub == "模式":
            async for res in self.session_usage_stats_mode(event):
                yield res
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
        
        def run_diag():
            conn = sqlite3.connect(self.db_path, timeout=10)
            try:
                total_calls = conn.execute("SELECT COALESCE(SUM(call_count),0) FROM model_call_stats").fetchone()[0] or 0
                model_rows = conn.execute("SELECT COUNT(*) FROM model_call_stats").fetchone()[0] or 0
                recent = conn.execute(
                    "SELECT model_name, provider_name, SUM(call_count), SUM(total_tokens) "
                    "FROM model_call_stats GROUP BY model_name, provider_name "
                    "ORDER BY MAX(updated_at) DESC LIMIT 5"
                ).fetchall()
                return total_calls, model_rows, recent
            finally:
                conn.close()

        total_calls, model_rows, recent = await self.db.run_async(run_diag)
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
        
        data, window_label, _start, _end = await self.db.run_async(
            self._query_rolling_usage, bucket_type
        )

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
