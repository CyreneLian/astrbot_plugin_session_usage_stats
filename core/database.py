"""
模型用量统计 数据库操作层
"""
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
import asyncio
from concurrent.futures import ThreadPoolExecutor
from astrbot.api import logger

class DatabaseManager:
    """数据库管理器，包含线程池异步执行包装，防止 SQLite 阻塞主线程事件循环"""
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="usage_stats_db")
        self._init_sqlite()

    def _execute_sync(self, func, *args, **kwargs):
        """在同步上下文中执行的方法包装"""
        return func(*args, **kwargs)

    async def run_async(self, func, *args, **kwargs):
        """将同步的 SQLite 操作放到线程池异步执行"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self._execute_sync, func, *args, **kwargs)

    def close(self):
        """关闭线程池"""
        self._executor.shutdown(wait=True)

    def _init_sqlite(self):
        """初始化数据库表与索引"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scan_state (
                    state_key TEXT PRIMARY KEY,
                    last_message_id INTEGER NOT NULL DEFAULT 0,
                    last_scan_at TEXT,
                    clear_at TEXT
                )
                """
            )
            # 迁移逻辑
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
            conn.execute("CREATE INDEX IF NOT EXISTS idx_model_usage_bucket ON model_usage_stats(bucket_type, bucket_key, model_name, provider_name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_model_call_bucket ON model_call_stats(bucket_type, bucket_key, model_name, provider_name)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS alert_cooldown (
                    key TEXT PRIMARY KEY,
                    day TEXT NOT NULL,
                    updated_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS session_limits (
                    platform_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    limit_enabled INTEGER,
                    limit_value INTEGER,
                    alert_enabled INTEGER,
                    alert_value INTEGER,
                    updated_at TEXT,
                    PRIMARY KEY (platform_id, session_id)
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    def get_provider_stats_cursor(self) -> int:
        """获取主库 provider_stats 增量同步游标（同步，供外部线程池调用）"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            row = conn.execute(
                "SELECT last_message_id FROM scan_state WHERE state_key = ?",
                ("provider_stats_cursor",),
            ).fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()

    def is_notice_seen(self) -> bool:
        """查询升级提示是否已确认过（服务端存储，不受浏览器沙箱影响）"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            row = conn.execute(
                "SELECT 1 FROM scan_state WHERE state_key = ?",
                ("notice_seen",),
            ).fetchone()
            return row is not None
        finally:
            conn.close()

    def set_notice_seen(self):
        """标记升级提示已确认"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            now = datetime.now().isoformat(timespec="seconds")
            conn.execute(
                """
                INSERT INTO scan_state(state_key, last_message_id, last_scan_at, clear_at)
                VALUES (?, ?, ?, NULL)
                ON CONFLICT(state_key) DO UPDATE SET
                    last_scan_at=excluded.last_scan_at
                """,
                ("notice_seen", 0, now),
            )
            conn.commit()
        finally:
            conn.close()

    def get_alert_sent_state(self) -> dict:
        """获取告警冷却记录（持久化，避免重载时丢失导致重复发送）"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            row = conn.execute(
                "SELECT last_scan_at FROM scan_state WHERE state_key = ?",
                ("alert_sent_state",),
            ).fetchone()
            if row and row[0]:
                import json
                try:
                    return json.loads(row[0])
                except Exception:
                    return {}
            return {}
        finally:
            conn.close()

    def set_alert_sent_state(self, state: dict):
        """保存告警冷却记录（持久化，避免重载时丢失导致重复发送）"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            import json
            now = datetime.now().isoformat(timespec="seconds")
            state_json = json.dumps(state, ensure_ascii=False)
            conn.execute(
                """
                INSERT INTO scan_state(state_key, last_message_id, last_scan_at, clear_at)
                VALUES (?, ?, ?, NULL)
                ON CONFLICT(state_key) DO UPDATE SET
                    last_scan_at=excluded.last_scan_at
                """,
                ("alert_sent_state", 0, state_json),
            )
            conn.commit()
        finally:
            conn.close()

    def get_session_limit(self, platform_id: str, session_id: str):
        """查询单会话的自定义限制/告警配置；无记录返回 None（表示跟随插件默认值）"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            row = conn.execute(
                "SELECT limit_enabled, limit_value, alert_enabled, alert_value "
                "FROM session_limits WHERE platform_id = ? AND session_id = ?",
                (platform_id, session_id),
            ).fetchone()
            if not row:
                return None
            return {
                "limit_enabled": None if row[0] is None else int(row[0]),
                "limit_value": None if row[1] is None else int(row[1]),
                "alert_enabled": None if row[2] is None else int(row[2]),
                "alert_value": None if row[3] is None else int(row[3]),
            }
        finally:
            conn.close()

    def upsert_session_limit(self, platform_id: str, session_id: str,
                             limit_enabled, limit_value, alert_enabled, alert_value):
        """写入/更新单会话配置。字段传 None 表示该项跟随插件默认值；
        四项全 None 时直接删除该行（完全恢复默认）。"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            if limit_enabled is None and limit_value is None and alert_enabled is None and alert_value is None:
                conn.execute(
                    "DELETE FROM session_limits WHERE platform_id = ? AND session_id = ?",
                    (platform_id, session_id),
                )
            else:
                now = datetime.now().isoformat(timespec="seconds")
                conn.execute(
                    """
                    INSERT INTO session_limits(platform_id, session_id, limit_enabled, limit_value, alert_enabled, alert_value, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(platform_id, session_id) DO UPDATE SET
                        limit_enabled=excluded.limit_enabled,
                        limit_value=excluded.limit_value,
                        alert_enabled=excluded.alert_enabled,
                        alert_value=excluded.alert_value,
                        updated_at=excluded.updated_at
                    """,
                    (platform_id, session_id, limit_enabled, limit_value, alert_enabled, alert_value, now),
                )
            conn.commit()
        finally:
            conn.close()

    def list_session_limits(self) -> dict:
        """列出全部单会话自定义配置：{(platform_id, session_id): {...}}"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            rows = conn.execute(
                "SELECT platform_id, session_id, limit_enabled, limit_value, alert_enabled, alert_value "
                "FROM session_limits"
            ).fetchall()
            return {
                (str(r[0]), str(r[1])): {
                    "limit_enabled": None if r[2] is None else int(r[2]),
                    "limit_value": None if r[3] is None else int(r[3]),
                    "alert_enabled": None if r[4] is None else int(r[4]),
                    "alert_value": None if r[5] is None else int(r[5]),
                }
                for r in rows
            }
        finally:
            conn.close()

    def claim_alert_cooldowns(self, keys, day: str) -> list:
        """原子抢占告警冷却：同一 key 同一自然日只有首次抢占成功。

        利用 INSERT ... ON CONFLICT DO UPDATE ... WHERE day != excluded.day：
        插入成功 / 跨天更新成功 → rowcount=1（抢占成功）；
        当日已存在 → WHERE 不命中 → rowcount=0（今天已发过，跳过）。
        多实例 / 多循环并发下也仅一个能抢到，杜绝重复发送。
        """
        if not keys:
            return []
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            now = datetime.now().isoformat(timespec="seconds")
            claimed = []
            for key in keys:
                cur = conn.execute(
                    """
                    INSERT INTO alert_cooldown(key, day, updated_at) VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        day=excluded.day, updated_at=excluded.updated_at
                    WHERE alert_cooldown.day IS NULL OR alert_cooldown.day != excluded.day
                    """,
                    (key, day, now),
                )
                if cur.rowcount == 1:
                    claimed.append(key)
            conn.commit()
            return claimed
        finally:
            conn.close()

    def release_alert_cooldowns(self, keys):
        """发送失败后释放冷却抢占，让下次检查可重试（保留「失败不标记冷却」约定）"""
        if not keys:
            return
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            for key in keys:
                conn.execute("DELETE FROM alert_cooldown WHERE key = ?", (key,))
            conn.commit()
        finally:
            conn.close()

    def list_alert_cooldowns(self) -> dict:
        """查询全部原子冷却抢占记录（诊断用）"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            rows = conn.execute("SELECT key, day, updated_at FROM alert_cooldown").fetchall()
            return {str(r[0]): {"day": str(r[1]), "updated_at": str(r[2])} for r in rows}
        finally:
            conn.close()

    def set_provider_stats_cursor(self, cursor_id: int):
        """设置主库 provider_stats 增量同步游标（同步，供外部线程池调用）"""
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
                ("provider_stats_cursor", int(cursor_id), now),
            )
            conn.commit()
        finally:
            conn.close()

    def get_last_message_id(self) -> int:
        """获取最后一次增量扫描的消息 ID (同步，供外部线程池调用)"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            row = conn.execute(
                "SELECT last_message_id FROM scan_state WHERE state_key = ?",
                ("global",),
            ).fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()

    def set_last_message_id(self, message_id: int):
        """设置增量扫描的消息 ID (同步，供外部线程池调用)"""
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

    def get_clear_at(self) -> Optional[str]:
        """获取清空标记时间"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            row = conn.execute(
                "SELECT clear_at FROM scan_state WHERE state_key = ?",
                ("global",),
            ).fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def set_clear_at(self, clear_at_iso: str, last_message_id: int):
        """设置清空标记时间"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            now = datetime.now().isoformat(timespec="seconds")
            conn.execute(
                """
                INSERT INTO scan_state(state_key, last_message_id, last_scan_at, clear_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(state_key) DO UPDATE SET
                    last_message_id=excluded.last_message_id,
                    last_scan_at=excluded.last_scan_at,
                    clear_at=excluded.clear_at
                """,
                ("global", int(last_message_id), now, clear_at_iso),
            )
            conn.commit()
        finally:
            conn.close()

    def upsert_usage_row(
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

    def deduct_model_call_row(
        self,
        model_name: str,
        provider_name: str,
        bucket_type: str,
        bucket_key: str,
        call_dec: int,
        input_dec: int,
        output_dec: int,
        total_dec: int,
    ):
        """从 model_call_stats 扣减对话调用（数据层扣减，防止 wrapper 抓到的对话调用与主库重复）。

        正常对话调用会同时被 AstrBot 主库 provider_stats 记录（权威），
        同步对话模型时按 (模型, Provider, 桶) 从 model_call_stats 扣减，
        让 model_call_stats 只保留主库未覆盖的纯后台差额（插件直连/嵌入/重排）。
        """
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            conn.execute(
                """
                UPDATE model_call_stats SET
                    call_count = MAX(0, call_count - ?),
                    input_tokens = MAX(0, input_tokens - ?),
                    output_tokens = MAX(0, output_tokens - ?),
                    total_tokens = MAX(0, total_tokens - ?),
                    updated_at = ?
                WHERE platform_id='system' AND session_id='__background__'
                  AND bucket_type=? AND bucket_key=?
                  AND model_name=? AND provider_name=?
                """,
                (
                    int(call_dec), int(input_dec), int(output_dec), int(total_dec),
                    datetime.now().isoformat(timespec="seconds"),
                    bucket_type, bucket_key,
                    model_name, provider_name,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def upsert_model_usage_row(
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

    def upsert_model_call_row(
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

    def query_usage_stats(self, bucket_type: str, bucket_key: str, platform_id: str = None, session_id: str = None) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            query = "SELECT platform_id, session_id, round_count, user_message_count, bot_message_count, input_tokens, output_tokens, total_tokens FROM usage_stats WHERE bucket_type = ? AND bucket_key = ?"
            params = [bucket_type, bucket_key]
            if platform_id:
                query += " AND platform_id = ?"
                params.append(platform_id)
            if session_id:
                query += " AND session_id = ?"
                params.append(session_id)
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()
            return [
                {
                    "platform_id": r[0],
                    "session_id": r[1],
                    "round_count": r[2],
                    "user_message_count": r[3],
                    "bot_message_count": r[4],
                    "input_tokens": r[5],
                    "output_tokens": r[6],
                    "total_tokens": r[7],
                }
                for r in rows
            ]
        finally:
            conn.close()

    def query_model_usage_stats(self, bucket_type: str, bucket_key: str) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            cursor = conn.execute(
                "SELECT platform_id, session_id, model_name, provider_name, call_count, input_tokens, output_tokens, total_tokens FROM model_usage_stats WHERE bucket_type = ? AND bucket_key = ?",
                (bucket_type, bucket_key),
            )
            rows = cursor.fetchall()
            return [
                {
                    "platform_id": r[0],
                    "session_id": r[1],
                    "model_name": r[2],
                    "provider_name": r[3],
                    "call_count": r[4],
                    "input_tokens": r[5],
                    "output_tokens": r[6],
                    "total_tokens": r[7],
                }
                for r in rows
            ]
        finally:
            conn.close()

    def query_model_call_stats(self, bucket_type: str, bucket_key: str) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            cursor = conn.execute(
                "SELECT platform_id, session_id, model_name, provider_name, call_count, input_tokens, output_tokens, total_tokens FROM model_call_stats WHERE bucket_type = ? AND bucket_key = ?",
                (bucket_type, bucket_key),
            )
            rows = cursor.fetchall()
            return [
                {
                    "platform_id": r[0],
                    "session_id": r[1],
                    "model_name": r[2],
                    "provider_name": r[3],
                    "call_count": r[4],
                    "input_tokens": r[5],
                    "output_tokens": r[6],
                    "total_tokens": r[7],
                }
                for r in rows
            ]
        finally:
            conn.close()

    def cleanup_old_data(
        self,
        cutoff_30d_iso: str,
        cutoff_retention_iso: str | None,
    ) -> Dict[str, int]:
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            conn.execute("PRAGMA busy_timeout=10000")

            # 1. 超过 30 天的小时桶固定自动清除（不可关闭）
            deleted_usage_hour = conn.execute(
                "DELETE FROM usage_stats WHERE bucket_type='hour' AND updated_at IS NOT NULL AND updated_at < ?",
                (cutoff_30d_iso,),
            ).rowcount
            deleted_model_usage_hour = conn.execute(
                "DELETE FROM model_usage_stats WHERE bucket_type='hour' AND updated_at IS NOT NULL AND updated_at < ?",
                (cutoff_30d_iso,),
            ).rowcount
            deleted_model_call_hour = conn.execute(
                "DELETE FROM model_call_stats WHERE bucket_type='hour' AND updated_at IS NOT NULL AND updated_at < ?",
                (cutoff_30d_iso,),
            ).rowcount

            # 2. 超过保留天数（默认 365 天）的数据完全清除（全量桶）
            deleted_usage_full = 0
            deleted_model_usage_full = 0
            deleted_model_call_full = 0
            if cutoff_retention_iso:
                deleted_usage_full = conn.execute(
                    "DELETE FROM usage_stats WHERE updated_at IS NOT NULL AND updated_at < ?",
                    (cutoff_retention_iso,),
                ).rowcount
                deleted_model_usage_full = conn.execute(
                    "DELETE FROM model_usage_stats WHERE updated_at IS NOT NULL AND updated_at < ?",
                    (cutoff_retention_iso,),
                ).rowcount
                deleted_model_call_full = conn.execute(
                    "DELETE FROM model_call_stats WHERE updated_at IS NOT NULL AND updated_at < ?",
                    (cutoff_retention_iso,),
                ).rowcount

            conn.commit()
            return {
                "deleted_usage": int((deleted_usage_hour or 0) + (deleted_usage_full or 0)),
                "deleted_model_usage": int((deleted_model_usage_hour or 0) + (deleted_model_usage_full or 0)),
                "deleted_model_call": int((deleted_model_call_hour or 0) + (deleted_model_call_full or 0)),
                "vacuumed": 0,
            }
        finally:
            conn.close()
