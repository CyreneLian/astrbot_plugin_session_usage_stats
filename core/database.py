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

    def cleanup_old_data(self, cutoff_iso: str, auto_cleanup_vacuum: bool) -> Dict[str, int]:
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            conn.execute("PRAGMA busy_timeout=10000")
            deleted_usage = conn.execute(
                "DELETE FROM usage_stats WHERE updated_at IS NOT NULL AND updated_at < ?",
                (cutoff_iso,),
            ).rowcount
            deleted_model_usage = conn.execute(
                "DELETE FROM model_usage_stats WHERE updated_at IS NOT NULL AND updated_at < ?",
                (cutoff_iso,),
            ).rowcount
            deleted_model_call = conn.execute(
                "DELETE FROM model_call_stats WHERE updated_at IS NOT NULL AND updated_at < ?",
                (cutoff_iso,),
            ).rowcount
            conn.commit()
            vacuumed = 0
            if auto_cleanup_vacuum:
                try:
                    conn.execute("VACUUM")
                    vacuumed = 1
                except Exception as e:
                    logger.warning(f"[session_usage_stats] VACUUM 失败: {e}", exc_info=True)
            return {
                "deleted_usage": int(deleted_usage or 0),
                "deleted_model_usage": int(deleted_model_usage or 0),
                "deleted_model_call": int(deleted_model_call or 0),
                "vacuumed": int(vacuumed),
            }
        finally:
            conn.close()
