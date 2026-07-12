"""
历史消息扫描与自动扫描 Service
"""
import json
import sqlite3
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple, Optional
from astrbot.api import logger
from .database import DatabaseManager

class MessageScanner:
    """历史消息增量扫描与自动扫描逻辑"""
    def __init__(self, db: DatabaseManager, config: Any, plugin: Any):
        self.db = db
        self.config = config
        self.plugin = plugin
        self._scan_lock = asyncio.Lock()
        self._auto_task: Optional[asyncio.Task] = None
        self._stopping = False

    def start(self):
        """启动自动扫描任务"""
        if self.config.enable_auto_scan:
            self._stopping = False
            self._auto_task = asyncio.create_task(self._auto_scan_loop())

    async def terminate(self):
        """卸载时安全取消任务"""
        self._stopping = True
        if self._auto_task:
            self._auto_task.cancel()
            try:
                await self._auto_task
            except asyncio.CancelledError:
                pass

    def _should_skip_history_content(self, content: Dict[str, Any]) -> bool:
        text = self.plugin._extract_plain_text_from_history_content(content)
        msg_type = content.get("type")
        if msg_type == "user" and self.plugin._is_stats_command_text(text):
            return True
        if msg_type == "bot" and self.plugin._is_stats_result_text(text):
            return True
        return False

    async def _fetch_new_records(self, last_message_id: int) -> List[Any]:
        db_path = self.plugin._resolve_main_db_path()
        if not db_path:
            logger.debug("[session_usage_stats] 未找到可用 platform_message_history 主库，本轮历史补扫跳过")
            return []

        def sync_fetch():
            conn = sqlite3.connect(str(db_path), timeout=5)
            try:
                cur = conn.execute(
                    "SELECT id, platform_id, user_id, content, created_at "
                    "FROM platform_message_history "
                    "WHERE id > ? "
                    "ORDER BY id ASC LIMIT ?",
                    (int(last_message_id), int(self.config.scan_batch_size)),
                )
                return cur.fetchall()
            finally:
                conn.close()

        return await self.db.run_async(sync_fetch)

    async def scan_incremental(self, reason: str = "manual") -> Dict[str, int]:
        async with self._scan_lock:
            processed = 0
            touched_sessions: Set[str] = set()
            last_message_id = await self.db.run_async(self.db.get_last_message_id)
            max_seen_id = last_message_id

            while True:
                rows = await self._fetch_new_records(last_message_id=max_seen_id)
                if not rows:
                    break

                # 提取数据准备批量插入或逐条 upsert
                # 因为我们要保证 ThreadPoolExecutor 异步，这里把 upsert 包装为批处理函数
                def process_rows_batch(batch_rows):
                    conn = sqlite3.connect(self.db.db_path, timeout=10)
                    now = datetime.now().isoformat(timespec="seconds")
                    local_processed = 0
                    local_touched = set()
                    try:
                        for row in batch_rows:
                            row_id, platform_id, session_id, content_raw, created_at = row
                            if platform_id not in self.config.enabled_platforms:
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

                            if msg_type != "bot":
                                continue

                            # 过滤掉没有 agent_stats 的中间/工具调用消息，避免重复计算轮数和 Token
                            if "agent_stats" not in content:
                                continue

                            input_tokens, output_tokens, total_tokens = self.plugin._extract_token_usage(content)
                            bucket_keys = self.plugin._build_bucket_keys(created_at)

                            for bucket_type, bucket_key in bucket_keys.items():
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
                                        1, 1, 1,
                                        input_tokens,
                                        output_tokens,
                                        total_tokens,
                                        now,
                                    ),
                                )
                            local_processed += 1
                            local_touched.add(f"{platform_id}:{session_id}")
                        conn.commit()
                    finally:
                        conn.close()
                    return local_processed, local_touched

                local_proc, local_touch = await self.db.run_async(process_rows_batch, rows)
                processed += local_proc
                touched_sessions.update(local_touch)

                for row in rows:
                    max_seen_id = max(max_seen_id, int(row[0]))

                await self.db.run_async(self.db.set_last_message_id, max_seen_id)

                if len(rows) < self.config.scan_batch_size:
                    break

                await asyncio.sleep(0)

            return {
                "processed": processed,
                "touched_sessions": len(touched_sessions),
                "last_message_id": max_seen_id,
            }

    async def _auto_scan_loop(self):
        await asyncio.sleep(10)  # 启动后延迟 10 秒首次扫描
        while not self._stopping:
            try:
                await self.scan_incremental(reason="auto_scan")
            except Exception as e:
                logger.error(f"[session_usage_stats] 自动扫描异常: {e}", exc_info=True)
            interval = max(1, self.config.auto_scan_interval_minutes)
            for _ in range(interval * 60):
                if self._stopping:
                    break
                await asyncio.sleep(1)
