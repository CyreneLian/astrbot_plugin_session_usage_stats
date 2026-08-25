"""
Quart/Web API 路由处理器
"""
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple
from quart import jsonify, request as qreq
from astrbot.api import logger

class ApiHandler:
    """Quart/Web API 路由处理器"""
    def __init__(self, plugin: Any):
        self.plugin = plugin

    def register_apis(self):
        """注册 API 路由"""
        self.plugin.context.register_web_api(
            "/astrbot_plugin_session_usage_stats/page/stats",
            self._api_stats,
            ["GET"],
            "模型用量数据查询"
        )
        self.plugin.context.register_web_api(
            "/astrbot_plugin_session_usage_stats/page/trend",
            self._api_trend,
            ["GET"],
            "模型用量趋势查询"
        )
        self.plugin.context.register_web_api(
            "/astrbot_plugin_session_usage_stats/page/model_stats",
            self._api_model_stats,
            ["GET"],
            "模型用量占比与模型统计查询"
        )
        self.plugin.context.register_web_api(
            "/astrbot_plugin_session_usage_stats/page/clear",
            self._api_clear,
            ["POST"],
            "清空模型用量数据"
        )
        self.plugin.context.register_web_api(
            "/astrbot_plugin_session_usage_stats/page/notice_status",
            self._api_notice_status,
            ["GET"],
            "升级提示已读状态查询"
        )
        self.plugin.context.register_web_api(
            "/astrbot_plugin_session_usage_stats/page/notice_ack",
            self._api_notice_ack,
            ["POST"],
            "升级提示确认已读"
        )

    async def _api_stats(self):
        bucket_type = qreq.args.get("bucket_type", "day")
        if bucket_type not in ("day", "week", "month", "year", "all"):
            bucket_type = "day"
        try:
            await self.plugin._scan_before_query()
            # 数据库查询放到线程池，防止 I/O 阻塞
            data, window_label, start, end = await self.plugin.db.run_async(
                self.plugin._query_rolling_usage, bucket_type
            )
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
            return jsonify(self.plugin._empty_api_window_payload(bucket_type, "stats", e))

    async def _api_trend(self):
        bucket_type = qreq.args.get("bucket_type", "day")
        if bucket_type not in ("day", "week", "month", "year", "all"):
            bucket_type = "day"
        try:
            await self.plugin._scan_before_query()

            start, end, window_label, cst = self.plugin._rolling_window(bucket_type)
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
            elif bucket_type == "year":
                granularity = "month"
                count = 12
                # 最近12个月
                labels = []
                cur = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                for _ in range(12):
                    labels.append(cur.strftime("%Y-%m"))
                    # 增加一个月
                    year = cur.year + (cur.month // 12)
                    month = (cur.month % 12) + 1
                    cur = cur.replace(year=year, month=month)
                def key_of(dt):
                    m_str = dt.strftime("%Y-%m")
                    return m_str if m_str in labels else None
            elif bucket_type == "all":
                granularity = "month"
                labels = []
                # 从第一笔记录到现在按月生成标签
                cur = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                while cur <= end:
                    labels.append(cur.strftime("%Y-%m"))
                    year = cur.year + (cur.month // 12)
                    month = (cur.month % 12) + 1
                    cur = cur.replace(year=year, month=month)
                def key_of(dt):
                    m_str = dt.strftime("%Y-%m")
                    return m_str if m_str in labels else None
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

            # 线程池中异步查询
            hour_rows = await self.plugin.db.run_async(
                self.plugin._query_stored_hour_rows_for_trend, self.plugin._rolling_hour_bucket_keys(start, end, cst)
            )
            filled_keys: set[tuple[str, str, str]] = set()

            for row in hour_rows:
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
            payload = self.plugin._empty_api_window_payload(bucket_type, "trend", e)
            payload["granularity"] = "hour" if bucket_type == "day" else "day"
            return jsonify(payload)

    async def _api_model_stats(self):
        bucket_type = qreq.args.get("bucket_type", "day")
        scope = qreq.args.get("scope", "chat")
        if bucket_type not in ("day", "week", "month", "year", "all"):
            bucket_type = "day"
        if scope not in ("chat", "all"):
            scope = "chat"
        try:
            try:
                self.plugin._wrap_model_call_providers()
            except Exception as e:
                logger.debug(f"[session_usage_stats] 查询模型统计前补包 Provider 失败: {e}", exc_info=True)
            if scope == "all":
                await self.plugin._flush_model_call_buffer()
            
            rows, window_label, start, end = await self.plugin.db.run_async(
                self.plugin._query_model_usage, bucket_type, scope
            )
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
            return jsonify(self.plugin._empty_api_window_payload(bucket_type, "model_stats", e))

    async def _api_notice_status(self):
        """返回升级提示是否已被确认（供前端决定是否弹窗）"""
        seen = await self.plugin.db.run_async(self.plugin.db.is_notice_seen)
        return {"seen": bool(seen)}

    async def _api_notice_ack(self):
        """标记升级提示为已确认"""
        await self.plugin.db.run_async(self.plugin.db.set_notice_seen)
        return {"ok": True}

    async def _api_clear(self):
        """物理清空插件数据库三张统计表 + VACUUM 瘦身（真正的清空）。"""
        try:
            # ① 物理清空三张统计表（DELETE 全部数据）
            def clear_all():
                conn = sqlite3.connect(self.plugin.db_path, timeout=10)
                try:
                    conn.execute("DELETE FROM usage_stats")
                    conn.execute("DELETE FROM model_usage_stats")
                    conn.execute("DELETE FROM model_call_stats")
                    conn.commit()
                finally:
                    conn.close()
            await self.plugin.db.run_async(clear_all)

            # ② 更新扫描游标与同步游标到主库最新，防止 scanner/同步补扫旧数据重建
            last_message_id = self.plugin._get_main_history_max_id()
            self.plugin.db.set_last_message_id(last_message_id)
            try:
                main_db_path = self.plugin._resolve_main_db_path()
                if main_db_path:
                    mconn = sqlite3.connect(str(main_db_path), timeout=5)
                    try:
                        ps_max = mconn.execute("SELECT COALESCE(MAX(id),0) FROM provider_stats").fetchone()[0] or 0
                    finally:
                        mconn.close()
                    self.plugin.db.set_provider_stats_cursor(int(ps_max))
            except Exception as e:
                logger.warning(f"[session_usage_stats] 更新同步游标失败: {e}", exc_info=True)

            # ③ 设置 clear_at（防御：滚动窗口从当前开始，防止残留回流）
            try:
                from zoneinfo import ZoneInfo
                cst = ZoneInfo("Asia/Shanghai")
                clear_at_iso = datetime.now(cst).isoformat(timespec="seconds")
                self.plugin.db.set_clear_at(clear_at_iso, last_message_id)
            except Exception as e:
                logger.warning(f"[session_usage_stats] 设置清空标记失败: {e}", exc_info=True)

            # ④ VACUUM 瘦身（物理删除后才有意义）
            def run_vacuum():
                conn = sqlite3.connect(self.plugin.db_path, timeout=10)
                try:
                    conn.execute("VACUUM")
                    return 1
                finally:
                    conn.close()
            vacuumed = await self.plugin.db.run_async(run_vacuum)

            return jsonify({"ok": True, "deleted_usage": 0, "deleted_model_usage": 0, "deleted_model_call": 0, "vacuumed": vacuumed})
        except Exception as e:
            return jsonify(self.plugin._api_error_payload("clear", e, last_message_id=None))
