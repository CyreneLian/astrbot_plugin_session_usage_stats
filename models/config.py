"""
插件配置模型模块
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any

@dataclass
class SessionUsageStatsConfig:
    """插件配置模型"""
    enable_auto_scan: bool = True
    auto_scan_interval_minutes: int = 5
    scan_batch_size: int = 500
    enabled_platforms: List[str] = field(default_factory=lambda: ["webchat"])
    include_threads: bool = False
    enable_event_capture: bool = True
    event_capture_platforms: List[str] = field(default_factory=lambda: ["aiocqhttp"])
    auto_cleanup_enabled: bool = True
    auto_cleanup_retention_days: int = 365
    alert_enabled: bool = False
    alert_daily_token_threshold: int = 0
    alert_session_token_threshold: int = 0
    alert_target_id: List[str] = field(default_factory=list)      # 告警目标：QQ 号直接填（私聊），群号加 g: 前缀（群聊）
    alert_check_interval_minutes: int = 30

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "SessionUsageStatsConfig":
        """从字典创建配置实例"""
        return cls(
            enable_auto_scan=True,  # 强制开启，保证统计准确性
            auto_scan_interval_minutes=cls._safe_int(config.get("auto_scan_interval_minutes", 5), 5, minimum=1),
            scan_batch_size=cls._safe_int(config.get("scan_batch_size", 500), 500, minimum=1),
            enabled_platforms=cls._parse_str_list(config.get("enabled_platforms", ["webchat"]) or ["webchat"]),
            include_threads=bool(config.get("include_threads", False)),
            enable_event_capture=True,  # 强制开启，保证统计准确性
            event_capture_platforms=cls._parse_str_list(config.get("event_capture_platforms", ["aiocqhttp"]) or ["aiocqhttp"]),
            auto_cleanup_enabled=bool(config.get("auto_cleanup_enabled", True)),
            auto_cleanup_retention_days=cls._safe_int(config.get("auto_cleanup_retention_days", 365), 365, minimum=1),
            alert_enabled=bool(config.get("alert_enabled", False)),
            alert_daily_token_threshold=cls._safe_int(config.get("alert_daily_token_threshold", 0), 0),
            alert_session_token_threshold=cls._safe_int(config.get("alert_session_token_threshold", 0), 0),
            alert_target_id=cls._parse_str_list(config.get("alert_target_id", [])),
            alert_check_interval_minutes=cls._safe_int(config.get("alert_check_interval_minutes", 30), 30, minimum=1),
        )

    @staticmethod
    def _safe_int(raw, default: int, minimum: int | None = None) -> int:
        """安全整数解析：非法值回退默认，可附带下限兜底"""
        try:
            v = int(str(raw).strip())
        except (TypeError, ValueError):
            return default
        if minimum is not None and v < minimum:
            return max(default, minimum) if default >= minimum else minimum
        return v

    @staticmethod
    def _parse_str_list(raw) -> List[str]:
        """清洗字符串列表：逐项去空白、过滤空项"""
        if isinstance(raw, str):
            raw = [raw]
        elif not isinstance(raw, (list, tuple)):
            raw = []
        return [s for s in (str(x).strip() for x in raw) if s]

    def validate(self) -> bool:
        """验证配置有效性"""
        if self.auto_scan_interval_minutes <= 0:
            raise ValueError("自动扫描间隔时间必须大于 0 分钟")
        if self.scan_batch_size <= 0:
            raise ValueError("单批扫描数量必须大于 0")
        if self.auto_cleanup_retention_days < 1:
            raise ValueError("数据保留天数不能小于 1 天")
        if self.alert_daily_token_threshold < 0:
            raise ValueError("全局每日 Token 告警阈值不能为负数（填 0 表示不启用）")
        if self.alert_session_token_threshold < 0:
            raise ValueError("单会话每日 Token 告警阈值不能为负数（填 0 表示不启用）")
        if self.alert_check_interval_minutes < 1:
            raise ValueError("告警检查间隔必须大于 0 分钟")
        return True
