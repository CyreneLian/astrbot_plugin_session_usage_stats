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
    alert_mode: str = "token"                                      # 告警模式：token / rounds
    alert_daily_threshold: int = 0                                    # 全局每日告警阈值（按 alert_mode 解释 Token/轮数）
    alert_session_threshold: int = 0                                  # 单会话每日告警阈值（按 alert_mode 解释 Token/轮数）
    alert_target_id: List[str] = field(default_factory=list)      # 告警目标：QQ 号直接填（私聊），群号加 g: 前缀（群聊）
    alert_check_interval_minutes: int = 30
    # 用量限制
    limit_enabled: bool = False                                    # 限制总开关
    limit_mode: str = "token"                                      # 限制模式：token / rounds
    limit_session_default: int = 0                                 # 单会话每日限制默认值（0=不启用）
    limit_global_default: int = 0                                  # 全局每日限制（0=不启用）

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "SessionUsageStatsConfig":
        """从字典创建配置实例"""
        _am = cls._parse_mode(config.get("alert_mode", "token"))
        if _am == "rounds":
            _daily_src = config.get("alert_daily_threshold", config.get("alert_daily_rounds_threshold", 0))
            _sess_src = config.get("alert_session_threshold", config.get("alert_session_rounds_threshold", 0))
        else:
            _daily_src = config.get("alert_daily_threshold", config.get("alert_daily_token_threshold", 0))
            _sess_src = config.get("alert_session_threshold", config.get("alert_session_token_threshold", 0))
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
            alert_mode=cls._parse_mode(config.get("alert_mode", "token")),
            alert_daily_threshold=cls._safe_int(_daily_src, 0, minimum=0),
            alert_session_threshold=cls._safe_int(_sess_src, 0, minimum=0),
            alert_target_id=cls._parse_str_list(config.get("alert_target_id", [])),
            alert_check_interval_minutes=cls._safe_int(config.get("alert_check_interval_minutes", 30), 30, minimum=1),
            limit_enabled=cls._parse_bool(config.get("limit_enabled", False)),
            limit_mode=cls._parse_mode(config.get("limit_mode", "token")),
            limit_session_default=cls._safe_int(config.get("limit_session_default", 0), 0, minimum=0),
            limit_global_default=cls._safe_int(config.get("limit_global_default", 0), 0, minimum=0),
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
    def _parse_bool(raw, default: bool = False) -> bool:
        """严格布尔解析：仅接受显式真/假值，非法值回退默认"""
        if isinstance(raw, bool):
            return raw
        if raw is None:
            return default
        s = str(raw).strip().lower()
        if s in ("1", "true", "yes", "on", "开启", "是"):
            return True
        if s in ("0", "false", "no", "off", "关闭", "否", ""):
            return False
        return default

    @staticmethod
    def _parse_mode(raw) -> str:
        """模式解析：仅接受 token / rounds，非法值回退 token"""
        v = str(raw).strip().lower()
        return v if v in ("token", "rounds") else "token"

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
        if self.alert_daily_threshold < 0:
            raise ValueError("全局每日告警阈值不能为负数（填 0 表示不启用）")
        if self.alert_session_threshold < 0:
            raise ValueError("单会话每日告警阈值不能为负数（填 0 表示不启用）")
        if self.alert_check_interval_minutes < 1:
            raise ValueError("告警检查间隔必须大于 0 分钟")

        if self.limit_session_default < 0:
            raise ValueError("单会话每日用量限制不能为负数（填 0 表示不启用）")
        if self.limit_global_default < 0:
            raise ValueError("全局每日用量限制不能为负数（填 0 表示不启用）")
        return True
