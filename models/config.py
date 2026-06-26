"""
插件配置模型模块
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any

@dataclass
class SessionUsageStatsConfig:
    """插件配置模型"""
    enable_auto_scan: bool = False
    auto_scan_interval_minutes: int = 5
    scan_batch_size: int = 500
    enabled_platforms: List[str] = field(default_factory=lambda: ["webchat", "aiocqhttp"])
    include_threads: bool = False
    enable_event_capture: bool = False
    event_capture_platforms: List[str] = field(default_factory=lambda: ["aiocqhttp"])
    auto_cleanup_enabled: bool = True
    auto_cleanup_retention_days: int = 30
    auto_cleanup_vacuum: bool = True

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "SessionUsageStatsConfig":
        """从字典创建配置实例"""
        return cls(
            enable_auto_scan=bool(config.get("enable_auto_scan", False)),
            auto_scan_interval_minutes=int(config.get("auto_scan_interval_minutes", 5) or 5),
            scan_batch_size=int(config.get("scan_batch_size", 500) or 500),
            enabled_platforms=list(config.get("enabled_platforms", ["webchat", "aiocqhttp"]) or ["webchat", "aiocqhttp"]),
            include_threads=bool(config.get("include_threads", False)),
            enable_event_capture=bool(config.get("enable_event_capture", False)),
            event_capture_platforms=list(config.get("event_capture_platforms", ["aiocqhttp"]) or ["aiocqhttp"]),
            auto_cleanup_enabled=bool(config.get("auto_cleanup_enabled", True)),
            auto_cleanup_retention_days=int(config.get("auto_cleanup_retention_days", 30) or 30),
            auto_cleanup_vacuum=bool(config.get("auto_cleanup_vacuum", True)),
        )

    def validate(self) -> bool:
        """验证配置有效性"""
        if self.auto_scan_interval_minutes <= 0:
            raise ValueError("自动扫描间隔时间必须大于 0 分钟")
        if self.scan_batch_size <= 0:
            raise ValueError("单批扫描数量必须大于 0")
        if self.auto_cleanup_retention_days < 1:
            raise ValueError("数据保留天数不能小于 1 天")
        return True
