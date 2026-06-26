"""
错误处理与工具函数
"""
import functools
import logging
from typing import Callable, Any, AsyncGenerator
from astrbot.api.event import AstrMessageEvent

logger = logging.getLogger(__name__)

def handle_errors(func: Callable) -> Callable:
    """统一错误处理装饰器

    捕获并处理函数执行过程中的各种异常，向用户返回友好的错误提示。
    """
    @functools.wraps(func)
    async def wrapper(self, event: AstrMessageEvent, *args, **kwargs) -> AsyncGenerator[Any, None]:
        try:
            async for result in func(self, event, *args, **kwargs):
                yield result
        except PermissionError as e:
            logger.error(f"[{func.__name__}] 权限不足: {e}", exc_info=True)
            yield event.plain_result("❌ 权限不足，请检查文件权限")
        except (IOError, OSError) as e:
            logger.error(f"[{func.__name__}] 文件操作失败: {e}", exc_info=True)
            yield event.plain_result("❌ 文件操作失败，请检查文件是否存在")
        except Exception as e:
            error_type = type(e).__name__
            logger.error(f"[{func.__name__}] 执行失败 [{error_type}]: {e}", exc_info=True)
            yield event.plain_result("❌ 操作失败，请联系管理员")
    return wrapper
