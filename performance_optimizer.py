"""
Performance Optimization Module for High-Concurrency Telegram Bot
Includes caching, rate limiting, connection pooling, and monitoring
"""

import asyncio
import time
import logging
from typing import Dict, Any, Optional, Callable
from functools import wraps
from collections import defaultdict, deque
from datetime import datetime, timedelta
import weakref

logger = logging.getLogger(__name__)

class MemoryCache:
    """High-performance in-memory cache with TTL support"""
    
    def __init__(self, default_ttl: int = 300, max_size: int = 10000):
        self.default_ttl = default_ttl
        self.max_size = max_size
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._access_times: Dict[str, float] = {}
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start background cleanup task for MemoryCache"""
        if self._cleanup_task is None or (self._cleanup_task.done() if self._cleanup_task else False):
            self._cleanup_task = asyncio.create_task(self._cleanup_expired())

    async def stop(self):
        """Stop background cleanup task for MemoryCache"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
    
    async def _cleanup_expired(self):
        """Background task to clean up expired entries"""
        while True:
            try:
                current_time = time.time()
                expired_keys = []
                
                for key, data in self._cache.items():
                    if current_time > data["expires_at"]:
                        expired_keys.append(key)
                
                for key in expired_keys:
                    self._cache.pop(key, None)
                    self._access_times.pop(key, None)
                
                # If cache is too large, remove least recently used items
                if len(self._cache) > self.max_size:
                    # Sort by access time and remove oldest
                    sorted_keys = sorted(self._access_times.items(), key=lambda x: x[1])
                    keys_to_remove = sorted_keys[:len(self._cache) - self.max_size]
                    
                    for key, _ in keys_to_remove:
                        self._cache.pop(key, None)
                        self._access_times.pop(key, None)
                
                logger.debug(f"Cache cleanup: {len(expired_keys)} expired, {len(self._cache)} remaining")
                
                # Sleep for 60 seconds before next cleanup
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in cache cleanup: {e}")
                await asyncio.sleep(60)
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        if key not in self._cache:
            return None
        
        data = self._cache[key]
        current_time = time.time()
        
        if current_time > data["expires_at"]:
            # Expired
            self._cache.pop(key, None)
            self._access_times.pop(key, None)
            return None
        
        # Update access time
        self._access_times[key] = current_time
        return data["value"]
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache with TTL"""
        if ttl is None:
            ttl = self.default_ttl
        
        current_time = time.time()
        expires_at = current_time + ttl
        
        self._cache[key] = {
            "value": value,
            "expires_at": expires_at
        }
        self._access_times[key] = current_time
    
    def delete(self, key: str) -> bool:
        """Delete key from cache"""
        if key in self._cache:
            self._cache.pop(key, None)
            self._access_times.pop(key, None)
            return True
        return False
    
    def clear(self) -> None:
        """Clear all cache entries"""
        self._cache.clear()
        self._access_times.clear()
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "hit_rate": getattr(self, "_hit_rate", 0.0),
            "miss_rate": getattr(self, "_miss_rate", 0.0)
        }

class RateLimiter:
    """Token bucket rate limiter for API calls"""
    
    def __init__(self, max_tokens: int = 100, refill_rate: float = 10.0):
        self.max_tokens = max_tokens
        self.refill_rate = refill_rate
        self.tokens = max_tokens
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens from the bucket"""
        async with self._lock:
            now = time.time()
            
            # Refill tokens based on time passed
            time_passed = now - self.last_refill
            tokens_to_add = time_passed * self.refill_rate
            self.tokens = min(self.max_tokens, self.tokens + tokens_to_add)
            self.last_refill = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            
            return False
    
    async def wait_for_tokens(self, tokens: int = 1) -> None:
        """Wait until tokens are available"""
        while not await self.acquire(tokens):
            await asyncio.sleep(0.1)

class UserRateLimiter:
    """Per-user rate limiting"""
    
    def __init__(self, max_requests: int = 30, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.user_requests: Dict[int, deque] = defaultdict(deque)
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start background cleanup task for UserRateLimiter"""
        if self._cleanup_task is None or (self._cleanup_task.done() if self._cleanup_task else False):
            self._cleanup_task = asyncio.create_task(self._cleanup_old_requests())

    async def stop(self):
        """Stop background cleanup task for UserRateLimiter"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
    
    async def _cleanup_old_requests(self):
        """Clean up old request records"""
        while True:
            try:
                current_time = time.time()
                cutoff_time = current_time - self.window_seconds
                
                for user_id in list(self.user_requests.keys()):
                    requests = self.user_requests[user_id]
                    
                    # Remove old requests
                    while requests and requests[0] < cutoff_time:
                        requests.popleft()
                    
                    # Remove empty deques
                    if not requests:
                        del self.user_requests[user_id]
                
                await asyncio.sleep(30)  # Cleanup every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in user rate limiter cleanup: {e}")
                await asyncio.sleep(30)
    
    def is_allowed(self, user_id: int) -> bool:
        """Check if user is allowed to make a request"""
        current_time = time.time()
        cutoff_time = current_time - self.window_seconds
        
        requests = self.user_requests[user_id]
        
        # Remove old requests
        while requests and requests[0] < cutoff_time:
            requests.popleft()
        
        # Check if under limit
        if len(requests) < self.max_requests:
            requests.append(current_time)
            return True
        
        return False
    
    def get_reset_time(self, user_id: int) -> float:
        """Get time when rate limit resets for user"""
        requests = self.user_requests[user_id]
        if not requests:
            return 0
        
        return requests[0] + self.window_seconds

class ConnectionPool:
    """Async connection pool for database operations"""
    
    def __init__(self, create_connection: Callable, max_connections: int = 50):
        self.create_connection = create_connection
        self.max_connections = max_connections
        self._pool: asyncio.Queue = asyncio.Queue(maxsize=max_connections)
        self._created_connections = 0
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """Acquire a connection from the pool"""
        try:
            # Try to get an existing connection
            connection = self._pool.get_nowait()
            return connection
        except asyncio.QueueEmpty:
            # Create new connection if under limit
            async with self._lock:
                if self._created_connections < self.max_connections:
                    connection = await self.create_connection()
                    self._created_connections += 1
                    return connection
            
            # Wait for a connection to become available
            return await self._pool.get()
    
    async def release(self, connection):
        """Release a connection back to the pool"""
        try:
            self._pool.put_nowait(connection)
        except asyncio.QueueFull:
            # Pool is full, close the connection
            if hasattr(connection, "close"):
                await connection.close()
            self._created_connections -= 1
    
    async def close_all(self):
        """Close all connections in the pool"""
        while not self._pool.empty():
            try:
                connection = self._pool.get_nowait()
                if hasattr(connection, "close"):
                    await connection.close()
            except asyncio.QueueEmpty:
                break
        
        self._created_connections = 0

class PerformanceMonitor:
    """Monitor bot performance metrics"""
    
    def __init__(self):
        self.metrics = {
            "requests_total": 0,
            "requests_per_second": 0,
            "average_response_time": 0,
            "error_rate": 0,
            "active_users": set(),
            "database_queries": 0,
            "cache_hits": 0,
            "cache_misses": 0
        }
        self.response_times = deque(maxlen=1000)
        self.error_count = 0
        self.start_time = time.time()
        self._last_reset = time.time()
    
    def record_request(self, response_time: float, user_id: int, success: bool = True):
        """Record a request"""
        self.metrics["requests_total"] += 1
        self.response_times.append(response_time)
        self.metrics["active_users"].add(user_id)
        
        if not success:
            self.error_count += 1
        
        # Update averages
        if self.response_times:
            self.metrics["average_response_time"] = sum(self.response_times) / len(self.response_times)
        
        # Calculate requests per second
        current_time = time.time()
        time_diff = current_time - self._last_reset
        if time_diff >= 60:  # Reset every minute
            self.metrics["requests_per_second"] = self.metrics["requests_total"] / time_diff
            self._last_reset = current_time
        
        # Calculate error rate
        if self.metrics["requests_total"] > 0:
            self.metrics["error_rate"] = self.error_count / self.metrics["requests_total"]
    
    def record_database_query(self):
        """Record a database query"""
        self.metrics["database_queries"] += 1
    
    def record_cache_hit(self):
        """Record a cache hit"""
        self.metrics["cache_hits"] += 1
    
    def record_cache_miss(self):
        """Record a cache miss"""
        self.metrics["cache_misses"] += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current performance statistics"""
        uptime = time.time() - self.start_time
        
        cache_total = self.metrics["cache_hits"] + self.metrics["cache_misses"]
        cache_hit_rate = (self.metrics["cache_hits"] / cache_total * 100) if cache_total > 0 else 0
        
        return {
            "uptime_seconds": uptime,
            "requests_total": self.metrics["requests_total"],
            "requests_per_second": self.metrics["requests_per_second"],
            "average_response_time_ms": self.metrics["average_response_time"] * 1000,
            "error_rate_percent": self.metrics["error_rate"] * 100,
            "active_users_count": len(self.metrics["active_users"]),
            "database_queries": self.metrics["database_queries"],
            "cache_hit_rate_percent": cache_hit_rate,
            "memory_usage_mb": self._get_memory_usage()
        }
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except ImportError:
            return 0.0

# Global instances (initialized without starting background tasks)
cache = MemoryCache(default_ttl=300, max_size=10000)
rate_limiter = RateLimiter(max_tokens=100, refill_rate=10.0)
user_rate_limiter = UserRateLimiter(max_requests=30, window_seconds=60)
performance_monitor = PerformanceMonitor()

def cached(ttl: int = 300, key_func: Optional[Callable] = None):
    """Decorator for caching function results"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                cache_key = f"{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
            
            # Try to get from cache
            result = cache.get(cache_key)
            if result is not None:
                performance_monitor.record_cache_hit()
                return result
            
            performance_monitor.record_cache_miss()
            
            # Execute function and cache result
            result = await func(*args, **kwargs)
            cache.set(cache_key, result, ttl)
            
            return result
        return wrapper
    return decorator

def rate_limited(max_requests: int = 30, window_seconds: int = 60):
    """Decorator for rate limiting functions per user"""
    def decorator(func):
        @wraps(func)
        async def wrapper(update, context, *args, **kwargs):
            user_id = update.effective_user.id
            
            if not user_rate_limiter.is_allowed(user_id):
                reset_time = user_rate_limiter.get_reset_time(user_id)
                wait_time = reset_time - time.time()
                
                await update.message.reply_text(
                    f"⚠️ Rate limit exceeded. Please wait {int(wait_time)} seconds."
                )
                return
            
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator

def monitored(func):
    """Decorator for monitoring function performance"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        success = True
        user_id = None
        
        try:
            # Try to extract user_id from update
            if args and hasattr(args[0], "effective_user"):
                user_id = args[0].effective_user.id
            
            result = await func(*args, **kwargs)
            return result
            
        except Exception as e:
            success = False
            raise e
            
        finally:
            response_time = time.time() - start_time
            if user_id:
                performance_monitor.record_request(response_time, user_id, success)
    
    return wrapper

async def optimize_database_queries():
    """Optimize database queries by batching and caching"""
    # This would contain database-specific optimizations
    pass

async def preload_cache():
    """Preload frequently accessed data into cache"""
    try:
        from database import get_db
        
        db = get_db()
        
        # Preload system settings
        system_settings = await db.get_system_value("bot_settings")
        if system_settings:
            cache.set("system_settings", system_settings, ttl=3600)
        
        # Preload frequently accessed tokens
        valid_token = await db.get_valid_token()
        if valid_token:
            cache.set("valid_system_token", valid_token, ttl=1800)
        
        logger.info("Cache preloaded successfully")
        
    except Exception as e:
        logger.error(f"Error preloading cache: {e}")

async def cleanup_resources():
    """Cleanup resources on shutdown"""
    try:
        # Cancel cleanup tasks
        await cache.stop()
        await user_rate_limiter.stop()
        
        # Clear cache
        cache.clear()
        
        logger.info("Performance optimization resources cleaned up")
        
    except Exception as e:
        logger.error(f"Error cleaning up performance resources: {e}")

# Utility functions for performance optimization
async def batch_database_operations(operations: list, batch_size: int = 100):
    """Batch database operations for better performance"""
    results = []
    
    for i in range(0, len(operations), batch_size):
        batch = operations[i:i + batch_size]
        batch_results = await asyncio.gather(*batch, return_exceptions=True)
        results.extend(batch_results)
    
    return results

def get_performance_stats() -> Dict[str, Any]:
    """Get current performance statistics"""
    return {
        "performance": performance_monitor.get_stats(),
        "cache": cache.stats(),
        "rate_limiter": {
            "tokens_available": rate_limiter.tokens,
            "max_tokens": rate_limiter.max_tokens
        }
    }



