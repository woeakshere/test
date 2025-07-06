"""
Configuration Management for Optimized Telegram Bot
Handles environment variables and configuration validation
"""

import os
import logging
from typing import List, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class Config:
    """Configuration class with validation and defaults"""
    
    # Telegram Bot Configuration
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMINS: List[int] = []
    OWNER_ID: int = 0
    DATABASE_CHANNEL: int = int(os.getenv("DATABASE_CHANNEL", "0"))
    LINKS_CHANNEL: int = int(os.getenv("LINKS_CHANNEL", "0"))
    FORCE_SUB: int = int(os.getenv("FORCE_SUB", "0"))
    
    # Token System Configuration
    TOKEN_DURATION: int = int(os.getenv("TOKEN_DURATION", "24"))
    TOKEN_VERIFICATION_ENABLED: bool = os.getenv("TOKEN_VERIFICATION_ENABLED", "1") == "1"
    GET_TOKEN: str = os.getenv("GET_TOKEN", "")
    RENAME_TEMPLATE: str = os.getenv("RENAME_TEMPLATE", "")
    
    # MongoDB Configuration
    MONGODB_URI: str = os.getenv("MONGODB_URI", "")
    MONGODB_DATABASE: str = os.getenv("MONGODB_DATABASE", "")

    MONGODB_MAX_POOL_SIZE: int = int(os.getenv("MONGODB_MAX_POOL_SIZE", "100"))
    MONGODB_MIN_POOL_SIZE: int = int(os.getenv("MONGODB_MIN_POOL_SIZE", "10"))
    
    # Performance Configuration
    CACHE_TTL: int = int(os.getenv("CACHE_TTL", "300"))
    CACHE_MAX_SIZE: int = int(os.getenv("CACHE_MAX_SIZE", "10000"))
    RATE_LIMIT_REQUESTS: int = int(os.getenv("RATE_LIMIT_REQUESTS", "30"))
    RATE_LIMIT_WINDOW: int = int(os.getenv("RATE_LIMIT_WINDOW", "60"))
    MAX_CONCURRENT_REQUESTS: int = int(os.getenv("MAX_CONCURRENT_REQUESTS", "100"))
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: Optional[str] = os.getenv("LOG_FILE")
    
    # Development Configuration
    DEBUG: bool = os.getenv("DEBUG", "0") == "1"
    
    def __init__(self):
        """Initialize configuration and validate required settings"""
        self._parse_admins()
        self._validate_config()
    
    def _parse_admins(self):
        """Parse ADMINS environment variable"""
        admins_str = os.getenv("ADMINS", "")
        if admins_str:
            try:
                self.ADMINS = [int(id.strip()) for id in admins_str.split(",") if id.strip()]
                if self.ADMINS:
                    self.OWNER_ID = self.ADMINS[0]
            except ValueError as e:
                logger.error(f"Invalid ADMINS format: {e}")
                self.ADMINS = []
    
    def _validate_config(self):
        """Validate required configuration"""
        errors = []
        
        if not self.BOT_TOKEN:
            errors.append("BOT_TOKEN is required")
        
        if not self.MONGODB_URI:
            errors.append("MONGODB_URI is required")
        
        if not self.ADMINS:
            errors.append("At least one admin must be configured in ADMINS")
        
        if self.DATABASE_CHANNEL == 0:
            errors.append("DATABASE_CHANNEL must be configured")
        
        if self.TOKEN_DURATION <= 0:
            errors.append("TOKEN_DURATION must be positive")
        
        if self.CACHE_TTL <= 0:
            errors.append("CACHE_TTL must be positive")
        
        if self.RATE_LIMIT_REQUESTS <= 0:
            errors.append("RATE_LIMIT_REQUESTS must be positive")
        
        if self.RATE_LIMIT_WINDOW <= 0:
            errors.append("RATE_LIMIT_WINDOW must be positive")
        
        if errors:
            error_msg = "Configuration validation failed:\n" + "\n".join(f"- {error}" for error in errors)
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info("Configuration validation passed")
    
    def get_mongodb_config(self) -> dict:
        """Get MongoDB connection configuration"""
        return {
            "uri": self.MONGODB_URI,
            "database": self.MONGODB_DATABASE,
            "maxPoolSize": self.MONGODB_MAX_POOL_SIZE,
            "minPoolSize": self.MONGODB_MIN_POOL_SIZE,
            "serverSelectionTimeoutMS": 5000,
            "connectTimeoutMS": 10000,
            "socketTimeoutMS": 20000,
            "retryWrites": True,
            "retryReads": True
        }
    
    def get_cache_config(self) -> dict:
        """Get cache configuration"""
        return {
            "default_ttl": self.CACHE_TTL,
            "max_size": self.CACHE_MAX_SIZE
        }
    
    def get_rate_limit_config(self) -> dict:
        """Get rate limiting configuration"""
        return {
            "max_requests": self.RATE_LIMIT_REQUESTS,
            "window_seconds": self.RATE_LIMIT_WINDOW,
            "max_concurrent": self.MAX_CONCURRENT_REQUESTS
        }
    
    def get_logging_config(self) -> dict:
        """Get logging configuration"""
        config = {
            "level": getattr(logging, self.LOG_LEVEL.upper(), logging.INFO),
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
        
        if self.LOG_FILE:
            config["filename"] = self.LOG_FILE
        
        return config
    
    def is_admin(self, user_id: int) -> bool:
        """Check if user is an admin"""
        return user_id in self.ADMINS
    
    def is_owner(self, user_id: int) -> bool:
        """Check if user is the owner"""
        return user_id == self.OWNER_ID
    
    def __repr__(self) -> str:
        """String representation of config (without sensitive data)"""
        return (
            f"Config("
            f"admins_count={len(self.ADMINS)}, "
            f"database_channel={self.DATABASE_CHANNEL}, "
            f"links_channel={self.LINKS_CHANNEL}, "
            f"token_duration={self.TOKEN_DURATION}, "
            f"debug={self.DEBUG}"
            f")"
        )

# Global configuration instance
config = Config()

def setup_logging():
    """Setup logging configuration"""
    log_config = config.get_logging_config()
    
    # Configure root logger
    logging.basicConfig(
        level=log_config["level"],
        format=log_config["format"],
        filename=log_config.get("filename"),
        filemode="a" if log_config.get("filename") else None
    )
    
    # Set specific logger levels
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("motor").setLevel(logging.WARNING)
    
    if config.DEBUG:
        logging.getLogger(__name__.split('.')[0]).setLevel(logging.DEBUG)
    
    logger.info(f"Logging configured: {log_config}")

def validate_environment():
    """Validate environment and configuration"""
    try:
        # Test MongoDB URI format
        from pymongo import MongoClient
        from urllib.parse import urlparse
        
        parsed = urlparse(config.MONGODB_URI)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Invalid MONGODB_URI format")
        
        logger.info("Environment validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Environment validation failed: {e}")
        return False

def get_config() -> Config:
    """Get global configuration instance"""
    return config

