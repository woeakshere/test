"""
MongoDB Database Connection and Models for Telegram Bot
Optimized for high-performance handling of thousands of concurrent users
"""

import os
import logging
import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from pymongo import IndexModel, ASCENDING, DESCENDING, TEXT
from pymongo.errors import DuplicateKeyError, ConnectionFailure, OperationFailure

# Configure logging
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Async MongoDB database manager with connection pooling and optimization"""
    
    def __init__(self, mongodb_uri: str, database_name: str):
        self.mongodb_uri = mongodb_uri
        self.database_name = "Cluster0" # Explicitly set database name from URI appName
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
        self._collections: Dict[str, AsyncIOMotorCollection] = {}
        
    async def connect(self):
        """Establish connection to MongoDB with optimized settings"""
        try:
            self.client = AsyncIOMotorClient(
                self.mongodb_uri,
                maxPoolSize=100,
                minPoolSize=10,
                maxIdleTimeMS=30000,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=20000,
                retryWrites=True,
                retryReads=True
            )
            
            # Test authentication explicitly
            try:
                # This forces authentication check
                await self.client.server_info()
            except OperationFailure as e:
                if "Authentication failed" in str(e):
                    logger.critical("MongoDB authentication failed. Check credentials in connection URI.")
                    # Log sanitized URI for debugging (without password)
                    sanitized_uri = re.sub(r"://(.*?):(.*?)@", "://[USER]:[PASSWORD]@", self.mongodb_uri)
                    logger.debug(f"Connection URI: {sanitized_uri}")
                    return False
                raise
            
            self.db = self.client[self.database_name]
            await self._initialize_collections()
            logger.info(f"✅ MongoDB connection successful: {self.database_name}")
            return True
            
        except ConnectionFailure as e:
            logger.error(f"❌ Connection failure: {e}")
        except OperationFailure as e:
            logger.error(f"❌ Operation failure: {e.code_name} - {e.details}")
        except Exception as e:
            logger.error(f"❌ Unexpected connection error: {str(e)}")
        return False
    
    async def _initialize_collections(self):
        """Initialize collections and create indexes"""
        # Define collections
        collection_names = ["users", "files", "batches", "tokens", "groups", "system"]
        
        for name in collection_names:
            self._collections[name] = self.db[name]
        
        # Create indexes for performance
        await self._create_indexes()
    
    async def _create_indexes(self):
        """Create database indexes for optimal performance"""
        try:
            # Users collection indexes
            await self._collections["users"].create_indexes([
                IndexModel([("user_id", ASCENDING)], unique=True),
                IndexModel([("is_banned", ASCENDING)]),
                IndexModel([("created_at", DESCENDING)])
            ])
            
            # Files collection indexes
            await self._collections["files"].create_indexes([
                IndexModel([("file_id", ASCENDING)], unique=True),
                IndexModel([("created_at", DESCENDING)]),
                IndexModel([("created_by", ASCENDING), ("created_at", DESCENDING)]),
                IndexModel([("custom_name", TEXT), ("caption", TEXT)]),  # Text search
                IndexModel([("access_count", DESCENDING)]),
                IndexModel([("last_accessed", DESCENDING)])
            ])
            
            # Batches collection indexes
            await self._collections["batches"].create_indexes([
                IndexModel([("batch_id", ASCENDING)], unique=True),
                IndexModel([("created_at", DESCENDING)]),
                IndexModel([("created_by", ASCENDING), ("created_at", DESCENDING)]),
                IndexModel([("access_count", DESCENDING)])
            ])
            
            # Tokens collection indexes (with TTL for automatic cleanup)
            await self._collections["tokens"].create_indexes([
                IndexModel([("token", ASCENDING)], unique=True),
                IndexModel([("expiry", ASCENDING)], expireAfterSeconds=0),  # TTL index
                IndexModel([("user_id", ASCENDING)]),
                IndexModel([("created_at", DESCENDING)])
            ])
            
            # Groups collection indexes
            await self._collections["groups"].create_indexes([
                IndexModel([("chat_id", ASCENDING)], unique=True),
                IndexModel([("last_activity", DESCENDING)]),
                IndexModel([("total_files_shared", DESCENDING)])
            ])
            
            # System collection indexes
            await self._collections["system"].create_indexes([
                IndexModel([("key", ASCENDING)], unique=True),
                IndexModel([("updated_at", DESCENDING)])
            ])
            
            logger.info("Database indexes created successfully")
            
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
    
    # User operations
    async def get_user(self, user_id: int) -> Optional[Dict]:
        """Get user by ID"""
        try:
            return await self._collections["users"].find_one({"user_id": user_id})
        except Exception as e:
            logger.error(f"Error getting user {user_id}: {e}")
            return None
    
    async def is_user_banned(self, user_id: int) -> bool:
        """Check if user is banned (optimized for frequent calls)"""
        try:
            user = await self._collections["users"].find_one(
                {"user_id": user_id, "is_banned": True},
                {"_id": 1}  # Only return _id field for efficiency
            )
            return user is not None
        except Exception as e:
            logger.error(f"Error checking ban status for user {user_id}: {e}")
            return False
    
    async def ban_user(self, user_id: int, reason: str = "") -> bool:
        """Ban a user"""
        try:
            await self._collections["users"].update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "is_banned": True,
                        "ban_date": datetime.now(timezone.utc),
                        "ban_reason": reason,
                        "updated_at": datetime.now(timezone.utc)
                    },
                    "$setOnInsert": {
                        "created_at": datetime.now(timezone.utc)
                    }
                },
                upsert=True
            )
            logger.info(f"Banned user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error banning user {user_id}: {e}")
            return False
    
    async def unban_user(self, user_id: int) -> bool:
        """Unban a user"""
        try:
            result = await self._collections["users"].update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "is_banned": False,
                        "updated_at": datetime.now(timezone.utc)
                    }
                }
            )
            if result.modified_count > 0:
                logger.info(f"Unbanned user {user_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error unbanning user {user_id}: {e}")
            return False
    
    async def get_banned_users(self) -> List[int]:
        """Get list of banned user IDs"""
        try:
            cursor = self._collections["users"].find(
                {"is_banned": True},
                {"user_id": 1, "_id": 0}
            )
            users = await cursor.to_list(length=None)
            return [user["user_id"] for user in users]
        except Exception as e:
            logger.error(f"Error getting banned users: {e}")
            return []
    
    # File operations
    async def save_file(self, file_id: str, message_id: int, custom_name: str = "",
                       media_type: str = "unknown", caption: str = "",
                       file_link: str = "", links_channel_msg_id: int = None,
                       created_by: int = 0) -> bool:
        """Save file metadata"""
        try:
            file_doc = {
                "file_id": file_id,
                "message_id": message_id,
                "custom_name": custom_name,
                "media_type": media_type,
                "caption": caption,
                "file_link": file_link,
                "links_channel_msg_id": links_channel_msg_id,
                "created_by": created_by,
                "created_at": datetime.now(timezone.utc),
                "access_count": 0,
                "last_accessed": None
            }
            
            await self._collections["files"].insert_one(file_doc)
            logger.info(f"Saved file {file_id}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"File {file_id} already exists")
            return False
        except Exception as e:
            logger.error(f"Error saving file {file_id}: {e}")
            return False
    
    async def get_file(self, file_id: str) -> Optional[Dict]:
        """Get file by ID and increment access count"""
        try:
            # Update access count and last accessed time
            result = await self._collections["files"].find_one_and_update(
                {"file_id": file_id},
                {
                    "$inc": {"access_count": 1},
                    "$set": {"last_accessed": datetime.now(timezone.utc)}
                },
                return_document=True
            )
            return result
        except Exception as e:
            logger.error(f"Error getting file {file_id}: {e}")
            return None
    
    async def search_files(self, query: str, date_filter: str = None,
                          limit: int = 50) -> List[Dict]:
        """Search files by text query and optional date filter"""
        try:
            search_filter = {}
            
            # Text search
            if query:
                search_filter["$text"] = {"$search": query}
            
            # Date filter
            if date_filter:
                try:
                    date_obj = datetime.strptime(date_filter, "%Y-%m-%d")
                    next_day = date_obj + timedelta(days=1)
                    search_filter["created_at"] = {
                        "$gte": date_obj,
                        "$lt": next_day
                    }
                except ValueError:
                    logger.warning(f"Invalid date format: {date_filter}")
            
            cursor = self._collections["files"].find(search_filter).limit(limit)
            
            # Sort by relevance if text search, otherwise by date
            if query:
                cursor = cursor.sort([("score", {"$meta": "textScore"})])
            else:
                cursor = cursor.sort([("created_at", DESCENDING)])
            
            return await cursor.to_list(length=limit)
            
        except Exception as e:
            logger.error(f"Error searching files: {e}")
            return []
    
    # Batch operations
    async def save_batch(self, batch_id: str, files: List[str],
                        links_channel_msg_id: int = None,
                        created_by: int = 0) -> bool:
        """Save batch metadata"""
        try:
            batch_doc = {
                "batch_id": batch_id,
                "files": files,
                "total_files": len(files),
                "links_channel_msg_id": links_channel_msg_id,
                "created_by": created_by,
                "created_at": datetime.now(timezone.utc),
                "access_count": 0,
                "last_accessed": None
            }
            
            await self._collections["batches"].insert_one(batch_doc)
            logger.info(f"Saved batch {batch_id} with {len(files)} files")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"Batch {batch_id} already exists")
            return False
        except Exception as e:
            logger.error(f"Error saving batch {batch_id}: {e}")
            return False
    
    async def get_batch(self, batch_id: str) -> Optional[Dict]:
        """Get batch by ID and increment access count"""
        try:
            result = await self._collections["batches"].find_one_and_update(
                {"batch_id": batch_id},
                {
                    "$inc": {"access_count": 1},
                    "$set": {"last_accessed": datetime.now(timezone.utc)}
                },
                return_document=True
            )
            return result
        except Exception as e:
            logger.error(f"Error getting batch {batch_id}: {e}")
            return None
    
    # Token operations
    async def save_token(self, token: str, user_id: int, expiry: datetime) -> bool:
        """Save access token with automatic expiry"""
        try:
            token_doc = {
                "token": token,
                "user_id": user_id,
                "expiry": expiry,
                "created_at": datetime.now(timezone.utc),
                "used_count": 0,
                "last_used": None
            }
            
            await self._collections["tokens"].insert_one(token_doc)
            logger.info(f"Saved token for user {user_id}, expires at {expiry}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"Token {token} already exists")
            return False
        except Exception as e:
            logger.error(f"Error saving token: {e}")
            return False
    
    async def verify_token(self, token: str) -> Optional[int]:
        """Verify token and return user_id if valid"""
        try:
            result = await self._collections["tokens"].find_one_and_update(
                {
                    "token": token,
                    "expiry": {"$gt": datetime.now(timezone.utc)}
                },
                {
                    "$inc": {"used_count": 1},
                    "$set": {"last_used": datetime.now(timezone.utc)}
                },
                return_document=True
            )
            
            if result:
                return result["user_id"]
            return None
            
        except Exception as e:
            logger.error(f"Error verifying token: {e}")
            return None
    
    async def check_user_token(self, user_id: int) -> bool:
        """Check if user has a valid token"""
        try:
            token = await self._collections["tokens"].find_one({
                "$or": [
                    {"user_id": user_id},
                    {"user_id": 0}  # System tokens valid for all users
                ],
                "expiry": {"$gt": datetime.now(timezone.utc)}
            })
            return token is not None
        except Exception as e:
            logger.error(f"Error checking user token: {e}")
            return False
    
    async def get_valid_token(self) -> Optional[Dict]:
        """Get a valid system token (user_id = 0)"""
        try:
            return await self._collections["tokens"].find_one({
                "user_id": 0,
                "expiry": {"$gt": datetime.now(timezone.utc)}
            }, sort=[("expiry", DESCENDING)])
        except Exception as e:
            logger.error(f"Error getting valid token: {e}")
            return None
    
    # Group operations
    async def get_group_settings(self, chat_id: int) -> Dict:
        """Get group settings"""
        try:
            group = await self._collections["groups"].find_one({"chat_id": chat_id})
            if group:
                return group
            
            # Return default settings if group not found
            return {
                "chat_id": chat_id,
                "auto_delete_minutes": 0,
                "total_files_shared": 0,
                "total_searches": 0,
                "active_members": {},
                "search_terms": {},
                "last_activity": datetime.now(timezone.utc),
                "created_at": datetime.now(timezone.utc)
            }
        except Exception as e:
            logger.error(f"Error getting group settings for {chat_id}: {e}")
            return {}
    
    async def update_group_settings(self, chat_id: int, settings: Dict) -> bool:
        """Update group settings"""
        try:
            settings["updated_at"] = datetime.now(timezone.utc)
            
            await self._collections["groups"].update_one(
                {"chat_id": chat_id},
                {
                    "$set": settings,
                    "$setOnInsert": {"created_at": datetime.now(timezone.utc)}
                },
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error updating group settings for {chat_id}: {e}")
            return False
    
    async def update_group_stats(self, chat_id: int, action_type: str,
                                user_id: int = None, search_term: str = None) -> bool:
        """Update group statistics"""
        try:
            update_doc = {
                "$set": {"last_activity": datetime.now(timezone.utc)},
                "$setOnInsert": {
                    "chat_id": chat_id,
                    "created_at": datetime.now(timezone.utc),
                    "auto_delete_minutes": 0,
                    "total_files_shared": 0,
                    "total_searches": 0,
                    "active_members": {},
                    "search_terms": {}
                }
            }
            
            if action_type == "file":
                update_doc["$inc"] = {"total_files_shared": 1}
            elif action_type == "search":
                update_doc["$inc"] = {"total_searches": 1}
                if search_term:
                    update_doc["$inc"][f"search_terms.{search_term}"] = 1
            
            if user_id:
                update_doc["$inc"][f"active_members.{user_id}"] = 1
            
            await self._collections["groups"].update_one(
                {"chat_id": chat_id},
                update_doc,
                upsert=True
            )
            return True
            
        except Exception as e:
            logger.error(f"Error updating group stats for {chat_id}: {e}")
            return False
    
    # System operations
    async def get_system_value(self, key: str) -> Any:
        """Get system configuration value"""
        try:
            doc = await self._collections["system"].find_one({"key": key})
            return doc["value"] if doc else None
        except Exception as e:
            logger.error(f"Error getting system value {key}: {e}")
            return None
    
    async def set_system_value(self, key: str, value: Any) -> bool:
        """Set system configuration value"""
        try:
            await self._collections["system"].update_one(
                {"key": key},
                {
                    "$set": {
                        "value": value,
                        "updated_at": datetime.now(timezone.utc)
                    }
                },
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error setting system value {key}: {e}")
            return False
    
    # Health check
    async def health_check(self) -> bool:
        """Check database connection health"""
        try:
            await self.client.admin.command("ping")
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    async def close(self):
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

# Global database instance
db_manager: Optional[DatabaseManager] = None

async def init_database(mongodb_uri: str, database_name: str) -> bool:
    """Initialize global database connection"""
    global db_manager
    
    db_manager = DatabaseManager(mongodb_uri, database_name)
    return await db_manager.connect()

async def close_database():
    """Close global database connection"""
    global db_manager
    if db_manager:
        try:
            await db_manager.close()
            logger.info("Database connection closed successfully")
            return True
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")
            return False
    return True

def get_db() -> DatabaseManager:
    """Get global database manager instance"""
    global db_manager
    
    if not db_manager:
        raise RuntimeError("Database not initialized. Call init_database() first.")
    
    return db_manager




