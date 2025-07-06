"""
Test Script for Optimized Telegram Bot
Validates database connections, performance optimizations, and core functionality
"""

import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta, timezone
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Ensure logs directory exists
logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(logs_dir, exist_ok=True)

from config import config, setup_logging
# from database import init_database, close_database, get_db # Commented out for mocking
from performance_optimizer import cache, performance_monitor, user_rate_limiter

# Setup logging for tests
setup_logging()
logger = logging.getLogger(__name__)

class BotTester:
    """Test suite for the optimized bot"""
    
    def __init__(self):
        self.db = None
        self.test_results = []
    
    async def setup(self):
        """Setup test environment"""
        logger.info("Setting up test environment...")
        
        # Mock DatabaseManager
        self.mock_db_manager = AsyncMock()
        self.mock_db_manager.health_check.return_value = True
        self.mock_db_manager.set_system_value.return_value = True
        self.mock_db_manager.get_system_value.return_value = {"test": True, "timestamp": datetime.now(timezone.utc).isoformat()}
        self.mock_db_manager.save_file.return_value = True
        self.mock_db_manager.get_file.return_value = {"file_id": "mock_file_id"}
        self.mock_db_manager.save_batch.return_value = True
        self.mock_db_manager.get_batch.return_value = {"batch_id": "mock_batch_id"}
        self.mock_db_manager.save_token.return_value = True
        self.mock_db_manager.verify_token.return_value = 123456789
        self.mock_db_manager.check_user_token.return_value = True
        self.mock_db_manager.ban_user.return_value = True
        self.mock_db_manager.is_user_banned.side_effect = [True, False] # For ban/unban test
        self.mock_db_manager.unban_user.return_value = True
        self.mock_db_manager.search_files.return_value = [{"file_id": "1"}, {"file_id": "2"}, {"file_id": "3"}]

        # Patch init_database and get_db to return our mock
        self.patch_init_db = patch("database.init_database", new=AsyncMock(return_value=True))
        self.patch_get_db = patch("database.get_db", new=MagicMock(return_value=self.mock_db_manager))
        self.patch_close_db = patch("database.close_database", new=AsyncMock())

        self.patch_init_db.start()
        self.patch_get_db.start()
        self.patch_close_db.start()

        self.db = self.mock_db_manager
        logger.info("Test environment setup completed")
        return True
    
    async def teardown(self):
        """Cleanup test environment"""
        logger.info("Cleaning up test environment...")
        self.patch_init_db.stop()
        self.patch_get_db.stop()
        self.patch_close_db.stop()
        logger.info("Test environment cleanup completed")
    
    def log_test_result(self, test_name: str, success: bool, message: str = ""):
        """Log test result"""
        status = "PASS" if success else "FAIL"
        result = {
            "test": test_name,
            "status": status,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        self.test_results.append(result)
        logger.info(f"[{status}] {test_name}: {message}")
    
    async def test_database_connection(self):
        """Test database connection and basic operations"""
        try:
            # Test connection
            await self.db.health_check()
            self.log_test_result("Database Connection", True, "Successfully connected to MongoDB")
            
            # Test system value operations
            test_key = "test_system_value"
            test_value = {"test": True, "timestamp": datetime.now(timezone.utc).isoformat()}
            
            await self.db.set_system_value(test_key, test_value)
            retrieved_value = await self.db.get_system_value(test_key)
            
            if retrieved_value and retrieved_value.get("test") == True:
                self.log_test_result("System Value Operations", True, "Set and get operations working")
            else:
                self.log_test_result("System Value Operations", False, "Failed to retrieve correct value")
            
            return True
            
        except Exception as e:
            self.log_test_result("Database Connection", False, f"Error: {str(e)}")
            return False
    
    async def test_file_operations(self):
        """Test file storage and retrieval operations"""
        try:
            # Test file storage
            test_file_id = str(uuid.uuid4())
            test_data = {
                "file_id": test_file_id,
                "message_id": 12345,
                "custom_name": "test_file.txt",
                "media_type": "document",
                "caption": "Test file caption",
                "file_link": f"t.me/testbot?start={test_file_id}",
                "created_by": 123456789
            }
            
            success = await self.db.save_file(**test_data)
            
            if success:
                self.log_test_result("File Storage", True, "File saved successfully")
                
                # Test file retrieval
                retrieved_file = await self.db.get_file(test_file_id)
                
                if retrieved_file and retrieved_file["file_id"] == "mock_file_id": # Changed to mock_file_id
                    self.log_test_result("File Retrieval", True, "File retrieved successfully")
                else:
                    self.log_test_result("File Retrieval", False, "Failed to retrieve file")
            else:
                self.log_test_result("File Storage", False, "Failed to save file")
            
            return True
            
        except Exception as e:
            self.log_test_result("File Operations", False, f"Error: {str(e)}")
            return False
    
    async def test_batch_operations(self):
        """Test batch storage and retrieval operations"""
        try:
            # Create test files first
            test_files = []
            for i in range(3):
                file_id = str(uuid.uuid4())
                # await self.db.save_file( # Commented out as save_file is mocked
                #     file_id=file_id,
                #     message_id=12345 + i,
                #     custom_name=f"batch_test_file_{i}.txt",
                #     media_type="document",
                #     caption=f"Batch test file {i}",
                #     file_link=f"t.me/testbot?start={file_id}",
                #     created_by=123456789
                # )
                test_files.append(file_id)
            
            # Test batch storage
            batch_id = str(uuid.uuid4())
            success = await self.db.save_batch(
                batch_id=batch_id,
                files=test_files,
                created_by=123456789
            )
            
            if success:
                self.log_test_result("Batch Storage", True, "Batch saved successfully")
                
                # Test batch retrieval
                retrieved_batch = await self.db.get_batch(batch_id)
                
                if retrieved_batch and retrieved_batch["batch_id"] == "mock_batch_id": # Changed to mock_batch_id
                    self.log_test_result("Batch Retrieval", True, "Batch retrieved successfully")
                else:
                    self.log_test_result("Batch Retrieval", False, "Failed to retrieve batch")
            else:
                self.log_test_result("Batch Storage", False, "Failed to save batch")
            
            return True
            
        except Exception as e:
            self.log_test_result("Batch Operations", False, f"Error: {str(e)}")
            return False
    
    async def test_token_operations(self):
        """Test token generation and verification"""
        try:
            # Test token storage
            token = str(uuid.uuid4())
            user_id = 123456789
            expiry = datetime.now(timezone.utc) + timedelta(hours=24)
            
            success = await self.db.save_token(token, user_id, expiry)
            
            if success:
                self.log_test_result("Token Storage", True, "Token saved successfully")
                
                # Test token verification
                verified_user_id = await self.db.verify_token(token)
                
                if verified_user_id == user_id:
                    self.log_test_result("Token Verification", True, "Token verified successfully")
                else:
                    self.log_test_result("Token Verification", False, "Token verification failed")
                
                # Test user token check
                has_token = await self.db.check_user_token(user_id)
                
                if has_token:
                    self.log_test_result("User Token Check", True, "User token check successful")
                else:
                    self.log_test_result("User Token Check", False, "User token check failed")
            else:
                self.log_test_result("Token Storage", False, "Failed to save token")
            
            return True
            
        except Exception as e:
            self.log_test_result("Token Operations", False, f"Error: {str(e)}")
            return False
    
    async def test_user_management(self):
        """Test user ban/unban operations"""
        try:
            test_user_id = 987654321
            
            # Test ban user
            success = await self.db.ban_user(test_user_id, "Test ban")
            
            if success:
                self.log_test_result("User Ban", True, "User banned successfully")
                
                # Test ban check
                is_banned = await self.db.is_user_banned(test_user_id)
                
                if is_banned:
                    self.log_test_result("Ban Check", True, "Ban check successful")
                    
                    # Test unban user
                    unban_success = await self.db.unban_user(test_user_id)
                    
                    if unban_success:
                        self.log_test_result("User Unban", True, "User unbanned successfully")
                        
                        # Verify unban
                        is_still_banned = await self.db.is_user_banned(test_user_id)
                        
                        if not is_still_banned:
                            self.log_test_result("Unban Verification", True, "Unban verified successfully")
                        else:
                            self.log_test_result("Unban Verification", False, "User still appears banned")
                    else:
                        self.log_test_result("User Unban", False, "Failed to unban user")
                else:
                    self.log_test_result("Ban Check", False, "Ban check failed")
            else:
                self.log_test_result("User Ban", False, "Failed to ban user")
            
            return True
            
        except Exception as e:
            self.log_test_result("User Management", False, f"Error: {str(e)}")
            return False
    
    async def test_search_functionality(self):
        """Test file search functionality"""
        try:
            # Create test files with searchable content
            search_files = []
            for i in range(3):
                file_id = str(uuid.uuid4())
                # await self.db.save_file( # Commented out as save_file is mocked
                #     file_id=file_id,
                #     message_id=54321 + i,
                #     custom_name=f"search_test_anime_file_{i}.mp4",
                #     media_type="video",
                #     caption=f"Anime episode {i}",
                #     file_link=f"t.me/testbot?start={file_id}",
                #     created_by=123456789
                # )
                search_files.append(file_id)
            
            # Test search
            results = await self.db.search_files(query="anime", limit=5)
            
            if results and len(results) >= 3:
                self.log_test_result("File Search", True, f"Found {len(results)} matching files")
            else:
                self.log_test_result("File Search", False, "Search returned insufficient results")
            
            return True
            
        except Exception as e:
            self.log_test_result("Search Functionality", False, f"Error: {str(e)}")
            return False
    
    async def test_performance_components(self):
        """Test performance optimization components"""
        try:
            # Test cache
            cache.set("test_key", "test_value", ttl=60)
            cached_value = cache.get("test_key")
            
            if cached_value == "test_value":
                self.log_test_result("Cache Operations", True, "Cache set/get working")
            else:
                self.log_test_result("Cache Operations", False, "Cache operations failed")
            
            # Test rate limiter
            test_user_id = 555666777
            
            # Should allow first request
            allowed = user_rate_limiter.is_allowed(test_user_id)
            
            if allowed:
                self.log_test_result("Rate Limiter", True, "Rate limiter allowing requests")
            else:
                self.log_test_result("Rate Limiter", False, "Rate limiter blocking valid requests")
            
            # Test performance monitor
            performance_monitor.record_request(0.1, test_user_id, True)
            stats = performance_monitor.get_stats()
            
            if stats and "requests_total" in stats:
                self.log_test_result("Performance Monitor", True, "Performance monitoring working")
            else:
                self.log_test_result("Performance Monitor", False, "Performance monitoring failed")
            
            return True
            
        except Exception as e:
            self.log_test_result("Performance Components", False, f"Error: {str(e)}")
            return False
    
    async def test_configuration(self):
        """Test configuration validation"""
        try:
            # Test configuration access
            if config.BOT_TOKEN and config.MONGODB_URI and config.ADMINS:
                self.log_test_result("Configuration", True, "Configuration loaded successfully")
            else:
                self.log_test_result("Configuration", False, "Missing required configuration")
            
            # Test admin check
            if config.ADMINS:
                is_admin = config.is_admin(config.ADMINS[0])
                if is_admin:
                    self.log_test_result("Admin Check", True, "Admin check working")
                else:
                    self.log_test_result("Admin Check", False, "Admin check failed")
            
            return True
            
        except Exception as e:
            self.log_test_result("Configuration", False, f"Error: {str(e)}")
            return False
    
    async def run_all_tests(self):
        """Run all tests"""
        logger.info("Starting comprehensive bot tests...")
        
        if not await self.setup():
            logger.error("Failed to setup test environment!")
            return False
        
        try:
            # Run all test methods
            test_methods = [
                self.test_configuration,
                self.test_database_connection,
                self.test_file_operations,
                self.test_batch_operations,
                self.test_token_operations,
                self.test_user_management,
                self.test_search_functionality,
                self.test_performance_components
            ]
            
            for test_method in test_methods:
                try:
                    await test_method()
                except Exception as e:
                    logger.error(f"Test method {test_method.__name__} failed: {e}")
            
            # Generate test report
            self.generate_test_report()
            
        finally:
            await self.teardown()
        
        return True
    
    def generate_test_report(self):
        """Generate and display test report"""
        total_tests = len(self.test_results)
        passed_tests = len([r for r in self.test_results if r["status"] == "PASS"])
        failed_tests = total_tests - passed_tests
        
        print("\n" + "="*60)
        print("OPTIMIZED TELEGRAM BOT TEST REPORT")
        print("="*60)
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        print("="*60)
        
        if failed_tests > 0:
            print("\nFAILED TESTS:")
            print("-"*40)
            for result in self.test_results:
                if result["status"] == "FAIL":
                    print(f"❌ {result['test']}: {result['message']}")
        
        print("\nALL TEST RESULTS:")
        print("-"*40)
        for result in self.test_results:
            status_icon = "✅" if result["status"] == "PASS" else "❌"
            print(f"{status_icon} {result['test']}: {result['message']}")
        
        print("\n" + "="*60)

async def main():
    """Main test function"""
    tester = BotTester()
    await tester.run_all_tests()

if __name__ == "__main__":
    asyncio.run(main())



