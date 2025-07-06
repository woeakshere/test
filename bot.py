"""
Final Optimized Telegram Bot with MongoDB Integration
High-performance bot designed for thousands of concurrent users
Includes caching, rate limiting, performance monitoring, and optimizations
"""

import os
import logging
import random
import uuid
import sys
import asyncio
import time
import re
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackContext,
    CallbackQueryHandler,
    ConversationHandler
)

# Import custom modules
from config import config, setup_logging, validate_environment
from database import init_database, close_database, get_db
from performance_optimizer import (
    cached, rate_limited, monitored, 
    cache, performance_monitor, user_rate_limiter,
    preload_cache, cleanup_resources, get_performance_stats
)

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

# Validate environment
if not validate_environment():
    logger.error("Environment validation failed!")
    sys.exit(1)

# Mikasa's Personality Database
MIKASA_QUOTES = {
    'ban': ["Threat neutralized. Eren is safe.", "A Lot Of People I Used To Care About Aren't Here Either"],
    'unban': ["🎌 Access restored!", "⚔️ Second chance granted!"],
    'error': ["💔 Failed... Eren would be disappointed!", "I Don't Want To Lose What Little Family I Have Left."],
    'success': [" I Am Strong. Stronger Than All Of You. Extremely Strong. I Can Kill All The Titans Out There. Even If I Am Alone", "Eat It, Eat And Stay Alive, I Won't Let You Starve To Death"],
    'warning': [" Not so fast!", " I'm watching you..."],
    'info': ["📜 Report:", "📜Status:"],
    'greeting': ["⚔️ I'll protect you.", "I'll always be by your side","My Scarf, Thank You For Always Wrapping It Around Me","✊Shinzo wo sasageyo"],
    'default': ["If I can't, then I'll just die. But if I win I live. Unless I fight, I cannot win","Once I'm dead, I won't even be able to remember you. So I'll win, no matter what. I'll live, no matter what"],
    'welcome': ["Welcome to our group! I'll protect everyone here.", "A new comrade has joined our ranks. Together we'll fight!"]
}

def mikasa_reply(category='default'):
    return random.choice(MIKASA_QUOTES.get(category, MIKASA_QUOTES['default'])) + "\n"

def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not config.is_admin(update.effective_user.id):
            await update.message.reply_text(mikasa_reply('warning') + "Unauthorized!")
            return
        return await func(update, context)
    return wrapper

def owner_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not config.is_owner(update.effective_user.id):
            await update.message.reply_text(mikasa_reply('warning') + "This command is only available to the owner!")
            return
        return await func(update, context)
    return wrapper

@monitored
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced error handler with database logging and monitoring"""
    logger.error(f"Error: {context.error}")
    
    try:
        # Safely get chat_id
        chat_id = None
        if update and hasattr(update, 'effective_chat') and update.effective_chat:
            chat_id = update.effective_chat.id
        else:
            chat_id = config.ADMINS[0] if config.ADMINS else None
        
        # Only send message if we have a valid chat_id
        if chat_id:
            await context.bot.send_message(
                chat_id=chat_id,
                text=mikasa_reply('error') + f"Error: {context.error}"
            )
        
        # Log error to database
        db = get_db()
        await db.set_system_value("last_error", {
            "error": str(context.error),
            "timestamp": datetime.utcnow().isoformat(),
            "chat_id": chat_id
        })
        
    except Exception as e:
        logger.error(f"Failed to handle error: {e}")

# ========== TOKEN VERIFICATION SYSTEM ========== #
@cached(ttl=1800, key_func=lambda user_id, context: f"token_gen_{user_id}")
async def generate_token(user_id, context):
    """Generate a unique token for a user with caching"""
    token = str(uuid.uuid4())
    expiry = datetime.utcnow() + timedelta(hours=config.TOKEN_DURATION)
    
    # Store token in database
    db = get_db()
    success = await db.save_token(token, user_id, expiry)
    
    if not success:
        logger.error(f"Failed to save token for user {user_id}")
        return None, None
    
    # Create verification URL
    bot_username = context.bot.username
    verification_url = f"https://t.me/{bot_username}?start=verify_{token}"
    
    # Send direct token URL to all admins
    expiry_time = expiry.strftime('%Y-%m-%d %H:%M:%S')
    token_message = f"🔑 New Token Generated\n\nVerification URL: {verification_url}\nExpires: {expiry_time}"
    
    # Send to admins in parallel
    admin_tasks = []
    for admin_id in config.ADMINS:
        task = context.bot.send_message(chat_id=admin_id, text=token_message)
        admin_tasks.append(task)
    
    if admin_tasks:
        await asyncio.gather(*admin_tasks, return_exceptions=True)
    
    # Send and pin token URL in database channel
    try:
        db_msg = await context.bot.send_message(
            chat_id=config.DATABASE_CHANNEL,
            text=f"🔑 Current Access Token (valid for {config.TOKEN_DURATION} hours)\n\nVerification URL: {verification_url}"
        )
        
        await context.bot.pin_chat_message(
            chat_id=config.DATABASE_CHANNEL,
            message_id=db_msg.message_id,
            disable_notification=True
        )
        logger.info(f"Pinned token URL message {db_msg.message_id} in database channel")
        
    except Exception as e:
        logger.error(f"Failed to send/pin token URL in database channel: {e}")
    
    return token, verification_url

@cached(ttl=60, key_func=lambda token: f"verify_token_{token}")
async def verify_token(token):
    """Verify if a token is valid and not expired with caching"""
    try:
        db = get_db()
        user_id = await db.verify_token(token)
        return user_id
    except Exception as e:
        logger.error(f"Error verifying token: {e}")
        return None

@cached(ttl=300, key_func=lambda user_id: f"user_token_{user_id}")
async def check_user_token(user_id):
    """Check if a user has a valid token with caching"""
    try:
        db = get_db()
        return await db.check_user_token(user_id)
    except Exception as e:
        logger.error(f"Error checking user token: {e}")
        return False

async def refresh_token(context: CallbackContext):
    """Generate a new token only if no valid token exists or current token is about to expire"""
    logger.info("Scheduled token refresh triggered")
    
    try:
        db = get_db()
        token_info = await db.get_valid_token()
        
        if token_info:
            expiry = token_info["expiry"]
            current_time = datetime.utcnow()
            
            # If token still has more than 1 hour of validity, don't generate a new one
            if expiry - current_time > timedelta(hours=1):
                logger.info(f"Using existing valid token that expires at {expiry}")
                return
        
        # Clear cache for token generation
        cache.delete("token_gen_0")
        
        # No valid token found or token is about to expire, generate a new one
        logger.info("No valid token found or token is about to expire, generating a new one")
        await generate_token(0, context)
        
    except Exception as e:
        logger.error(f"Error in refresh_token: {e}")

# ========== COMMAND HANDLERS ========== #
@monitored
@rate_limited(max_requests=10, window_seconds=60)
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /start command with or without arguments"""
    user_id = update.effective_user.id
    args = context.args
    
    # Check ban status with caching
    cache_key = f"ban_status_{user_id}"
    is_banned = cache.get(cache_key)
    
    if is_banned is None:
        try:
            db = get_db()
            is_banned = await db.is_user_banned(user_id)
            cache.set(cache_key, is_banned, ttl=300)  # Cache for 5 minutes
        except Exception as e:
            logger.error(f"Error checking ban status: {e}")
            is_banned = False
    
    if is_banned:
        await update.message.reply_text(mikasa_reply('ban') + "You are banned from using this bot!")
        return
    
    # If there are arguments, it might be a file ID, batch ID, or token
    if args:
        arg = args[0]
        
        # Check if it's a token verification
        if arg.startswith("verify_"):
            token = arg[7:]  # Remove "verify_" prefix
            logger.info(f"Verifying token: {token} for user {user_id}")
            
            verified_user_id = await verify_token(token)
            
            if verified_user_id is not None and (verified_user_id == user_id or verified_user_id == 0):
                # Clear user token cache
                cache.delete(f"user_token_{user_id}")
                
                await update.message.reply_text(
                    mikasa_reply('success') + "Token verified successfully! You now have access for 24 hours."
                )
                return
            else:
                await update.message.reply_text(
                    mikasa_reply('warning') + "Invalid or expired token. Please get a new token."
                )
                return
        
        # Check if user has a valid token (only if token verification is enabled)
        if config.TOKEN_VERIFICATION_ENABLED:
            has_valid_token = await check_user_token(user_id)
            
            if not has_valid_token:
                keyboard = []
                if config.GET_TOKEN and config.GET_TOKEN.startswith(('http://', 'https://')):
                    keyboard.append([InlineKeyboardButton("Get Token", url=config.GET_TOKEN)])
                else:
                    _, verification_url = await generate_token(user_id, context)
                    keyboard.append([InlineKeyboardButton("Get Token", url=verification_url)])
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await update.message.reply_text(
                    mikasa_reply('warning') + "You need to verify access to use this bot.\n\n"
                    "Click the button below to get a 24-hour access token:",
                    reply_markup=reply_markup
                )
                return
        
        # If we get here, user has a valid token, proceed with file/batch handling
        await send_file(update, context)
        return
    
    # No arguments, show welcome message
    welcome_message = (
        f"{mikasa_reply('greeting')}Welcome to the Optimized File Sharing Bot!\n\n"
        "Use this bot to access shared files and batches.\n\n"
        "Available commands:\n"
        "/menu - Show main menu\n"
        "/help - Show help information"
    )
    
    # Check token verification
    if config.TOKEN_VERIFICATION_ENABLED:
        has_valid_token = await check_user_token(user_id)
        
        if not has_valid_token:
            keyboard = []
            if config.GET_TOKEN and config.GET_TOKEN.startswith(('http://', 'https://')):
                keyboard.append([InlineKeyboardButton("Get Token", url=config.GET_TOKEN)])
            else:
                _, verification_url = await generate_token(user_id, context)
                keyboard.append([InlineKeyboardButton("Get Token", url=verification_url)])
            
            keyboard.append([InlineKeyboardButton("📋 Main Menu", callback_data="menu")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                welcome_message + "\n\nYou need to verify access to use this bot.\n"
                "Click the button below to get a 24-hour access token:",
                reply_markup=reply_markup
            )
            return
    
    # User has valid token or verification disabled
    keyboard = [[InlineKeyboardButton("📋 Main Menu", callback_data="menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(welcome_message, reply_markup=reply_markup)

@owner_only
@monitored
async def token_toggle_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle token verification on/off"""
    try:
        # Toggle the setting
        new_value = not config.TOKEN_VERIFICATION_ENABLED
        config.TOKEN_VERIFICATION_ENABLED = new_value
        
        # Store in database
        db = get_db()
        await db.set_system_value("token_verification_enabled", new_value)
        
        status = "enabled" if new_value else "disabled"
        await update.message.reply_text(
            mikasa_reply('success') + f"Token verification has been {status}."
        )
    except Exception as e:
        logger.error(f"Error updating token verification setting: {e}")
        await update.message.reply_text(
            mikasa_reply('error') + f"Failed to update token verification setting. Error: {str(e)}"
        )

@owner_only
@monitored
async def performance_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show performance statistics"""
    try:
        stats = get_performance_stats()
        
        perf = stats['performance']
        cache_stats = stats['cache']
        
        stats_text = (
            f"{mikasa_reply('info')}📊 Performance Statistics:\n\n"
            f"🕐 Uptime: {perf['uptime_seconds']:.0f} seconds\n"
            f"📈 Total Requests: {perf['requests_total']}\n"
            f"⚡ Requests/Second: {perf['requests_per_second']:.2f}\n"
            f"⏱️ Avg Response Time: {perf['average_response_time_ms']:.2f}ms\n"
            f"❌ Error Rate: {perf['error_rate_percent']:.2f}%\n"
            f"👥 Active Users: {perf['active_users_count']}\n"
            f"🗄️ Database Queries: {perf['database_queries']}\n"
            f"💾 Cache Hit Rate: {perf['cache_hit_rate_percent']:.2f}%\n"
            f"🧠 Memory Usage: {perf['memory_usage_mb']:.2f}MB\n\n"
            f"Cache Size: {cache_stats['size']}/{cache_stats['max_size']}"
        )
        
        await update.message.reply_text(stats_text)
        
    except Exception as e:
        logger.error(f"Error getting performance stats: {e}")
        await update.message.reply_text(mikasa_reply('error') + "Failed to get performance statistics!")

# ========== FILE HANDLING ========== #
@admin_only
@monitored
async def start_batch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['batch'] = []
    logger.info(f"Started new batch for user {update.effective_user.id}")
    await update.message.reply_text(mikasa_reply('success') + "Batch collection started!")

@admin_only
@monitored
async def end_batch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'batch' not in context.user_data or not context.user_data['batch']:
        await update.message.reply_text(mikasa_reply('warning') + "No active batch!")
        return
    
    batch_files = context.user_data['batch']
    batch_id = str(uuid.uuid4())
    
    try:
        # Store batch in database
        db = get_db()
        success = await db.save_batch(
            batch_id=batch_id,
            files=batch_files,
            created_by=update.effective_user.id
        )
        
        if not success:
            await update.message.reply_text(mikasa_reply('error') + "Failed to save batch!")
            return
        
        batch_link = f"t.me/{context.bot.username}?start={batch_id}"
        
        # Store batch link in links channel if configured
        if config.LINKS_CHANNEL:
            try:
                date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                files_str = ", ".join(batch_files)
                if len(files_str) > 100:
                    files_str = files_str[:97] + "..."
                
                link_msg = await context.bot.send_message(
                    chat_id=config.LINKS_CHANNEL,
                    text=f"🔗 Batch Link\n\n"
                         f"ID: {batch_id}\n"
                         f"Files: {files_str}\n"
                         f"Date: {date_str}\n"
                         f"Total Files: {len(batch_files)}\n"
                         f"Link: {batch_link}\n\n"
                         f"#batch_{batch_id}\n"
                         f"#batch_files_{','.join(batch_files)}"
                )
                
                logger.info(f"Stored batch link in links channel, message ID: {link_msg.message_id}")
                
            except Exception as e:
                logger.error(f"Failed to store batch link in links channel: {e}")
        
        await update.message.reply_text(
            mikasa_reply('success') + f"Batch stored!\nShare link:\n{batch_link}"
        )
        context.user_data.pop('batch')
        
    except Exception as e:
        logger.error(f"Error saving batch: {e}")
        await update.message.reply_text(mikasa_reply('error') + "Failed to save batch!")

@admin_only
@monitored
async def rename_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Command to rename the next file to be stored"""
    context.user_data['awaiting_rename'] = True
    await update.message.reply_text(
        mikasa_reply('info') + "Please enter the new name for the next file:"
    )

@admin_only
@monitored
async def store_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    
    # Check if this is a text message for rename
    if 'awaiting_rename' in context.user_data and context.user_data['awaiting_rename']:
        if hasattr(update.message, 'text') and update.message.text and not update.message.text.startswith('/'):
            new_name = update.message.text
            context.user_data['custom_filename'] = new_name
            context.user_data['awaiting_rename'] = False
            
            await update.message.reply_text(
                mikasa_reply('success') + f"Next file will be renamed to: {new_name}\n\nNow send the file."
            )
            return
    
    # Regular file storage
    file_id = str(uuid.uuid4())
    custom_filename = context.user_data.pop('custom_filename', None)
    
    try:
        # Forward the message to the database channel
        msg = await context.bot.forward_message(
            chat_id=config.DATABASE_CHANNEL,
            from_chat_id=update.message.chat_id,
            message_id=update.message.message_id
        )
        
        file_link = f"t.me/{context.bot.username}?start={file_id}"
        
        # Store file metadata in database
        db = get_db()
        success = await db.save_file(
            file_id=file_id,
            message_id=msg.message_id,
            custom_name=custom_filename or "",
            media_type=get_media_type(update.message),
            caption=update.message.caption or "",
            file_link=file_link,
            created_by=update.effective_user.id
        )
        
        if not success:
            await update.message.reply_text(mikasa_reply('error') + "Failed to store file!")
            return
        
        # Store complete file metadata in links channel if configured
        if config.LINKS_CHANNEL:
            try:
                media_type = get_media_type(update.message)
                caption = update.message.caption or ""
                date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                link_msg = await context.bot.send_message(
                    chat_id=config.LINKS_CHANNEL,
                    text=f"🔗 File Link\n\n"
                         f"ID: {file_id}\n"
                         f"Name: {custom_filename if custom_filename else 'Unnamed file'}\n"
                         f"Type: {media_type}\n"
                         f"Date: {date_str}\n"
                         f"Caption: {caption}\n"
                         f"Message ID: {msg.message_id}\n\n"
                         f"Link: {file_link}\n\n"
                         f"#file_{file_id}"
                )
                
                logger.info(f"Stored file metadata in links channel, message ID: {link_msg.message_id}")
                
            except Exception as e:
                logger.error(f"Failed to store file metadata in links channel: {e}")
        
        # Handle batch
        if 'batch' in context.user_data:
            context.user_data['batch'].append(file_id)
            reply_text = "File added to batch! Send more or /lastbatch"
        else:
            reply_text = f"File stored!\nLink: {file_link}"
        
        await update.message.reply_text(mikasa_reply('success') + reply_text)
        
    except Exception as e:
        logger.error(f"Error storing file: {e}")
        await update.message.reply_text(mikasa_reply('error') + "Failed to store file!")

def get_media_type(message):
    """Determine the type of media in a message"""
    if message.photo:
        return "photo"
    elif message.video:
        return "video"
    elif message.audio:
        return "audio"
    elif message.document:
        return "document"
    elif message.animation:
        return "animation"
    elif message.voice:
        return "voice"
    elif message.video_note:
        return "video_note"
    elif message.sticker:
        return "sticker"
    else:
        return "unknown"

@monitored
@rate_limited(max_requests=20, window_seconds=60)
async def send_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    file_id = context.args[0] if context.args else None
    
    # Check ban status with caching
    cache_key = f"ban_status_{user_id}"
    is_banned = cache.get(cache_key)
    
    if is_banned is None:
        try:
            db = get_db()
            is_banned = await db.is_user_banned(user_id)
            cache.set(cache_key, is_banned, ttl=300)
        except Exception as e:
            logger.error(f"Error checking ban status: {e}")
            is_banned = False
    
    if is_banned:
        await update.message.reply_text(mikasa_reply('ban') + "Banned!")
        return
    
    # Check token verification
    if config.TOKEN_VERIFICATION_ENABLED:
        has_valid_token = await check_user_token(user_id)
        
        if not has_valid_token:
            keyboard = []
            if config.GET_TOKEN and config.GET_TOKEN.startswith(('http://', 'https://')):
                keyboard.append([InlineKeyboardButton("Get Token", url=config.GET_TOKEN)])
            else:
                _, verification_url = await generate_token(user_id, context)
                keyboard.append([InlineKeyboardButton("Get Token", url=verification_url)])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                mikasa_reply('warning') + "You need to verify access to use this bot.\n\n"
                "Click the button below to get a 24-hour access token:",
                reply_markup=reply_markup
            )
            return
    
    # Force subscription check
    if config.FORCE_SUB != 0:
        try:
            member = await context.bot.get_chat_member(config.FORCE_SUB, user_id)
            if member.status not in ['member', 'administrator', 'creator']:
                await update.message.reply_text(
                    mikasa_reply('warning') + "Join channel first!",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                        "Join Channel", url=f"t.me/{config.FORCE_SUB}"
                    )]])
                )
                return
        except Exception as e:
            logger.error(f"Force sub error: {e}")
    
    if not file_id:
        await update.message.reply_text(mikasa_reply('warning') + "No file ID provided!")
        return
    
    try:
        db = get_db()
        
        # Try to get file from database (with caching via get_file method)
        file_data = await db.get_file(file_id)
        
        if file_data:
            # Single file found
            message_id = file_data.get("message_id")
            if not message_id:
                await update.message.reply_text(mikasa_reply('warning') + "Invalid file data!")
                return
            
            custom_name = file_data.get("custom_name")
            caption = f"{custom_name}" if custom_name else None
            
            try:
                sent_msg = await context.bot.copy_message(
                    chat_id=update.effective_chat.id,
                    from_chat_id=config.DATABASE_CHANNEL,
                    message_id=message_id,
                    caption=caption,
                    protect_content=True
                )
                logger.info(f"Sent file {file_id} to user {user_id}")
                
            except Exception as e:
                logger.error(f"Error sending file: {e}")
                await update.message.reply_text(mikasa_reply('error') + "Failed to send file!")
        else:
            # File not found, check if it's a batch
            batch_data = await db.get_batch(file_id)
            
            if batch_data:
                batch_files = batch_data.get("files", [])
                
                if not batch_files:
                    await update.message.reply_text(mikasa_reply('warning') + "Invalid batch data!")
                    return
                
                sent_messages = []
                missing_files = []
                
                # Process files in smaller batches to avoid overwhelming
                batch_size = 5
                for i in range(0, len(batch_files), batch_size):
                    batch_chunk = batch_files[i:i + batch_size]
                    
                    # Process chunk in parallel
                    tasks = []
                    for fid in batch_chunk:
                        tasks.append(db.get_file(fid))
                    
                    file_results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for fid, file_data in zip(batch_chunk, file_results):
                        if isinstance(file_data, Exception):
                            logger.error(f"Error getting batch file {fid}: {file_data}")
                            missing_files.append(fid)
                            continue
                        
                        if file_data:
                            message_id = file_data.get("message_id")
                            if not message_id:
                                missing_files.append(fid)
                                continue
                            
                            custom_name = file_data.get("custom_name")
                            caption = f"{custom_name}" if custom_name else None
                            
                            try:
                                sent_msg = await context.bot.copy_message(
                                    chat_id=update.effective_chat.id,
                                    from_chat_id=config.DATABASE_CHANNEL,
                                    message_id=message_id,
                                    caption=caption,
                                    protect_content=True
                                )
                                sent_messages.append(sent_msg.message_id)
                                
                            except Exception as e:
                                logger.error(f"Error sending batch file {fid}: {e}")
                                missing_files.append(fid)
                        else:
                            missing_files.append(fid)
                    
                    # Small delay between batches
                    if i + batch_size < len(batch_files):
                        await asyncio.sleep(0.5)
                
                # Notify about missing files
                if missing_files:
                    await update.message.reply_text(
                        mikasa_reply('warning') + f"Some files in this batch ({len(missing_files)} of {len(batch_files)}) could not be found."
                    )
                
                if not sent_messages:
                    await update.message.reply_text(mikasa_reply('warning') + "No valid files in batch!")
            else:
                await update.message.reply_text(mikasa_reply('warning') + "File or batch not found!")
                
    except Exception as e:
        logger.error(f"Error in send_file: {e}")
        await update.message.reply_text(mikasa_reply('error') + "An error occurred while processing your request!")

# ========== ADMIN COMMANDS ========== #
@admin_only
@monitored
async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(mikasa_reply('warning') + "Provide user ID!")
        return
    
    try:
        user_id = int(context.args[0])
        reason = " ".join(context.args[1:]) if len(context.args) > 1 else ""
        
        db = get_db()
        success = await db.ban_user(user_id, reason)
        
        if success:
            # Clear cache for this user
            cache.delete(f"ban_status_{user_id}")
            cache.delete(f"user_token_{user_id}")
            
            await update.message.reply_text(mikasa_reply('ban') + f"Banned {user_id}!")
        else:
            await update.message.reply_text(mikasa_reply('warning') + "Already banned!")
            
    except ValueError:
        await update.message.reply_text(mikasa_reply('warning') + "Invalid ID!")
    except Exception as e:
        logger.error(f"Error banning user: {e}")
        await update.message.reply_text(mikasa_reply('error') + "Failed to ban user!")

@admin_only
@monitored
async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(mikasa_reply('warning') + "Provide user ID!")
        return
    
    try:
        user_id = int(context.args[0])
        
        db = get_db()
        success = await db.unban_user(user_id)
        
        if success:
            # Clear cache for this user
            cache.delete(f"ban_status_{user_id}")
            
            await update.message.reply_text(mikasa_reply('unban') + f"Unbanned {user_id}!")
        else:
            await update.message.reply_text(mikasa_reply('warning') + "User not banned!")
            
    except ValueError:
        await update.message.reply_text(mikasa_reply('warning') + "Invalid ID!")
    except Exception as e:
        logger.error(f"Error unbanning user: {e}")
        await update.message.reply_text(mikasa_reply('error') + "Failed to unban user!")

@admin_only
@monitored
async def list_banned(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        db = get_db()
        banned_users = await db.get_banned_users()
        
        if banned_users:
            await update.message.reply_text(mikasa_reply('info') + f"Banned users: {', '.join(map(str, banned_users))}")
        else:
            await update.message.reply_text(mikasa_reply('info') + "No banned users!")
            
    except Exception as e:
        logger.error(f"Error listing banned users: {e}")
        await update.message.reply_text(mikasa_reply('error') + "Failed to list banned users!")

@admin_only
@monitored
async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    settings_msg = f"""
{mikasa_reply('info')}⚙️ Current Settings:
• Force Sub: {config.FORCE_SUB if config.FORCE_SUB else 'Disabled'}
• Admins: {len(config.ADMINS)} configured
• Token Duration: {config.TOKEN_DURATION} hours
• Token Verification: {'Enabled' if config.TOKEN_VERIFICATION_ENABLED else 'Disabled'}
• Database: MongoDB (Optimized)
• Links Channel: {'Configured' if config.LINKS_CHANNEL else 'Not configured'}
• Cache TTL: {config.CACHE_TTL} seconds
• Rate Limit: {config.RATE_LIMIT_REQUESTS} requests per {config.RATE_LIMIT_WINDOW} seconds
"""
    await update.message.reply_text(settings_msg)

@admin_only
@monitored
async def restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(mikasa_reply('default') + "Rebooting...")
    await cleanup_resources()
    await close_database()
    os.execv(sys.executable, [sys.executable] + sys.argv)

# ========== SEARCH FUNCTIONALITY ========== #
@monitored
@rate_limited(max_requests=15, window_seconds=60)
async def search_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search for files by keywords or date with caching"""
    if not context.args:
        await update.message.reply_text(
            mikasa_reply('warning') + "Please provide search keywords.\n\n"
            "Examples:\n"
            "/search anime\n"
            "/search date:2025-01-07"
        )
        return
    
    search_query = ' '.join(context.args).lower()
    
    # Update group stats if in a group
    if update.effective_chat.type in ["group", "supergroup"]:
        try:
            db = get_db()
            await db.update_group_stats(
                chat_id=update.effective_chat.id,
                action_type="search",
                user_id=update.effective_user.id,
                search_term=search_query
            )
        except Exception as e:
            logger.error(f"Error updating group stats: {e}")
    
    # Parse search query
    date_search = None
    if search_query.startswith("date:"):
        date_pattern = r"date:(\d{4}-\d{2}-\d{2})"
        match = re.search(date_pattern, search_query)
        if match:
            date_search = match.group(1)
            search_query = search_query.replace(f"date:{date_search}", "").strip()
    
    # Check cache first
    cache_key = f"search_{hash(search_query + str(date_search))}"
    cached_result = cache.get(cache_key)
    
    if cached_result:
        matching_files = cached_result
        logger.debug(f"Using cached search results for query: {search_query}")
    else:
        try:
            db = get_db()
            matching_files = await db.search_files(
                query=search_query if search_query else None,
                date_filter=date_search,
                limit=10
            )
            
            # Cache results for 5 minutes
            cache.set(cache_key, matching_files, ttl=300)
            
        except Exception as e:
            logger.error(f"Error searching files: {e}")
            await update.message.reply_text(
                mikasa_reply('error') + "An error occurred while searching for files."
            )
            return
    
    if not matching_files:
        await update.message.reply_text(
            mikasa_reply('info') + "No files found matching your search."
        )
        return
    
    # Create response with inline keyboard
    response_text = f"{mikasa_reply('success')}🔍 Search Results:\n\n"
    keyboard = []
    
    for i, file in enumerate(matching_files):
        file_id = file["file_id"]
        name = file.get("custom_name", "Unnamed file")
        media_type = file.get("media_type", "unknown")
        
        media_icon = get_media_icon(media_type)
        response_text += f"{i+1}. {media_icon} {name}\n"
        
        file_link = f"t.me/{context.bot.username}?start={file_id}"
        keyboard.append([InlineKeyboardButton(f"📄 File {i+1}", url=file_link)])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(response_text, reply_markup=reply_markup)

def get_media_icon(media_type):
    """Return an appropriate icon for the media type"""
    icons = {
        "photo": "🖼️",
        "video": "🎬",
        "audio": "🎵",
        "document": "📄",
        "animation": "🎭",
        "voice": "🎤",
        "video_note": "⭕",
        "sticker": "🏷️"
    }
    return icons.get(media_type, "📁")

# ========== GROUP FEATURES ========== #
@monitored
async def group_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show group statistics with caching"""
    if update.effective_chat.type not in ["group", "supergroup"]:
        await update.message.reply_text(
            mikasa_reply('warning') + "This command can only be used in group chats."
        )
        return
    
    try:
        # Check cache first
        cache_key = f"group_stats_{update.effective_chat.id}"
        cached_stats = cache.get(cache_key)
        
        if cached_stats:
            group_settings = cached_stats
        else:
            db = get_db()
            group_settings = await db.get_group_settings(update.effective_chat.id)
            # Cache for 2 minutes
            cache.set(cache_key, group_settings, ttl=120)
        
        stats_text = (
            f"{mikasa_reply('info')}📊 Group Statistics:\n\n"
            f"• Total Files Shared: {group_settings.get('total_files_shared', 0)}\n"
            f"• Total Searches: {group_settings.get('total_searches', 0)}\n"
            f"• Active Members: {len(group_settings.get('active_members', {}))}\n"
            f"• Auto-Delete Setting: {group_settings.get('auto_delete_minutes', 0)} minutes\n"
            f"• Last Activity: {group_settings.get('last_activity', 'Never')}"
        )
        
        await update.message.reply_text(stats_text)
        
    except Exception as e:
        logger.error(f"Error getting group stats: {e}")
        await update.message.reply_text(mikasa_reply('error') + "Failed to get group statistics!")

# ========== UI HANDLERS ========== #
@monitored
async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show the main menu"""
    keyboard = [
        [InlineKeyboardButton("📚 Help", callback_data="help")],
        [InlineKeyboardButton("ℹ️ About", callback_data="about")],
        [InlineKeyboardButton("🔍 Search Files", callback_data="search_menu")]
    ]
    
    if config.is_admin(update.effective_user.id):
        keyboard.extend([
            [InlineKeyboardButton("🔄 Start Batch", callback_data="start_batch"),
             InlineKeyboardButton("✅ End Batch", callback_data="end_batch")],
            [InlineKeyboardButton("✏️ Rename File", callback_data="rename_file")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="settings")]
        ])
    
    if config.is_owner(update.effective_user.id):
        keyboard.append([
            InlineKeyboardButton("📊 Performance", callback_data="performance"),
            InlineKeyboardButton("🛠️ Admin Panel", callback_data="admin_panel")
        ])
    
    if update.effective_chat.type in ["group", "supergroup"]:
        keyboard.append([InlineKeyboardButton("📊 Group Stats", callback_data="group_stats")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(mikasa_reply('info') + "Main Menu:", reply_markup=reply_markup)

@monitored
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button callbacks"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "performance" and config.is_owner(query.from_user.id):
        # Show performance stats
        stats = get_performance_stats()
        perf = stats['performance']
        
        stats_text = (
            f"📊 Performance Stats:\n\n"
            f"⚡ RPS: {perf['requests_per_second']:.2f}\n"
            f"⏱️ Avg Response: {perf['average_response_time_ms']:.2f}ms\n"
            f"👥 Active Users: {perf['active_users_count']}\n"
            f"💾 Cache Hit Rate: {perf['cache_hit_rate_percent']:.2f}%\n"
            f"🧠 Memory: {perf['memory_usage_mb']:.2f}MB"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(stats_text, reply_markup=reply_markup)
        return
    
    # Handle other standard menu options
    if query.data == "menu":
        keyboard = [
            [InlineKeyboardButton("📚 Help", callback_data="help")],
            [InlineKeyboardButton("ℹ️ About", callback_data="about")],
            [InlineKeyboardButton("🔍 Search Files", callback_data="search_menu")]
        ]
        
        if config.is_admin(query.from_user.id):
            keyboard.extend([
                [InlineKeyboardButton("🔄 Start Batch", callback_data="start_batch"),
                 InlineKeyboardButton("✅ End Batch", callback_data="end_batch")],
                [InlineKeyboardButton("✏️ Rename File", callback_data="rename_file")],
                [InlineKeyboardButton("⚙️ Settings", callback_data="settings")]
            ])
        
        if config.is_owner(query.from_user.id):
            keyboard.append([
                InlineKeyboardButton("📊 Performance", callback_data="performance"),
                InlineKeyboardButton("🛠️ Admin Panel", callback_data="admin_panel")
            ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(mikasa_reply('info') + "Main Menu:", reply_markup=reply_markup)
    
    elif query.data == "help":
        help_text = (
            f"{mikasa_reply('info')}Help Information:\n\n"
            "• To access a file, use the provided link\n"
            "• You need a valid token to access files\n"
            "• Use /search <keywords> to find files\n\n"
            "Commands:\n"
            "/start - Start the bot\n"
            "/menu - Show main menu\n"
            "/help - Show this help\n"
            "/search - Search for files"
        )
        
        if config.is_admin(query.from_user.id):
            help_text += (
                "\n\nAdmin Commands:\n"
                "/getlink - Store a file\n"
                "/firstbatch - Start batch\n"
                "/lastbatch - End batch\n"
                "/rename - Rename next file\n"
                "/ban <user_id> - Ban user\n"
                "/unban <user_id> - Unban user\n"
                "/settings - Show settings\n"
                "/restart - Restart bot"
            )
        
        if config.is_owner(query.from_user.id):
            help_text += (
                "\n\nOwner Commands:\n"
                "/tokentoggle - Toggle token verification\n"
                "/performance - Show performance stats"
            )
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(help_text, reply_markup=reply_markup)
    
    elif query.data == "about":
        about_text = (
            f"{mikasa_reply('info')}About This Bot:\n\n"
            "High-performance file sharing bot with MongoDB integration.\n\n"
            "Features:\n"
            "• MongoDB storage with connection pooling\n"
            "• Advanced caching system\n"
            "• Rate limiting and performance monitoring\n"
            "• Token verification system\n"
            "• Batch file sharing\n"
            "• Optimized for thousands of concurrent users\n"
            "• Real-time performance statistics"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(about_text, reply_markup=reply_markup)

@monitored
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help information"""
    help_text = (
        f"{mikasa_reply('info')}Help Information:\n\n"
        "• To access a file, use the provided link\n"
        "• You need a valid token to access files\n"
        "• Use /search <keywords> to find files\n\n"
        "Commands:\n"
        "/start - Start the bot\n"
        "/menu - Show main menu\n"
        "/help - Show this help\n"
        "/search - Search for files"
    )
    
    if config.is_admin(update.effective_user.id):
        help_text += (
            "\n\nAdmin Commands:\n"
            "/getlink - Store a file\n"
            "/firstbatch - Start batch\n"
            "/lastbatch - End batch\n"
            "/rename - Rename next file\n"
            "/ban <user_id> - Ban user\n"
            "/unban <user_id> - Unban user\n"
            "/settings - Show settings\n"
            "/restart - Restart bot"
        )
    
    if config.is_owner(update.effective_user.id):
        help_text += (
            "\n\nOwner Commands:\n"
            "/tokentoggle - Toggle token verification\n"
            "/performance - Show performance stats"
        )
    
    keyboard = [[InlineKeyboardButton("📋 Main Menu", callback_data="menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(help_text, reply_markup=reply_markup)

# ========== MESSAGE HANDLER ========== #
@monitored
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all messages"""
    if not update.message:
        return
    
    # Skip command messages
    if hasattr(update.message, 'text') and update.message.text and update.message.text.startswith('/'):
        return
    
    # In group chats, don't respond to regular messages
    if update.effective_chat.type in ["group", "supergroup"]:
        return
    
    # Only process file storage in private chats
    if update.effective_chat.type == "private":
        if config.is_admin(update.effective_user.id):
            await store_file(update, context)
        else:
            await update.message.reply_text(
                mikasa_reply('info') + "Use /menu to access the bot menu or /help for assistance."
            )

# ========== APPLICATION SETUP ========== #
async def post_init(application):
    """Initialize database and schedule tasks"""
    logger.info("Initializing optimized bot...")
    
    # Initialize database connection
    mongodb_config = config.get_mongodb_config()
    success = await init_database(mongodb_config["uri"], mongodb_config["database"])
    if not success:
        logger.error("Failed to initialize database connection!")
        # sys.exit(1) # Commented out to allow bot to continue running even if DB connection fails initially
    # Start performance optimization background tasks
    await cache.start()
    await user_rate_limiter.start()    
    # Preload cache
    await preload_cache()
    
    # Schedule token refresh
    application.job_queue.run_repeating(
        refresh_token,
        interval=config.TOKEN_DURATION * 3600 / 2,
        first=60,
        name="token_refresh"
    )
    
    # Generate initial token if needed
    db = get_db()
    token_info = await db.get_valid_token()
    
    if not token_info:
        logger.info("Generating initial token")
        await generate_token(0, application)
    
    logger.info("Optimized bot initialization completed successfully")

async def shutdown(application):
    """Cleanup on shutdown"""
    logger.info("Shutting down optimized bot...")
    await cleanup_resources()
    await close_database()
    logger.info("Bot shutdown completed")

if __name__ == "__main__":
    # Initialize application
    application = ApplicationBuilder().token(config.BOT_TOKEN).post_init(post_init).build()
    
    # Register handlers
    handlers = [
        CommandHandler("start", start_command),
        CommandHandler("menu", menu_command),
        CommandHandler("help", help_command),
        CommandHandler("getlink", store_file),
        CommandHandler("firstbatch", start_batch),
        CommandHandler("lastbatch", end_batch),
        CommandHandler("rename", rename_file),
        CommandHandler("ban", ban_user),
        CommandHandler("unban", unban_user),
        CommandHandler("listbanned", list_banned),
        CommandHandler("settings", settings_command),
        CommandHandler("restart", restart),
        CommandHandler("search", search_files),
        CommandHandler("groupstats", group_stats_command),
        CommandHandler("tokentoggle", token_toggle_command),
        CommandHandler("performance", performance_stats_command),
        CallbackQueryHandler(button_handler),
        MessageHandler(filters.ALL & ~filters.COMMAND, message_handler)
    ]
    
    for handler in handlers:
        application.add_handler(handler)
    
    application.add_error_handler(error_handler)
    
    print("⚔️ TATAKAE - Optimized High-Performance Bot Starting")
    print(f"Configuration: {config}")
    
    try:
        application.run_polling(drop_pending_updates=True)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.critical(f"Critical error: {e}")
    finally:
        # Cleanup
        asyncio.run(shutdown(application))