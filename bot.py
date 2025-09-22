import os
import logging
import time
from decimal import Decimal
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.constants import ParseMode
from supabase import create_client, Client
import re
from web3 import Web3
import json
from dotenv import load_dotenv
import asyncio

# Load environment variables from .env file
load_dotenv()

try:
    import gotrue._sync.gotrue_base_api as gbase

    _old_init = gbase.SyncClient.__init__

    def _new_init(self, *args, proxy=None, **kwargs):
        # Drop the proxy arg, forward everything else
        return _old_init(self, *args, **kwargs)

    gbase.SyncClient.__init__ = _new_init
    print("✅ Patched gotrue to ignore proxy argument")

except Exception as e:
    print("⚠️ Failed to patch gotrue:", e)

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 6950876107))

# BSC Testnet Configuration
BSC_TESTNET_URL = "https://data-seed-prebsc-1-s1.binance.org:8545/"
BSC_NODE_URL = os.getenv("BSC_NODE_URL", BSC_TESTNET_URL)
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS")
ADMIN_PRIVATE_KEY = os.getenv("ADMIN_PRIVATE_KEY")

# Required groups - these will be checked for membership
REQUIRED_GROUPS = [
    -1003083388928,  # Group 1
    -1003095619576,  # Group 2  
    -1002257059748   # Group 3
]

# Group names for display purposes
GROUP_NAMES = {
    -1003083388928: "MetaCore Airdrop Chat",
    -1003095619576: "MetaCore Airdrop News",
    -1002257059748: "Bot News"
}

# Group URLs for joining
GROUP_URLS = {
    -1003083388928: "https://t.me/MetaAirdropchat",
    -1003095619576: "https://t.me/metaairdropnews",
    -1002257059748: "https://t.me/botnewz1"
}

# Initialize clients
supabase = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None
w3 = Web3(Web3.HTTPProvider(BSC_NODE_URL)) if BSC_NODE_URL else None

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Disable httpx logging to prevent token leakage
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)

# Keyboards
MAIN_KEYBOARD = ReplyKeyboardMarkup([
    ['🔗 Referral Link', '💰 Balance'],
    ['💳 Set Wallet', '🏦 Withdraw'],
    ['👤 My Profile', '❓ Help']
], resize_keyboard=True)

GROUPS_KEYBOARD = ReplyKeyboardMarkup([
    ['✅ I\'ve Joined All Groups'],
    ['🔙 Back to Menu']
], resize_keyboard=True)

ADMIN_KEYBOARD = ReplyKeyboardMarkup([
    ['📊 Stats', '📢 Broadcast'],
    ['👤 User Info', '💰 Add Balance'],
    ['🏦 Withdrawals', '⚙️ Settings'],
    ['🌐 Network Info', '👥 Group Check']
], resize_keyboard=True)

# User states
user_states = {}

class UserState:
    MAIN = "main"
    JOINING_GROUPS = "joining_groups"
    SETTING_TELEGRAM = "setting_telegram"
    SETTING_TWITTER = "setting_twitter"
    SETTING_WALLET = "setting_wallet"
    WITHDRAWING = "withdrawing"
    ADMIN_MENU = "admin_menu"
    ADMIN_BROADCAST = "admin_broadcast"
    ADMIN_USER_INFO = "admin_user_info"
    ADMIN_ADD_BALANCE = "admin_add_balance"

# Anti-spam and security features
user_last_action = {}
RATE_LIMIT_SECONDS = 2

def rate_limit_check(user_id):
    """Check if user is rate limited"""
    now = time.time()
    if user_id in user_last_action:
        if now - user_last_action[user_id] < RATE_LIMIT_SECONDS:
            return False
    user_last_action[user_id] = now
    return True

def escape_markdown_v2(text):
    """Escape special characters for Markdown V2"""
    if text is None:
        return "N/A"
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{char}' if char in escape_chars else char for char in str(text))

# Helper functions
def get_user(user_id):
    if not supabase:
        return None
    try:
        result = supabase.table('users').select('*').eq('id', user_id).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        logger.error(f"Error getting user {user_id}: {e}")
        return None

def get_settings():
    if not supabase:
        return {
            'signup_bonus': 1000,
            'referral_bonus': 4000,
            'group_join_bonus': 500,
            'min_withdraw_amount': 4000
        }
    try:
        result = supabase.table('settings').select('*').execute()
        if result.data:
            return result.data[0]
        else:
            return {
                'signup_bonus': 1000,
                'referral_bonus': 4000,
                'group_join_bonus': 500,
                'min_withdraw_amount': 4000
            }
    except Exception as e:
        logger.error(f"Error getting settings: {e}")
        return {
            'signup_bonus': 1000,
            'referral_bonus': 4000,
            'group_join_bonus': 500,
            'min_withdraw_amount': 4000
        }

def create_user(user_id, username, full_name, invited_by=None):
    if not supabase:
        return None
    try:
        settings = get_settings()
        signup_bonus = settings['signup_bonus']
        
        user_data = {
            'id': user_id,
            'username': username,
            'full_name': full_name,
            'invited_by': invited_by,
            'balance': signup_bonus,
            'joined_all_groups': False,
            'telegram_handle': username,
            'twitter_handle': None,
            'has_received_signup_bonus': True,
            'has_received_group_bonus': False,
            'group_membership_status': {}  # Track individual group memberships
        }
        
        result = supabase.table('users').insert(user_data).execute()
        
        # Log signup transaction
        if result.data:
            supabase.table('transactions').insert({
                'user_id': user_id,
                'type': 'signup',
                'amount': str(signup_bonus),
                'description': 'Signup bonus'
            }).execute()
        
        # Credit referrer if exists
        if invited_by:
            credit_referrer(invited_by, user_id)
        
        return result.data[0] if result.data else None
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        return None

def credit_referrer(inviter_id, referred_id):
    if not supabase:
        return
    try:
        # Check if referral already exists
        existing = supabase.table('referrals').select('*').eq('inviter', inviter_id).eq('referred', referred_id).execute()
        if existing.data:
            return
        
        settings = get_settings()
        bonus = settings['referral_bonus']
        
        # Use the database function to add balance
        supabase.rpc('add_balance', {
            'user_id_param': inviter_id,
            'amount_param': str(bonus),
            'type_param': 'referral',
            'description_param': f'Referral bonus for user {referred_id}'
        }).execute()
        
        # Record referral
        supabase.table('referrals').insert({
            'inviter': inviter_id,
            'referred': referred_id,
            'bonus_credited': True
        }).execute()
        
    except Exception as e:
        logger.error(f"Error crediting referrer: {e}")

async def check_group_membership(context, user_id):
    """Check if user is member of ALL required groups - IMPROVED VERSION"""
    try:
        membership_status = {}
        failed_groups = []
        
        logger.info(f"Checking group membership for user {user_id}")
        
        for group_id in REQUIRED_GROUPS:
            try:
                member = await context.bot.get_chat_member(chat_id=group_id, user_id=user_id)
                group_name = GROUP_NAMES.get(group_id, f"Group {group_id}")
                
                # Check if user is actually a member
                if member.status in ['member', 'creator', 'administrator']:
                    membership_status[str(group_id)] = True
                    logger.info(f"User {user_id} is member of {group_name}")
                else:
                    membership_status[str(group_id)] = False
                    failed_groups.append(group_name)
                    logger.warning(f"User {user_id} is NOT a member of {group_name} (status: {member.status})")
                    
            except Exception as e:
                logger.error(f"Error checking group {group_id}: {e}")
                membership_status[str(group_id)] = False
                group_name = GROUP_NAMES.get(group_id, f"Group {group_id}")
                failed_groups.append(group_name)
        
        # Update user's membership status in database
        if supabase:
            supabase.table('users').update({
                'group_membership_status': membership_status
            }).eq('id', user_id).execute()
        
        # Return True only if user is member of ALL groups
        all_joined = all(membership_status.values())
        
        if not all_joined:
            logger.info(f"User {user_id} failed group check. Missing: {failed_groups}")
        else:
            logger.info(f"User {user_id} passed ALL group checks!")
        
        return all_joined, failed_groups
        
    except Exception as e:
        logger.error(f"Error in group membership check: {e}")
        return False, ["Error occurred during check"]

async def check_single_user_groups(context, user_id):
    """Admin function to check a single user's group membership"""
    try:
        membership_results = []
        
        for group_id in REQUIRED_GROUPS:
            try:
                member = await context.bot.get_chat_member(chat_id=group_id, user_id=user_id)
                group_name = GROUP_NAMES.get(group_id, f"Group {group_id}")
                
                status_emoji = "✅" if member.status in ['member', 'creator', 'administrator'] else "❌"
                membership_results.append(f"{status_emoji} {group_name}: {member.status}")
                
            except Exception as e:
                group_name = GROUP_NAMES.get(group_id, f"Group {group_id}")
                membership_results.append(f"❌ {group_name}: Error - {str(e)[:50]}...")
        
        return membership_results
        
    except Exception as e:
        logger.error(f"Error checking single user groups: {e}")
        return ["Error occurred during group check"]

def is_valid_bsc_address(address):
    """Validate BSC wallet address"""
    return re.match(r'^0x[a-fA-F0-9]{40}$', address) is not None

def is_valid_telegram_handle(handle):
    """Validate Telegram handle"""
    if handle.startswith('@'):
        handle = handle[1:]
    return re.match(r'^[a-zA-Z0-9_]{5,32}$', handle) is not None

def is_valid_twitter_handle(handle):
    """Validate Twitter handle"""
    if handle.startswith('@'):
        handle = handle[1:]
    return re.match(r'^[a-zA-Z0-9_]{1,15}$', handle) is not None

# Command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        user_id = user.id
        
        if not rate_limit_check(user_id):
            await update.message.reply_text("⚠️ Please wait before sending another command")
            return
        
        # Check if admin
        if user_id == ADMIN_ID:
            user_states[user_id] = UserState.ADMIN_MENU
            await update.message.reply_text("🔧 Admin Panel Access", reply_markup=ADMIN_KEYBOARD)
            return
        
        # Check for referral
        invited_by = None
        if context.args and context.args[0].startswith('ref'):
            try:
                invited_by = int(context.args[0][3:])
                if invited_by == user_id:
                    invited_by = None
            except ValueError:
                invited_by = None
        
        # Get or create user
        db_user = get_user(user_id)
        
        if not db_user:
            # New user - start onboarding process
            db_user = create_user(user_id, user.username, user.full_name, invited_by)
            if db_user:
                welcome_msg = "🎉 Welcome to MetaCore Airdrop!\n\n"
                welcome_msg += "✅ You received 1000 MetaCore signup bonus!\n\n"
                if invited_by:
                    welcome_msg += "🎁 Referral bonus credited to your referrer!\n\n"
                welcome_msg += "Let's get you set up! First, please provide your Telegram handle:"
                
                user_states[user_id] = UserState.SETTING_TELEGRAM
                await update.message.reply_text(welcome_msg)
                return
            else:
                await update.message.reply_text("❌ Error creating account. Please try again.")
                return
        else:
            # Existing user - check if they completed onboarding
            if not db_user.get('joined_all_groups', False):
                # User hasn't completed group joining
                if not db_user.get('telegram_handle') or not db_user.get('twitter_handle'):
                    # Complete profile setup first
                    if not db_user.get('telegram_handle'):
                        user_states[user_id] = UserState.SETTING_TELEGRAM
                        await update.message.reply_text("👋 Welcome back! Please provide your Telegram handle:")
                        return
                    elif not db_user.get('twitter_handle'):
                        user_states[user_id] = UserState.SETTING_TWITTER
                        await update.message.reply_text("👋 Welcome back! Please provide your Twitter handle:")
                        return
                else:
                    # Profile complete, need to join groups
                    await handle_join_groups(update, context)
                    return
            else:
                # User completed everything - show main menu
                welcome_msg = "👋 Welcome back to MetaCore Airdrop!\n\n"
                welcome_msg += "Choose an option below:"
                
                user_states[user_id] = UserState.MAIN
                await update.message.reply_text(welcome_msg, reply_markup=MAIN_KEYBOARD)
        
    except Exception as e:
        logger.error(f"Error in start command: {e}")
        await update.message.reply_text("❌ An error occurred. Please try again.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        text = update.message.text
        state = user_states.get(user_id, UserState.MAIN)
        
        if not rate_limit_check(user_id):
            await update.message.reply_text("⚠️ Please wait before sending another command")
            return
        
        # Handle admin states
        if user_id == ADMIN_ID:
            if state == UserState.ADMIN_BROADCAST:
                await process_admin_broadcast(update, context)
                return
            elif state == UserState.ADMIN_USER_INFO:
                await process_admin_user_info(update, context)
                return
            elif state == UserState.ADMIN_ADD_BALANCE:
                await process_admin_add_balance(update, context)
                return
            # Handle admin menu buttons
            elif text == '📊 Stats':
                await admin_stats(update, context)
                return
            elif text == '📢 Broadcast':
                user_states[user_id] = UserState.ADMIN_BROADCAST
                await update.message.reply_text("📢 Send the message to broadcast to all users:")
                return
            elif text == '👤 User Info':
                user_states[user_id] = UserState.ADMIN_USER_INFO
                await update.message.reply_text("👤 Send the user ID to get info:")
                return
            elif text == '💰 Add Balance':
                user_states[user_id] = UserState.ADMIN_ADD_BALANCE
                await update.message.reply_text("💰 Send: user_id amount\nExample: 123456789 1000")
                return
            elif text == '🏦 Withdrawals':
                await handle_withdrawals(update, context)
                return
            elif text == '⚙️ Settings':
                await handle_settings(update, context)
                return
            elif text == '🌐 Network Info':
                await handle_network_info(update, context)
                return
            elif text == '👥 Group Check':
                user_states[user_id] = UserState.ADMIN_USER_INFO
                await update.message.reply_text("👥 Send user ID to check their group membership:")
                return
        
        # Handle regular user states
        if state == UserState.SETTING_TELEGRAM:
            await process_telegram_handle(update, context)
            return
        elif state == UserState.SETTING_TWITTER:
            await process_twitter_handle(update, context)
            return
        elif state == UserState.SETTING_WALLET:
            await process_wallet_address(update, context)
            return
        elif state == UserState.WITHDRAWING:
            await process_withdrawal_amount(update, context)
            return
        
        # Handle menu buttons
        if text == '🔗 Referral Link':
            await handle_referral_link(update, context)
        elif text == '💰 Balance':
            await handle_balance(update, context)
        elif text == '💳 Set Wallet':
            await handle_set_wallet(update, context)
        elif text == '🏦 Withdraw':
            await handle_withdraw(update, context)
        elif text == '👤 My Profile':
            await handle_profile(update, context)
        elif text == '❓ Help':
            await handle_help(update, context)
        elif text == '✅ I\'ve Joined All Groups':
            await verify_group_membership(update, context)
        elif text == '🔙 Back to Menu':
            if user_id == ADMIN_ID:
                user_states[user_id] = UserState.ADMIN_MENU
                await update.message.reply_text("🔧 Admin Panel", reply_markup=ADMIN_KEYBOARD)
            else:
                user_states[user_id] = UserState.MAIN
                await update.message.reply_text("📋 Main Menu:", reply_markup=MAIN_KEYBOARD)
            
    except Exception as e:
        logger.error(f"Error handling message: {e}")
        await update.message.reply_text("❌ An error occurred. Please try again.")

async def process_telegram_handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        handle = update.message.text.strip()
        
        if is_valid_telegram_handle(handle):
            # Remove @ if present
            if handle.startswith('@'):
                handle = handle[1:]
            
            # Update user's telegram handle
            if supabase:
                supabase.table('users').update({'telegram_handle': handle}).eq('id', user_id).execute()
            
            # Move to Twitter handle
            user_states[user_id] = UserState.SETTING_TWITTER
            await update.message.reply_text(
                f"✅ Telegram handle saved: @{handle}\n\n"
                "Now, please provide your Twitter handle:"
            )
        else:
            await update.message.reply_text(
                "❌ Invalid Telegram handle!\n\n"
                "Please provide a valid handle:\n"
                "• Can start with @ (optional)\n"
                "• 5-32 characters\n"
                "• Only letters, numbers, and underscores\n\n"
                "Example: @username or username"
            )
    except Exception as e:
        logger.error(f"Error processing telegram handle: {e}")
        await update.message.reply_text("❌ Error saving Telegram handle.")

async def process_twitter_handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        handle = update.message.text.strip()
        
        if is_valid_twitter_handle(handle):
            # Remove @ if present
            if handle.startswith('@'):
                handle = handle[1:]
            
            # Update user's twitter handle
            if supabase:
                supabase.table('users').update({'twitter_handle': handle}).eq('id', user_id).execute()
            
            # Move to group joining
            await update.message.reply_text(
                f"✅ Twitter handle saved: @{handle}\n\n"
                "Great! Now let's join the required groups."
            )
            
            # Automatically show groups
            await handle_join_groups(update, context)
            
        else:
            await update.message.reply_text(
                "❌ Invalid Twitter handle!\n\n"
                "Please provide a valid handle:\n"
                "• Can start with @ (optional)\n"
                "• 1-15 characters\n"
                "• Only letters, numbers, and underscores\n\n"
                "Example: @username or username"
            )
    except Exception as e:
        logger.error(f"Error processing twitter handle: {e}")
        await update.message.reply_text("❌ Error saving Twitter handle.")

async def handle_join_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id] = UserState.JOINING_GROUPS
    
    msg = "📢 Join ALL these groups to participate:\n\n"
    for i, (group_id, url) in enumerate(GROUP_URLS.items(), 1):
        group_name = GROUP_NAMES.get(group_id, f"Group {group_id}")
        msg += f"{i}️⃣ [{group_name}]({url})\n"
    
    msg += "\n⚠️ You MUST join ALL groups!\n"
    msg += "After joining ALL groups, click the button below to verify:"
    
    await update.message.reply_text(msg, reply_markup=GROUPS_KEYBOARD, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

async def verify_group_membership(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    try:
        user = get_user(user_id)
        if not user:
            await update.message.reply_text("❌ User not found.")
            return
        
        # Check if user already received group bonus
        if user.get('has_received_group_bonus', False):
            msg = "✅ You have already joined all groups and received your bonus!\n\n"
            msg += "Welcome to the main menu:"
            
            user_states[user_id] = UserState.MAIN
            await update.message.reply_text(msg, reply_markup=MAIN_KEYBOARD)
            return
        
        # Show verification in progress
        verification_msg = await update.message.reply_text("🔍 Verifying your group membership...\nPlease wait...")
        
        # ACTUAL GROUP MEMBERSHIP CHECK - FIXED!
        is_member, failed_groups = await check_group_membership(context, user_id)
        
        if is_member:
            # Update user as verified and mark bonus as received
            if supabase:
                supabase.table('users').update({
                    'joined_all_groups': True,
                    'has_received_group_bonus': True
                }).eq('id', user_id).execute()
            
            # Get group join bonus
            settings = get_settings()
            bonus = settings['group_join_bonus']
            
            # Credit bonus using database function
            if supabase:
                supabase.rpc('add_balance', {
                    'user_id_param': user_id,
                    'amount_param': str(bonus),
                    'type_param': 'group_join',
                    'description_param': 'Group join bonus for completing all group requirements'
                }).execute()
            
            msg = "🎉 Excellent! You joined ALL required groups.\n\n"
            msg += f"🎁 You earned {bonus} MetaCore bonus!\n\n"
            msg += "Now you can access the main menu. Set your BSC wallet address to receive tokens."
            
            user_states[user_id] = UserState.MAIN
            await verification_msg.edit_text(msg)
            await update.message.reply_text("📋 Main Menu:", reply_markup=MAIN_KEYBOARD)
            
        else:
            failure_msg = "❌ You haven't joined ALL required groups yet.\n\n"
            failure_msg += "Missing groups:\n"
            for group in failed_groups:
                failure_msg += f"• {group}\n"
            failure_msg += "\n🔗 Please join ALL groups first, then try again!"
            
            await verification_msg.edit_text(failure_msg)
            await update.message.reply_text("Please join the missing groups:", reply_markup=GROUPS_KEYBOARD)
            
    except Exception as e:
        logger.error(f"Error verifying membership: {e}")
        await update.message.reply_text("❌ Error verifying group membership. Please try again.")

async def handle_referral_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        bot_username = context.bot.username
        
        referral_link = f"https://t.me/{bot_username}?start=ref{user_id}"
        
        # Get referral stats
        if supabase:
            referrals = supabase.table('referrals').select('*').eq('inviter', user_id).execute()
            referral_count = len(referrals.data) if referrals.data else 0
        else:
            referral_count = 0
        
        msg = f"🔗 Your Referral Link:\n"
        msg += f"`{referral_link}`\n\n"
        msg += f"📊 Your Stats:\n"
        msg += f"👥 Referrals: {referral_count}\n"
        msg += f"💰 Earned: {referral_count * 4000:,} MetaCore\n\n"
        msg += f"💡 Earn 4000 MetaCore (~$90) per referral!\n\n"
        msg += f"Share this link with friends to earn more tokens!"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"Error in referral link: {e}")
        await update.message.reply_text("❌ Error generating referral link.")

async def handle_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        user = get_user(user_id)
        
        if user:
            balance_tokens = float(user['balance'])
            
            msg = f"💰 Your MetaCore Balance\n\n"
            msg += f"🪙 {balance_tokens:,.0f} MetaCore\n"
            msg += f"💵 ≈ ${balance_tokens * 0.0225:,.2f} USD\n\n"
            
            if user.get('metacore_address'):
                address = user['metacore_address']
                msg += f"📍 Wallet: {address[:6]}...{address[-4:]}"
            else:
                msg += f"⚠️ No wallet set - please set your BSC address!"
        else:
            msg = "❌ User not found. Please /start first."
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error in balance: {e}")
        await update.message.reply_text("❌ Error getting balance.")

async def handle_set_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id] = UserState.SETTING_WALLET
    
    msg = "💳 Set Your BSC Wallet Address\n\n"
    msg += "⚠️ Send your MetaCore (BEP-20) wallet address\n"
    msg += "⚠️ Must start with 0x and be 42 characters\n"
    msg += "⚠️ Double-check - wrong address = lost tokens!\n\n"
    msg += "Example: 0x742d35Cc6634C0532925a3b8D4C0C8b3C2e1e1e1\n\n"
    msg += "🔗 BSC Testnet Network Details:\n"
    msg += "• Network Name: BSC Testnet\n"
    msg += "• RPC URL: https://data-seed-prebsc-1-s1.binance.org:8545/\n"
    msg += "• Chain ID: 97\n"
    msg += "• Symbol: tBNB\n"
    msg += "• Block Explorer: https://testnet.bscscan.com"
    
    await update.message.reply_text(msg)

async def process_wallet_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        address = update.message.text.strip()
        
        if is_valid_bsc_address(address):
            if supabase:
                supabase.table('users').update({'metacore_address': address}).eq('id', user_id).execute()
            user_states[user_id] = UserState.MAIN
            
            msg = f"✅ Wallet Address Saved!\n\n"
            msg += f"📍 Address: {address}\n\n"
            msg += f"🎉 You can now withdraw your MetaCore tokens!\n"
            msg += f"🔗 Make sure you have BSC Testnet configured in your wallet!"
            
            await update.message.reply_text(msg, reply_markup=MAIN_KEYBOARD)
        else:
            msg = "❌ Invalid wallet address!\n\n"
            msg += "Please send a valid BSC address:\n"
            msg += "• Must start with 0x\n"
            msg += "• Must be exactly 42 characters\n"
            msg += "• Only contains letters and numbers"
            await update.message.reply_text(msg)
            
    except Exception as e:
        logger.error(f"Error processing wallet: {e}")
        await update.message.reply_text("❌ Error saving wallet address.")

async def handle_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        user = get_user(user_id)
        
        if not user:
            await update.message.reply_text("❌ User not found. Please /start first.")
            return
        
        if not user['joined_all_groups']:
            await update.message.reply_text("❌ Please join all required groups first!")
            return
        
        if not user.get('metacore_address'):
            await update.message.reply_text("❌ Please set your BSC wallet address first!")
            return
        
        settings = get_settings()
        min_amount = settings['min_withdraw_amount']
        balance = float(user['balance'])
        
        if balance < min_amount:
            await update.message.reply_text(f"❌ Minimum withdrawal is {min_amount:,} MetaCore\nYour balance: {balance:,}")
            return
        
        user_states[user_id] = UserState.WITHDRAWING
        msg = f"🏦 Withdraw MetaCore Tokens\n\n"
        msg += f"💰 Available: {balance:,} MetaCore\n"
        msg += f"📍 To: {user['metacore_address'][:10]}...{user['metacore_address'][-6:]}\n"
        msg += f"🔢 Minimum: {min_amount:,} MetaCore\n\n"
        msg += f"Enter amount to withdraw:"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error in withdraw: {e}")
        await update.message.reply_text("❌ Error processing withdrawal request.")

async def process_withdrawal_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        amount_text = update.message.text.strip()
        
        try:
            amount = float(amount_text)
        except ValueError:
            await update.message.reply_text("❌ Invalid amount! Please enter a valid number.")
            return
        
        user = get_user(user_id)
        if not user:
            await update.message.reply_text("❌ User not found.")
            return
        
        settings = get_settings()
        min_amount = settings['min_withdraw_amount']
        balance = float(user['balance'])
        
        if amount < min_amount:
            await update.message.reply_text(f"❌ Minimum withdrawal: {min_amount:,} MetaCore")
            return
        
        if amount > balance:
            await update.message.reply_text(f"❌ Insufficient balance!\nAvailable: {balance:,}")
            return
        
        # Create withdrawal request
        if supabase:
            withdrawal_data = {
                'user_id': user_id,
                'amount': str(amount),
                'to_address': user['metacore_address'],
                'status': 'pending'
            }
            
            result = supabase.table('withdrawals').insert(withdrawal_data).execute()
            
            if result.data:
                # Deduct balance
                supabase.rpc('add_balance', {
                    'user_id_param': user_id,
                    'amount_param': str(-amount),
                    'type_param': 'withdrawal',
                    'description_param': f'Withdrawal request #{result.data[0]["id"]}'
                }).execute()
                
                user_states[user_id] = UserState.MAIN
                
                msg = f"✅ Withdrawal Request Submitted!\n\n"
                msg += f"🆔 Request ID: #{result.data[0]['id']}\n"
                msg += f"💰 Amount: {amount:,} MetaCore\n"
                msg += f"📍 To: {user['metacore_address']}\n"
                msg += f"⏳ Status: Pending\n\n"
                msg += f"⚡ Tokens will be sent within 24-48 hours!"
                
                await update.message.reply_text(msg, reply_markup=MAIN_KEYBOARD)
            else:
                await update.message.reply_text("❌ Error creating withdrawal request.")
        else:
            await update.message.reply_text("❌ Database not configured.")
        
    except Exception as e:
        logger.error(f"Error processing withdrawal: {e}")
        await update.message.reply_text("❌ Error processing withdrawal.")

async def handle_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        user = get_user(user_id)
        
        if user:
            balance = float(user['balance'])
            
            msg = f"👤 Your Profile\n\n"
            msg += f"🆔 ID: {user_id}\n"
            msg += f"👤 Username: @{user.get('username', 'N/A')}\n"
            msg += f"📝 Name: {user.get('full_name', 'N/A')}\n"
            msg += f"📱 Telegram: @{user.get('telegram_handle', 'Not set')}\n"
            msg += f"🐦 Twitter: @{user.get('twitter_handle', 'Not set')}\n"
            msg += f"💰 Balance: {balance:,} MetaCore\n"
            msg += f"👥 Groups: {'✅ Joined' if user.get('joined_all_groups') else '❌ Pending'}\n"
            
            if user.get('metacore_address'):
                address = user['metacore_address']
                msg += f"💳 Wallet: {address[:6]}...{address[-4:]}\n"
            else:
                msg += f"💳 Wallet: Not set\n"
            
            if user.get('invited_by'):
                msg += f"🎯 Invited by: {user['invited_by']}\n"
            
            # Get referral count
            if supabase:
                referrals = supabase.table('referrals').select('*').eq('inviter', user_id).execute()
                referral_count = len(referrals.data) if referrals.data else 0
                msg += f"🔗 Referrals: {referral_count}"
            
        else:
            msg = "❌ Profile not found."
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error in profile: {e}")
        await update.message.reply_text("❌ Error getting profile.")

async def handle_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "❓ MetaCore Airdrop Help\n\n"
    msg += "🎯 How to participate:\n"
    msg += "1. Complete profile setup\n"
    msg += "2. Join ALL required groups\n"
    msg += "3. Set your BSC wallet address\n"
    msg += "4. Earn tokens & withdraw\n\n"
    msg += "💰 Ways to earn:\n"
    msg += "• Signup bonus: 1000 MetaCore\n"
    msg += "• Group join bonus: 500 MetaCore\n"
    msg += "• Referral bonus: 4000 MetaCore each\n\n"
    msg += "🔗 Share your referral link to earn more!\n\n"
    msg += "⚠️ Important:\n"
    msg += "• Join ALL groups to unlock features\n"
    msg += "• Use BSC Testnet network\n"
    msg += "• Minimum withdrawal: 4000 MetaCore"
    
    await update.message.reply_text(msg)

# Admin functions
async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID or not supabase:
        return
    
    try:
        # Get stats
        users = supabase.table('users').select('*').execute()
        referrals = supabase.table('referrals').select('*').execute()
        withdrawals = supabase.table('withdrawals').select('*').execute()
        transactions = supabase.table('transactions').select('*').execute()
        
        total_users = len(users.data) if users.data else 0
        total_referrals = len(referrals.data) if referrals.data else 0
        total_withdrawals = len(withdrawals.data) if withdrawals.data else 0
        
        # Calculate totals
        total_balance = sum(float(user.get('balance', 0)) for user in users.data) if users.data else 0
        
        pending_withdrawals = [w for w in withdrawals.data if w.get('status') == 'pending'] if withdrawals.data else []
        completed_withdrawals = [w for w in withdrawals.data if w.get('status') == 'completed'] if withdrawals.data else []
        
        joined_groups = sum(1 for user in users.data if user.get('joined_all_groups')) if users.data else 0
        
        msg = f"📊 Bot Statistics\n\n"
        msg += f"👥 Total Users: {total_users}\n"
        msg += f"✅ Completed Groups: {joined_groups}\n"
        msg += f"🔗 Total Referrals: {total_referrals}\n"
        msg += f"💰 Total Balance: {total_balance:,.0f}\n"
        msg += f"🏦 Pending Withdrawals: {len(pending_withdrawals)}\n"
        msg += f"✅ Completed Withdrawals: {len(completed_withdrawals)}\n"
        
        if pending_withdrawals:
            pending_amount = sum(float(w.get('amount', 0)) for w in pending_withdrawals)
            msg += f"💸 Pending Amount: {pending_amount:,.0f}"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error in admin stats: {e}")
        await update.message.reply_text("❌ Error getting stats.")

async def process_admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        message = update.message.text
        if not supabase:
            await update.message.reply_text("❌ Database not configured.")
            return
            
        users = supabase.table('users').select('id').execute()
        
        sent = 0
        failed = 0
        
        status_msg = await update.message.reply_text(f"📡 Broadcasting to {len(users.data)} users...")
        
        for user in users.data:
            try:
                await context.bot.send_message(
                    chat_id=user['id'], 
                    text=f"📢 Admin Broadcast\n\n{message}"
                )
                sent += 1
                
                if sent % 50 == 0:
                    await status_msg.edit_text(f"📡 Sent to {sent}/{len(users.data)} users...")
                    
            except Exception as e:
                failed += 1
                logger.error(f"Failed to send to {user['id']}: {e}")
        
        user_states[update.effective_user.id] = UserState.ADMIN_MENU
        await status_msg.edit_text(f"✅ Broadcast complete!\n📤 Sent: {sent}\n❌ Failed: {failed}")
        
    except Exception as e:
        logger.error(f"Error in broadcast: {e}")
        await update.message.reply_text("❌ Error broadcasting message.")

async def process_admin_user_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        user_id = int(update.message.text.strip())
        
        # Check if this is a group check request
        if user_states.get(update.effective_user.id) == UserState.ADMIN_USER_INFO:
            membership_results = await check_single_user_groups(context, user_id)
            
            msg = f"👥 Group Membership Check for {user_id}:\n\n"
            msg += "\n".join(membership_results)
            
            user_states[update.effective_user.id] = UserState.ADMIN_MENU
            await update.message.reply_text(msg)
            return
        
        user = get_user(user_id)
        
        if user:
            balance = float(user['balance'])
            
            if supabase:
                referrals = supabase.table('referrals').select('*').eq('inviter', user_id).execute()
                withdrawals = supabase.table('withdrawals').select('*').eq('user_id', user_id).execute()
            else:
                referrals = None
                withdrawals = None
            
            username = user['username'] or 'N/A'
            full_name = user['full_name'] or 'N/A'
            wallet = user.get('metacore_address', 'Not set')
            telegram_handle = user.get('telegram_handle', 'Not set')
            twitter_handle = user.get('twitter_handle', 'Not set')
            
            msg = f"👤 User Info: {user_id}\n\n"
            msg += f"Username: @{username}\n"
            msg += f"Full Name: {full_name}\n"
            msg += f"Telegram: @{telegram_handle}\n"
            msg += f"Twitter: @{twitter_handle}\n"
            msg += f"Balance: {balance:,.0f} MetaCore\n"
            
            if referrals:
                msg += f"Referrals: {len(referrals.data)}\n"
            if withdrawals:
                msg += f"Withdrawals: {len(withdrawals.data)}\n"
                
            msg += f"Groups Joined: {'Yes' if user['joined_all_groups'] else 'No'}\n"
            msg += f"Group Bonus: {'Yes' if user.get('has_received_group_bonus', False) else 'No'}\n"
            msg += f"Wallet: {wallet}\n"
            msg += f"Invited By: {user['invited_by'] or 'Direct'}\n"
            
            await update.message.reply_text(msg)
        else:
            await update.message.reply_text("❌ User not found")
        
        user_states[update.effective_user.id] = UserState.ADMIN_MENU
            
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID")
    except Exception as e:
        logger.error(f"Error in user info: {e}")
        await update.message.reply_text("❌ Error getting user info.")

async def process_admin_add_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        parts = update.message.text.strip().split()
        if len(parts) != 2:
            await update.message.reply_text("❌ Format: user_id amount")
            return
            
        user_id = int(parts[0])
        amount = float(parts[1])
        
        user = get_user(user_id)
        if not user:
            await update.message.reply_text("❌ User not found")
            return
        
        if supabase:
            # Add balance using database function
            supabase.rpc('add_balance', {
                'user_id_param': user_id,
                'amount_param': str(amount),
                'type_param': 'admin_credit',
                'description_param': f'Admin credit by {update.effective_user.id}'
            }).execute()
        
        username = user['username'] or 'N/A'
        msg = f"✅ Added {amount:,.0f} MetaCore to @{username} ({user_id})"
        await update.message.reply_text(msg)
        
        user_states[update.effective_user.id] = UserState.ADMIN_MENU
        
        # Notify user
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"🎁 You received {amount:,.0f} MetaCore from admin!"
            )
        except:
            pass
            
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID or amount")
    except Exception as e:
        logger.error(f"Error adding balance: {e}")
        await update.message.reply_text("❌ Error adding balance.")

async def handle_withdrawals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID or not supabase:
        return
    
    try:
        withdrawals = supabase.table('withdrawals').select('*').eq('status', 'pending').order('created_at').execute()
        
        if not withdrawals.data:
            await update.message.reply_text("✅ No pending withdrawals")
            return
        
        msg = f"⏳ Pending Withdrawals ({len(withdrawals.data)})\n\n"
        
        for w in withdrawals.data[:10]:
            user = get_user(w['user_id'])
            username = user['username'] if user else 'Unknown'
            amount = float(w['amount'])
            address = w['to_address']
            
            msg += f"🆔 #{w['id']}\n"
            msg += f"👤 @{username} ({w['user_id']})\n"
            msg += f"💰 {amount:,.0f} MetaCore\n"
            msg += f"📍 {address[:10]}...{address[-6:]}\n"
            msg += f"⏰ {w['created_at'][:16]}\n\n"
        
        if len(withdrawals.data) > 10:
            msg += f"... and {len(withdrawals.data) - 10} more"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error getting withdrawals: {e}")
        await update.message.reply_text("❌ Error getting withdrawals.")

async def handle_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        settings = get_settings()
        
        msg = f"⚙️ Bot Settings\n\n"
        msg += f"💰 Signup Bonus: {settings['signup_bonus']} MetaCore\n"
        msg += f"🎁 Referral Bonus: {settings['referral_bonus']} MetaCore\n"
        msg += f"👥 Group Join Bonus: {settings['group_join_bonus']} MetaCore\n"
        msg += f"📊 Min Withdrawal: {settings['min_withdraw_amount']} MetaCore\n"
        msg += f"💵 Token Price: ${settings.get('token_price_usd', 0.0225)}\n"
        msg += f"🔗 Network: BSC Testnet\n\n"
        msg += f"📋 Required Groups: {len(REQUIRED_GROUPS)}"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error getting settings: {e}")
        await update.message.reply_text("❌ Error getting settings.")

async def handle_network_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        # Check Web3 connection
        is_connected = w3.is_connected() if w3 else False
        latest_block = w3.eth.block_number if is_connected else "N/A"
        
        msg = f"🔗 BSC Testnet Network Info\n\n"
        msg += f"📡 Connection: {'✅ Connected' if is_connected else '❌ Disconnected'}\n"
        msg += f"🔗 RPC URL: {BSC_NODE_URL}\n"
        msg += f"🆔 Chain ID: 97\n"
        msg += f"💰 Symbol: tBNB\n"
        msg += f"📊 Latest Block: {latest_block}\n"
        msg += f"🔍 Explorer: https://testnet.bscscan.com\n\n"
        
        if CONTRACT_ADDRESS:
            msg += f"📄 Contract: {CONTRACT_ADDRESS}\n"
        else:
            msg += f"📄 Contract: Not configured\n"
            
        if ADMIN_PRIVATE_KEY and w3:
            admin_account = w3.eth.account.from_key(ADMIN_PRIVATE_KEY)
            if is_connected:
                balance = w3.eth.get_balance(admin_account.address)
                balance_bnb = w3.from_wei(balance, 'ether')
                msg += f"💳 Admin Balance: {balance_bnb:.4f} tBNB"
            else:
                msg += f"💳 Admin Address: {admin_account.address}"
        else:
            msg += f"💳 Admin Key: Not configured"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error getting network info: {e}")
        await update.message.reply_text("❌ Error getting network info.")

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle callback queries"""
    query = update.callback_query
    await query.answer()
    
    # Handle any inline keyboard callbacks here if needed
    pass

# Error handler
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log errors caused by Updates."""
    logger.error(f"Exception while handling an update: {context.error}")

def main():
    """Start the bot"""
    try:
        if not BOT_TOKEN:
            logger.error("BOT_TOKEN not found in environment variables!")
            return
            
        application = Application.builder().token(BOT_TOKEN).build()
        
        # Add handlers
        application.add_handler(CommandHandler("start", start))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        application.add_handler(CallbackQueryHandler(handle_callback))
        
        # Add error handler
        application.add_error_handler(error_handler)
        
        logger.info("🚀 MetaCore Airdrop Bot started successfully on BSC Testnet!")
        logger.info(f"🔗 Connected to: {BSC_NODE_URL}")
        logger.info(f"📄 Contract: {CONTRACT_ADDRESS or 'Not configured'}")
        logger.info(f"💾 Database: {'Connected' if supabase else 'Not configured'}")
        
        # Start bot
        application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")

if __name__ == '__main__':
    main()
