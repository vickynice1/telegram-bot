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
    print("âœ… Patched gotrue to ignore proxy argument")

except Exception as e:
    print("âš ï¸ Failed to patch gotrue:", e)

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
    ['ðŸ”— Referral Link', 'ðŸ’° Balance'],
    ['ðŸ’³ Set Wallet', 'ðŸ¦ Withdraw'],
    ['ðŸ‘¤ My Profile', 'â“ Help']
], resize_keyboard=True)

GROUPS_KEYBOARD = ReplyKeyboardMarkup([
    ['âœ… I\'ve Joined All Groups'],
    ['ðŸ”™ Back to Menu']
], resize_keyboard=True)

ADMIN_KEYBOARD = ReplyKeyboardMarkup([
    ['ðŸ“Š Stats', 'ðŸ“¢ Broadcast'],
    ['ðŸ‘¤ User Info', 'ðŸ’° Add Balance'],
    ['ðŸ¦ Withdrawals', 'âš™ï¸ Settings'],
    ['ðŸŒ Network Info', 'ðŸ‘¥ Group Check']
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
                
                status_emoji = "âœ…" if member.status in ['member', 'creator', 'administrator'] else "âŒ"
                membership_results.append(f"{status_emoji} {group_name}: {member.status}")
                
            except Exception as e:
                group_name = GROUP_NAMES.get(group_id, f"Group {group_id}")
                membership_results.append(f"âŒ {group_name}: Error - {str(e)[:50]}...")
        
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
            await update.message.reply_text("âš ï¸ Please wait before sending another command")
            return
        
        # Check if admin
        if user_id == ADMIN_ID:
            user_states[user_id] = UserState.ADMIN_MENU
            await update.message.reply_text("ðŸ”§ Admin Panel Access", reply_markup=ADMIN_KEYBOARD)
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
                welcome_msg = "ðŸŽ‰ Welcome to MetaCore Airdrop!\n\n"
                welcome_msg += "âœ… You received 1000 MetaCore signup bonus!\n\n"
                if invited_by:
                    welcome_msg += "ðŸŽ Referral bonus credited to your referrer!\n\n"
                welcome_msg += "Let's get you set up! First, please provide your Telegram handle:"
                
                user_states[user_id] = UserState.SETTING_TELEGRAM
                await update.message.reply_text(welcome_msg)
                return
            else:
                await update.message.reply_text("âŒ Error creating account. Please try again.")
                return
        else:
            # Existing user - check if they completed onboarding
            if not db_user.get('joined_all_groups', False):
                # User hasn't completed group joining
                if not db_user.get('telegram_handle') or not db_user.get('twitter_handle'):
                    # Complete profile setup first
                    if not db_user.get('telegram_handle'):
                        user_states[user_id] = UserState.SETTING_TELEGRAM
                        await update.message.reply_text("ðŸ‘‹ Welcome back! Please provide your Telegram handle:")
                        return
                    elif not db_user.get('twitter_handle'):
                        user_states[user_id] = UserState.SETTING_TWITTER
                        await update.message.reply_text("ðŸ‘‹ Welcome back! Please provide your Twitter handle:")
                        return
                else:
                    # Profile complete, need to join groups
                    await handle_join_groups(update, context)
                    return
            else:
                # User completed everything - show main menu
                welcome_msg = "ðŸ‘‹ Welcome back to MetaCore Airdrop!\n\n"
                welcome_msg += "Choose an option below:"
                
                user_states[user_id] = UserState.MAIN
                await update.message.reply_text(welcome_msg, reply_markup=MAIN_KEYBOARD)
        
    except Exception as e:
        logger.error(f"Error in start command: {e}")
        await update.message.reply_text("âŒ An error occurred. Please try again.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        text = update.message.text
        state = user_states.get(user_id, UserState.MAIN)
        
        if not rate_limit_check(user_id):
            await update.message.reply_text("âš ï¸ Please wait before sending another command")
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
            elif text == 'ðŸ“Š Stats':
                await admin_stats(update, context)
                return
            elif text == 'ðŸ“¢ Broadcast':
                user_states[user_id] = UserState.ADMIN_BROADCAST
                await update.message.reply_text("ðŸ“¢ Send the message to broadcast to all users:")
                return
            elif text == 'ðŸ‘¤ User Info':
                user_states[user_id] = UserState.ADMIN_USER_INFO
                await update.message.reply_text("ðŸ‘¤ Send the user ID to get info:")
                return
            elif text == 'ðŸ’° Add Balance':
                user_states[user_id] = UserState.ADMIN_ADD_BALANCE
                await update.message.reply_text("ðŸ’° Send: user_id amount\nExample: 123456789 1000")
                return
            elif text == 'ðŸ¦ Withdrawals':
                await handle_withdrawals(update, context)
                return
            elif text == 'âš™ï¸ Settings':
                await handle_settings(update, context)
                return
            elif text == 'ðŸŒ Network Info':
                await handle_network_info(update, context)
                return
            elif text == 'ðŸ‘¥ Group Check':
                user_states[user_id] = UserState.ADMIN_USER_INFO
                await update.message.reply_text("ðŸ‘¥ Send user ID to check their group membership:")
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
        if text == 'ðŸ”— Referral Link':
            await handle_referral_link(update, context)
        elif text == 'ðŸ’° Balance':
            await handle_balance(update, context)
        elif text == 'ðŸ’³ Set Wallet':
            await handle_set_wallet(update, context)
        elif text == 'ðŸ¦ Withdraw':
            await handle_withdraw(update, context)
        elif text == 'ðŸ‘¤ My Profile':
            await handle_profile(update, context)
        elif text == 'â“ Help':
            await handle_help(update, context)
        elif text == 'âœ… I\'ve Joined All Groups':
            await verify_group_membership(update, context)
        elif text == 'ðŸ”™ Back to Menu':
            if user_id == ADMIN_ID:
                user_states[user_id] = UserState.ADMIN_MENU
                await update.message.reply_text("ðŸ”§ Admin Panel", reply_markup=ADMIN_KEYBOARD)
            else:
                user_states[user_id] = UserState.MAIN
                await update.message.reply_text("ðŸ“‹ Main Menu:", reply_markup=MAIN_KEYBOARD)
            
    except Exception as e:
        logger.error(f"Error handling message: {e}")
        await update.message.reply_text("âŒ An error occurred. Please try again.")

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
                f"âœ… Telegram handle saved: @{handle}\n\n"
                "Now, please provide your Twitter handle:"
            )
        else:
            await update.message.reply_text(
                "âŒ Invalid Telegram handle!\n\n"
                "Please provide a valid handle:\n"
                "â€¢ Can start with @ (optional)\n"
                "â€¢ 5-32 characters\n"
                "â€¢ Only letters, numbers, and underscores\n\n"
                "Example: @username or username"
            )
    except Exception as e:
        logger.error(f"Error processing telegram handle: {e}")
        await update.message.reply_text("âŒ Error saving Telegram handle.")

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
                f"âœ… Twitter handle saved: @{handle}\n\n"
                "Great! Now let's join the required groups."
            )
            
            # Automatically show groups
            await handle_join_groups(update, context)
            
        else:
            await update.message.reply_text(
                "âŒ Invalid Twitter handle!\n\n"
                "Please provide a valid handle:\n"
                "â€¢ Can start with @ (optional)\n"
                "â€¢ 1-15 characters\n"
                "â€¢ Only letters, numbers, and underscores\n\n"
                "Example: @username or username"
            )
    except Exception as e:
        logger.error(f"Error processing twitter handle: {e}")
        await update.message.reply_text("âŒ Error saving Twitter handle.")

async def handle_join_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id] = UserState.JOINING_GROUPS
    
    msg = "ðŸ“¢ Join ALL these groups to participate:\n\n"
    for i, (group_id, url) in enumerate(GROUP_URLS.items(), 1):
        group_name = GROUP_NAMES.get(group_id, f"Group {group_id}")
        msg += f"{i}ï¸âƒ£ [{group_name}]({url})\n"
    
    msg += "\nâš ï¸ You MUST join ALL groups!\n"
    msg += "After joining ALL groups, click the button below to verify:"
    
    await update.message.reply_text(msg, reply_markup=GROUPS_KEYBOARD, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

async def verify_group_membership(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    try:
        user = get_user(user_id)
        if not user:
            await update.message.reply_text("âŒ User not found.")
            return
        
        # Check if user already received group bonus
        if user.get('has_received_group_bonus', False):
            msg = "âœ… You have already joined all groups and received your bonus!\n\n"
            msg += "Welcome to the main menu:"
            
            user_states[user_id] = UserState.MAIN
            await update.message.reply_text(msg, reply_markup=MAIN_KEYBOARD)
            return
        
        # Show verification in progress
        verification_msg = await update.message.reply_text("ðŸ” Verifying your group membership...\nPlease wait...")
        
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
            
            msg = "ðŸŽ‰ Excellent! You joined ALL required groups.\n\n"
            msg += f"ðŸŽ You earned {bonus} MetaCore bonus!\n\n"
            msg += "Now you can access the main menu. Set your BSC wallet address to receive tokens."
            
            user_states[user_id] = UserState.MAIN
            await verification_msg.edit_text(msg)
            await update.message.reply_text("ðŸ“‹ Main Menu:", reply_markup=MAIN_KEYBOARD)
            
        else:
            failure_msg = "âŒ You haven't joined ALL required groups yet.\n\n"
            failure_msg += "Missing groups:\n"
            for group in failed_groups:
                failure_msg += f"â€¢ {group}\n"
            failure_msg += "\nðŸ”— Please join ALL groups first, then try again!"
            
            await verification_msg.edit_text(failure_msg)
            await update.message.reply_text("Please join the missing groups:", reply_markup=GROUPS_KEYBOARD)
            
    except Exception as e:
        logger.error(f"Error verifying membership: {e}")
        await update.message.reply_text("âŒ Error verifying group membership. Please try again.")

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
        
        msg = f"ðŸ”— Your Referral Link:\n"
        msg += f"`{referral_link}`\n\n"
        msg += f"ðŸ“Š Your Stats:\n"
        msg += f"ðŸ‘¥ Referrals: {referral_count}\n"
        msg += f"ðŸ’° Earned: {referral_count * 4000:,} MetaCore\n\n"
        msg += f"ðŸ’¡ Earn 4000 MetaCore (~$90) per referral!\n\n"
        msg += f"Share this link with friends to earn more tokens!"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"Error in referral link: {e}")
        await update.message.reply_text("âŒ Error generating referral link.")

async def handle_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        user = get_user(user_id)
        
        if user:
            balance_tokens = float(user['balance'])
            
            msg = f"ðŸ’° Your MetaCore Balance\n\n"
            msg += f"ðŸª™ {balance_tokens:,.0f} MetaCore\n"
            msg += f"ðŸ’µ â‰ˆ ${balance_tokens * 0.0225:,.2f} USD\n\n"
            
            if user.get('metacore_address'):
                address = user['metacore_address']
                msg += f"ðŸ“ Wallet: {address[:6]}...{address[-4:]}"
            else:
                msg += f"âš ï¸ No wallet set - please set your BSC address!"
        else:
            msg = "âŒ User not found. Please /start first."
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error in balance: {e}")
        await update.message.reply_text("âŒ Error getting balance.")

async def handle_set_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id] = UserState.SETTING_WALLET
    
    msg = "ðŸ’³ Set Your BSC Wallet Address\n\n"
    msg += "âš ï¸ Send your MetaCore (BEP-20) wallet address\n"
    msg += "âš ï¸ Must start with 0x and be 42 characters\n"
    msg += "âš ï¸ Double-check - wrong address = lost tokens!\n\n"
    msg += "Example: 0x742d35Cc6634C0532925a3b8D4C0C8b3C2e1e1e1\n\n"
    msg += "ðŸ”— BSC Testnet Network Details:\n"
    msg += "â€¢ Network Name: BSC Testnet\n"
    msg += "â€¢ RPC URL: https://data-seed-prebsc-1-s1.binance.org:8545/\n"
    msg += "â€¢ Chain ID: 97\n"
    msg += "â€¢ Symbol: tBNB\n"
    msg += "â€¢ Block Explorer: https://testnet.bscscan.com"
    
    await update.message.reply_text(msg)

async def process_wallet_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        address = update.message.text.strip()
        
        if is_valid_bsc_address(address):
            if supabase:
                supabase.table('users').update({'metacore_address': address}).eq('id', user_id).execute()
            user_states[user_id] = UserState.MAIN
            
            msg = f"âœ… Wallet Address Saved!\n\n"
            msg += f"ðŸ“ Address: {address}\n\n"
            msg += f"ðŸŽ‰ You can now withdraw your MetaCore tokens!\n"
            msg += f"ðŸ”— Make sure you have BSC Testnet configured in your wallet!"
            
            await update.message.reply_text(msg, reply_markup=MAIN_KEYBOARD)
        else:
            msg = "âŒ Invalid wallet address!\n\n"
            msg += "Please send a valid BSC address:\n"
            msg += "â€¢ Must start with 0x\n"
            msg += "â€¢ Must be exactly 42 characters\n"
            msg += "â€¢ Only contains letters and numbers"
            await update.message.reply_text(msg)
            
    except Exception as e:
        logger.error(f"Error processing wallet: {e}")
        await update.message.reply_text("âŒ Error saving wallet address.")

async def handle_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        user = get_user(user_id)
        
        if not user:
            await update.message.reply_text("âŒ User not found. Please /start first.")
            return
        
        if not user['joined_all_groups']:
            await update.message.reply_text("âŒ Please join all required groups first!")
            return
        
        if not user.get('metacore_address'):
            await update.message.reply_text("âŒ Please set your BSC wallet address first!")
            return
        
        settings = get_settings()
        min_amount = settings['min_withdraw_amount']
        balance = float(user['balance'])
        
        if balance < min_amount:
            await update.message.reply_text(f"âŒ Minimum withdrawal is {min_amount:,} MetaCore\nYour balance: {balance:,}")
            return
        
        user_states[user_id] = UserState.WITHDRAWING
        msg = f"ðŸ¦ Withdraw MetaCore Tokens\n\n"
        msg += f"ðŸ’° Available: {balance:,} MetaCore\n"
        msg += f"ðŸ“ To: {user['metacore_address'][:10]}...{user['metacore_address'][-6:]}\n"
        msg += f"ðŸ”¢ Minimum: {min_amount:,} MetaCore\n\n"
        msg += f"Enter amount to withdraw:"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error in withdraw: {e}")
        await update.message.reply_text("âŒ Error processing withdrawal request.")

async def process_withdrawal_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        amount_text = update.message.text.strip()
        
        try:
            amount = float(amount_text)
        except ValueError:
            await update.message.reply_text("âŒ Invalid amount! Please enter a valid number.")
            return
        
        user = get_user(user_id)
        if not user:
            await update.message.reply_text("âŒ User not found.")
            return
        
        settings = get_settings()
        min_amount = settings['min_withdraw_amount']
        balance = float(user['balance'])
        
        if amount < min_amount:
            await update.message.reply_text(f"âŒ Minimum withdrawal: {min_amount:,} MetaCore")
            return
        
        if amount > balance:
            await update.message.reply_text(f"âŒ Insufficient balance!\nAvailable: {balance:,}")
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
                
                msg = f"âœ… Withdrawal Request Submitted!\n\n"
                msg += f"ðŸ†” Request ID: #{result.data[0]['id']}\n"
                msg += f"ðŸ’° Amount: {amount:,} MetaCore\n"
                msg += f"ðŸ“ To: {user['metacore_address']}\n"
                msg += f"â³ Status: Pending\n\n"
                msg += f"âš¡ Tokens will be sent within 24-48 hours!"
                
                await update.message.reply_text(msg, reply_markup=MAIN_KEYBOARD)
            else:
                await update.message.reply_text("âŒ Error creating withdrawal request.")
        else:
            await update.message.reply_text("âŒ Database not configured.")
        
    except Exception as e:
        logger.error(f"Error processing withdrawal: {e}")
        await update.message.reply_text("âŒ Error processing withdrawal.")

async def handle_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        user = get_user(user_id)
        
        if user:
            balance = float(user['balance'])
            
            msg = f"ðŸ‘¤ Your Profile\n\n"
            msg += f"ðŸ†” ID: {user_id}\n"
            msg += f"ðŸ‘¤ Username: @{user.get('username', 'N/A')}\n"
            msg += f"ðŸ“ Name: {user.get('full_name', 'N/A')}\n"
            msg += f"ðŸ“± Telegram: @{user.get('telegram_handle', 'Not set')}\n"
            msg += f"ðŸ¦ Twitter: @{user.get('twitter_handle', 'Not set')}\n"
            msg += f"ðŸ’° Balance: {balance:,} MetaCore\n"
            msg += f"ðŸ‘¥ Groups: {'âœ… Joined' if user.get('joined_all_groups') else 'âŒ Pending'}\n"
            
            if user.get('metacore_address'):
                address = user['metacore_address']
                msg += f"ðŸ’³ Wallet: {address[:6]}...{address[-4:]}\n"
            else:
                msg += f"ðŸ’³ Wallet: Not set\n"
            
            if user.get('invited_by'):
                msg += f"ðŸŽ¯ Invited by: {user['invited_by']}\n"
            
            # Get referral count
            if supabase:
                referrals = supabase.table('referrals').select('*').eq('inviter', user_id).execute()
                referral_count = len(referrals.data) if referrals.data else 0
                msg += f"ðŸ”— Referrals: {referral_count}"
            
        else:
            msg = "âŒ Profile not found."
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error in profile: {e}")
        await update.message.reply_text("âŒ Error getting profile.")

async def handle_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "â“ MetaCore Airdrop Help\n\n"
    msg += "ðŸŽ¯ How to participate:\n"
    msg += "1. Complete profile setup\n"
    msg += "2. Join ALL required groups\n"
    msg += "3. Set your BSC wallet address\n"
    msg += "4. Earn tokens & withdraw\n\n"
    msg += "ðŸ’° Ways to earn:\n"
    msg += "â€¢ Signup bonus: 1000 MetaCore\n"
    msg += "â€¢ Group join bonus: 500 MetaCore\n"
    msg += "â€¢ Referral bonus: 4000 MetaCore each\n\n"
    msg += "ðŸ”— Share your referral link to earn more!\n\n"
    msg += "âš ï¸ Important:\n"
    msg += "â€¢ Join ALL groups to unlock features\n"
    msg += "â€¢ Use BSC Testnet network\n"
    msg += "â€¢ Minimum withdrawal: 4000 MetaCore"
    
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
        
        msg = f"ðŸ“Š Bot Statistics\n\n"
        msg += f"ðŸ‘¥ Total Users: {total_users}\n"
        msg += f"âœ… Completed Groups: {joined_groups}\n"
        msg += f"ðŸ”— Total Referrals: {total_referrals}\n"
        msg += f"ðŸ’° Total Balance: {total_balance:,.0f}\n"
        msg += f"ðŸ¦ Pending Withdrawals: {len(pending_withdrawals)}\n"
        msg += f"âœ… Completed Withdrawals: {len(completed_withdrawals)}\n"
        
        if pending_withdrawals:
            pending_amount = sum(float(w.get('amount', 0)) for w in pending_withdrawals)
            msg += f"ðŸ’¸ Pending Amount: {pending_amount:,.0f}"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error in admin stats: {e}")
        await update.message.reply_text("âŒ Error getting stats.")

async def process_admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        message = update.message.text
        if not supabase:
            await update.message.reply_text("âŒ Database not configured.")
            return
            
        users = supabase.table('users').select('id').execute()
        
        sent = 0
        failed = 0
        
        status_msg = await update.message.reply_text(f"ðŸ“¡ Broadcasting to {len(users.data)} users...")
        
        for user in users.data:
            try:
                await context.bot.send_message(
                    chat_id=user['id'], 
                    text=f"ðŸ“¢ Admin Broadcast\n\n{message}"
                )
                sent += 1
                
                if sent % 50 == 0:
                    await status_msg.edit_text(f"ðŸ“¡ Sent to {sent}/{len(users.data)} users...")
                    
            except Exception as e:
                failed += 1
                logger.error(f"Failed to send to {user['id']}: {e}")
        
        user_states[update.effective_user.id] = UserState.ADMIN_MENU
        await status_msg.edit_text(f"âœ… Broadcast complete!\nðŸ“¤ Sent: {sent}\nâŒ Failed: {failed}")
        
    except Exception as e:
        logger.error(f"Error in broadcast: {e}")
        await update.message.reply_text("âŒ Error broadcasting message.")

async def process_admin_user_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        user_id = int(update.message.text.strip())
        
        # Check if this is a group check request
        if user_states.get(update.effective_user.id) == UserState.ADMIN_USER_INFO:
            membership_results = await check_single_user_groups(context, user_id)
            
            msg = f"ðŸ‘¥ Group Membership Check for {user_id}:\n\n"
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
            
            msg = f"ðŸ‘¤ User Info: {user_id}\n\n"
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
            await update.message.reply_text("âŒ User not found")
        
        user_states[update.effective_user.id] = UserState.ADMIN_MENU
            
    except ValueError:
        await update.message.reply_text("âŒ Invalid user ID")
    except Exception as e:
        logger.error(f"Error in user info: {e}")
        await update.message.reply_text("âŒ Error getting user info.")

async def process_admin_add_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        parts = update.message.text.strip().split()
        if len(parts) != 2:
            await update.message.reply_text("âŒ Format: user_id amount")
            return
            
        user_id = int(parts[0])
        amount = float(parts[1])
        
        user = get_user(user_id)
        if not user:
            await update.message.reply_text("âŒ User not found")
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
        msg = f"âœ… Added {amount:,.0f} MetaCore to @{username} ({user_id})"
        await update.message.reply_text(msg)
        
        user_states[update.effective_user.id] = UserState.ADMIN_MENU
        
        # Notify user
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"ðŸŽ You received {amount:,.0f} MetaCore from admin!"
            )
        except:
            pass
            
    except ValueError:
        await update.message.reply_text("âŒ Invalid user ID or amount")
    except Exception as e:
        logger.error(f"Error adding balance: {e}")
        await update.message.reply_text("âŒ Error adding balance.")

async def handle_withdrawals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID or not supabase:
        return
    
    try:
        withdrawals = supabase.table('withdrawals').select('*').eq('status', 'pending').order('created_at').execute()
        
        if not withdrawals.data:
            await update.message.reply_text("âœ… No pending withdrawals")
            return
        
        msg = f"â³ Pending Withdrawals ({len(withdrawals.data)})\n\n"
        
        for w in withdrawals.data[:10]:
            user = get_user(w['user_id'])
            username = user['username'] if user else 'Unknown'
            amount = float(w['amount'])
            address = w['to_address']
            
            msg += f"ðŸ†” #{w['id']}\n"
            msg += f"ðŸ‘¤ @{username} ({w['user_id']})\n"
            msg += f"ðŸ’° {amount:,.0f} MetaCore\n"
            msg += f"ðŸ“ {address[:10]}...{address[-6:]}\n"
            msg += f"â° {w['created_at'][:16]}\n\n"
        
        if len(withdrawals.data) > 10:
            msg += f"... and {len(withdrawals.data) - 10} more"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error getting withdrawals: {e}")
        await update.message.reply_text("âŒ Error getting withdrawals.")

async def handle_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        settings = get_settings()
        
        msg = f"âš™ï¸ Bot Settings\n\n"
        msg += f"ðŸ’° Signup Bonus: {settings['signup_bonus']} MetaCore\n"
        msg += f"ðŸŽ Referral Bonus: {settings['referral_bonus']} MetaCore\n"
        msg += f"ðŸ‘¥ Group Join Bonus: {settings['group_join_bonus']} MetaCore\n"
        msg += f"ðŸ“Š Min Withdrawal: {settings['min_withdraw_amount']} MetaCore\n"
        msg += f"ðŸ’µ Token Price: ${settings.get('token_price_usd', 0.0225)}\n"
        msg += f"ðŸ”— Network: BSC Testnet\n\n"
        msg += f"ðŸ“‹ Required Groups: {len(REQUIRED_GROUPS)}"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error getting settings: {e}")
        await update.message.reply_text("âŒ Error getting settings.")

async def handle_network_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        # Check Web3 connection
        is_connected = w3.is_connected() if w3 else False
        latest_block = w3.eth.block_number if is_connected else "N/A"
        
        msg = f"ðŸ”— BSC Testnet Network Info\n\n"
        msg += f"ðŸ“¡ Connection: {'âœ… Connected' if is_connected else 'âŒ Disconnected'}\n"
        msg += f"ðŸ”— RPC URL: {BSC_NODE_URL}\n"
        msg += f"ðŸ†” Chain ID: 97\n"
        msg += f"ðŸ’° Symbol: tBNB\n"
        msg += f"ðŸ“Š Latest Block: {latest_block}\n"
        msg += f"ðŸ” Explorer: https://testnet.bscscan.com\n\n"
        
        if CONTRACT_ADDRESS:
            msg += f"ðŸ“„ Contract: {CONTRACT_ADDRESS}\n"
        else:
            msg += f"ðŸ“„ Contract: Not configured\n"
            
        if ADMIN_PRIVATE_KEY and w3:
            admin_account = w3.eth.account.from_key(ADMIN_PRIVATE_KEY)
            if is_connected:
                balance = w3.eth.get_balance(admin_account.address)
                balance_bnb = w3.from_wei(balance, 'ether')
                msg += f"ðŸ’³ Admin Balance: {balance_bnb:.4f} tBNB"
            else:
                msg += f"ðŸ’³ Admin Address: {admin_account.address}"
        else:
            msg += f"ðŸ’³ Admin Key: Not configured"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error getting network info: {e}")
        await update.message.reply_text("âŒ Error getting network info.")

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
        
        logger.info("ðŸš€ MetaCore Airdrop Bot started successfully on BSC Testnet!")
        logger.info(f"ðŸ”— Connected to: {BSC_NODE_URL}")
        logger.info(f"ðŸ“„ Contract: {CONTRACT_ADDRESS or 'Not configured'}")
        logger.info(f"ðŸ’¾ Database: {'Connected' if supabase else 'Not configured'}")
        
        # Start bot
        application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")

if __name__ == '__main__':
    main()
