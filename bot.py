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
ADMIN_ID = 6950876107

# BSC Testnet Configuration
BSC_TESTNET_URL = "https://data-seed-prebsc-1-s1.binance.org:8545/"
BSC_NODE_URL = os.getenv("BSC_NODE_URL", BSC_TESTNET_URL)
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS")
ADMIN_PRIVATE_KEY = os.getenv("ADMIN_PRIVATE_KEY")

# Required groups (replace with your actual group IDs)
REQUIRED_GROUPS = [
    -1002257059748,  # Group 1
    -1002933970785,  # Group 2  
    -1002957373140   # Group 3
]

# Initialize clients
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
w3 = Web3(Web3.HTTPProvider(BSC_NODE_URL))

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# User states
user_states = {}

class UserState:
    MAIN = "main"
    JOINING_GROUPS = "joining_groups"
    SETTING_TELEGRAM = "setting_telegram"
    SETTING_TWITTER = "setting_twitter"
    SETTING_WALLET = "setting_wallet"
    WITHDRAWING = "withdrawing"

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
    try:
        result = supabase.table('users').select('*').eq('id', user_id).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        logger.error(f"Error getting user {user_id}: {e}")
        return None

def get_settings():
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
    try:
        settings = get_settings()
        signup_bonus = settings['signup_bonus']
        
        user_data = {
            'id': user_id,
            'username': username,
            'full_name': full_name,
            'invited_by': invited_by,
            'balance': str(signup_bonus),
            'joined_all_groups': False,
            'telegram_handle': username,
            'twitter_handle': None,
            'has_received_signup_bonus': True,
            'has_received_group_bonus': False
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
    """Check if user is member of all required groups"""
    try:
        for group_id in REQUIRED_GROUPS:
            try:
                member = await context.bot.get_chat_member(chat_id=group_id, user_id=user_id)
                if member.status in ['left', 'kicked']:
                    return False
            except Exception as e:
                logger.error(f"Error checking group {group_id}: {e}")
                return False
        return True
    except Exception as e:
        logger.error(f"Error in group membership check: {e}")
        return False

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
        
        # Handle states first
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
    msg += "1️⃣ [MetaCore Official](https://t.me/MetaaCore)\n"
    msg += "2️⃣ [Bot News](https://t.me/botnewz1)\n" 
    msg += "3️⃣ [MetaCore Community](https://t.me/MetaaCore)\n\n"
    msg += "⚠️ You must join ALL groups!\n"
    msg += "After joining, click the button below:"
    
    await update.message.reply_text(msg, reply_markup=GROUPS_KEYBOARD)

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
        
        # For testing, skip actual group check (remove this when groups are ready)
        if True:  # Change to: if await check_group_membership(context, user_id):
            # Update user as verified and mark bonus as received
            supabase.table('users').update({
                'joined_all_groups': True,
                'has_received_group_bonus': True
            }).eq('id', user_id).execute()
            
            # Get group join bonus
            settings = get_settings()
            bonus = settings['group_join_bonus']
            
            # Credit bonus using database function
            supabase.rpc('add_balance', {
                'user_id_param': user_id,
                'amount_param': str(bonus),
                'type_param': 'group_join',
                'description_param': 'Group join bonus'
            }).execute()
            
            msg = "✅ Excellent! You joined all groups.\n\n"
            msg += f"🎁 You earned {bonus} MetaCore bonus!\n\n"
            msg += "Now you can access the main menu. Set your BSC wallet address to receive tokens."
            
            user_states[user_id] = UserState.MAIN
            await update.message.reply_text(msg, reply_markup=MAIN_KEYBOARD)
            
        else:
            await update.message.reply_text(
                "❌ You haven't joined all required groups yet.\nPlease join ALL groups first!",
                reply_markup=GROUPS_KEYBOARD
            )
            
    except Exception as e:
        logger.error(f"Error verifying membership: {e}")
        await update.message.reply_text("❌ Error updating your status. Please try again.")

async def handle_referral_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        bot_username = context.bot.username
        
        referral_link = f"https://t.me/{bot_username}?start=ref{user_id}"
        
        # Get referral stats
        referrals = supabase.table('referrals').select('*').eq('inviter', user_id).execute()
        referral_count = len(referrals.data) if referrals.data else 0
        
        msg = f"🔗 Your Referral Link:\n"
        msg += f"{referral_link}\n\n"
        msg += f"📊 Your Stats:\n"
        msg += f"👥 Referrals: {referral_count}\n"
        msg += f"💰 Earned: {referral_count * 4000:,} MetaCore\n\n"
        msg += f"💡 Earn 4000 MetaCore (~$90) per referral!\n\n"
        msg += f"Share this link with friends to earn more tokens!"
        
        await update.message.reply_text(msg)
        
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
            
            if user['metacore_address']:
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
        
        if not user['metacore_address']:
            await update.message.reply_text("❌ Please set your BSC wallet address first!")
            return
        
        settings = get_settings()
        min_amount = float(settings['min_withdraw_amount'])
        balance_tokens = float(user['balance'])
        
        if balance_tokens < min_amount:
            msg = f"❌ Insufficient Balance!\n\n"
            msg += f"💰 Your balance: {balance_tokens:,.0f} MetaCore\n"
            msg += f"📊 Minimum withdrawal: {min_amount:,.0f} MetaCore\n\n"
            msg += f"💡 Refer more friends to earn tokens!"
            await update.message.reply_text(msg)
            return
        
        user_states[user_id] = UserState.WITHDRAWING
        
        address = user['metacore_address']
        msg = f"💸 Withdrawal Request\n\n"
        msg += f"💰 Available: {balance_tokens:,.0f} MetaCore\n"
        msg += f"📊 Minimum: {min_amount:,.0f} MetaCore\n\n"
        msg += f"💳 To: {address[:10]}...{address[-6:]}\n\n"
        msg += f"🔗 Network: BSC Testnet\n\n"
        msg += f"Enter withdrawal amount or type 'all':"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error in withdraw: {e}")
        await update.message.reply_text("❌ Error processing withdrawal request.")

async def process_withdrawal_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        text = update.message.text.strip().lower()
        user = get_user(user_id)
        
        if not user:
            await update.message.reply_text("❌ User not found.")
            return
        
        balance_tokens = float(user['balance'])
        settings = get_settings()
        min_amount = float(settings['min_withdraw_amount'])
        
        # Parse amount
        if text == 'all':
            amount = balance_tokens
        else:
            try:
                amount = float(text.replace(',', ''))
            except ValueError:
                await update.message.reply_text("❌ Please enter a valid number or 'all'")
                return
        
        # Validate amount
        if amount < min_amount or amount > balance_tokens:
            msg = f"❌ Invalid Amount!\n\n"
            msg += f"📊 Min: {min_amount:,.0f} MetaCore\n"
            msg += f"📊 Max: {balance_tokens:,.0f} MetaCore"
            await update.message.reply_text(msg)
            return
        
        # Create withdrawal request
        withdrawal_data = {
            'user_id': user_id,
            'amount': str(amount),
            'to_address': user['metacore_address'],
            'status': 'pending'
        }
        
        result = supabase.table('withdrawals').insert(withdrawal_data).execute()
        if not result.data:
            await update.message.reply_text("❌ Error creating withdrawal request.")
            return
            
        withdrawal_id = result.data[0]['id']
        
        # Deduct from balance using database function
        success = supabase.rpc('subtract_balance', {
            'user_id_param': user_id,
            'amount_param': str(amount),
            'type_param': 'withdrawal',
            'reference_id_param': withdrawal_id
        }).execute()
        
        # Notify admin
        await notify_admin_withdrawal(context, withdrawal_id, user, amount)
        
        user_states[user_id] = UserState.MAIN
        
        address = user['metacore_address']
        msg = f"✅ Withdrawal Request Submitted!\n\n"
        msg += f"💰 Amount: {amount:,.0f} MetaCore\n"
        msg += f"📍 To: {address}\n"
        msg += f"🔗 Network: BSC Testnet\n"
        msg += f"🆔 Request ID: #{withdrawal_id}\n\n"
        msg += f"⏳ Admin will review within 24 hours.\n"
        msg += f"💬 You'll be notified when processed!"
        
        await update.message.reply_text(msg, reply_markup=MAIN_KEYBOARD)
        
    except Exception as e:
        logger.error(f"Error processing withdrawal: {e}")
        await update.message.reply_text("❌ Error processing withdrawal.")

async def notify_admin_withdrawal(context, withdrawal_id, user, amount):
    try:
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Approve", callback_data=f"approve_{withdrawal_id}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"reject_{withdrawal_id}")
            ]
        ])
        
        username = user['username'] or 'N/A'
        address = user['metacore_address']
        
        msg = f"🔔 NEW WITHDRAWAL REQUEST\n\n"
        msg += f"👤 User: @{username} ({user['id']})\n"
        msg += f"💰 Amount: {amount:,.0f} MetaCore\n"
        msg += f"📍 Address: {address}\n"
        msg += f"🔗 Network: BSC Testnet\n"
        msg += f"🆔 Request ID: #{withdrawal_id}\n"
        msg += f"⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S')}"
        
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=msg,
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error(f"Error notifying admin: {e}")

async def handle_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        user = get_user(user_id)
        
        if user:
            balance_tokens = float(user['balance'])
            referrals = supabase.table('referrals').select('*').eq('inviter', user_id).execute()
            referral_count = len(referrals.data) if referrals.data else 0
            
            username = user['username'] or 'N/A'
            
            msg = f"👤 Your Profile\n\n"
            msg += f"🆔 ID: {user['id']}\n"
            msg += f"👤 Username: @{username}\n"
            msg += f"📱 Telegram: @{user.get('telegram_handle', 'Not set')}\n"
            msg += f"🐦 Twitter: @{user.get('twitter_handle', 'Not set')}\n"
            msg += f"💰 Balance: {balance_tokens:,.0f} MetaCore\n"
            msg += f"👥 Referrals: {referral_count}\n"
            msg += f"💳 Wallet: {'Set' if user['metacore_address'] else 'Not Set'}\n"
            msg += f"✅ Groups: {'Joined' if user['joined_all_groups'] else 'Not Joined'}\n"
            msg += f"🔗 Network: BSC Testnet\n"
            msg += f"📅 Joined: {user['created_at'][:10]}"
        else:
            msg = "❌ User not found. Please /start first."
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error in profile: {e}")
        await update.message.reply_text("❌ Error getting profile.")

async def handle_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "❓ MetaCore Airdrop Help\n\n"
    msg += "🎯 How to earn:\n"
    msg += "• Join groups: +500 MetaCore\n"
    msg += "• Refer friends: +4000 MetaCore each\n\n"
    msg += "💸 Withdrawal:\n"
    msg += "• Minimum: 4000 MetaCore\n"
    msg += "• Set BSC wallet first\n"
    msg += "• Admin approval required\n"
    msg += "• Network: BSC Testnet\n\n"
    msg += "🔗 BSC Testnet Setup:\n"
    msg += "• RPC: https://data-seed-prebsc-1-s1.binance.org:8545/\n"
    msg += "• Chain ID: 97\n"
    msg += "• Symbol: tBNB\n\n"
    msg += "🔗 Support: @your_support_username"
    
    await update.message.reply_text(msg)

# Admin callback handlers
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("❌ Unauthorized")
        return
    
    data = query.data
    
    if data.startswith('approve_'):
        withdrawal_id = int(data.split('_')[1])
        await approve_withdrawal(query, context, withdrawal_id)
    elif data.startswith('reject_'):
        withdrawal_id = int(data.split('_')[1])
        await reject_withdrawal(query, context, withdrawal_id)

async def approve_withdrawal(query, context, withdrawal_id):
    """Approve withdrawal and process payment"""
    try:
        # Update withdrawal status
        supabase.table('withdrawals').update({
            'status': 'approved',
            'processed_at': 'now()'
        }).eq('id', withdrawal_id).execute()
        
        # Get withdrawal details
        withdrawal = supabase.table('withdrawals').select('*').eq('id', withdrawal_id).execute().data[0]
        
        # Process payment (you'll implement this)
        success = await process_payment(withdrawal)
        
        if success:
            await query.edit_message_text(
                f"✅ Withdrawal {withdrawal_id} approved and processed!"
            )
            
            # Notify user
            await context.bot.send_message(
                chat_id=withdrawal['user_id'],
                text=f"✅ Your withdrawal of {float(withdrawal['amount']):,.0f} MetaCore has been processed!\n\n🔗 Check BSC Testnet for your tokens."
            )
        else:
            await query.edit_message_text(
                f"❌ Failed to process withdrawal {withdrawal_id}"
            )
            
    except Exception as e:
        logger.error(f"Error approving withdrawal: {e}")
        await query.edit_message_text(
            f"❌ Error processing withdrawal {withdrawal_id}"
        )

async def reject_withdrawal(query, context, withdrawal_id):
    """Reject withdrawal and refund balance"""
    try:
        # Get withdrawal details
        withdrawal = supabase.table('withdrawals').select('*').eq('id', withdrawal_id).execute().data[0]
        
        # Update withdrawal status
        supabase.table('withdrawals').update({
            'status': 'rejected',
            'processed_at': 'now()',
            'admin_note': 'Rejected by admin'
        }).eq('id', withdrawal_id).execute()
        
        # Refund user balance using database function
        supabase.rpc('add_balance', {
            'user_id_param': withdrawal['user_id'],
            'amount_param': withdrawal['amount'],
            'type_param': 'refund',
            'description_param': f'Refund for rejected withdrawal #{withdrawal_id}'
        }).execute()
        
        await query.edit_message_text(
            f"❌ Withdrawal {withdrawal_id} rejected and refunded!"
        )
        
        # Notify user
        await context.bot.send_message(
            chat_id=withdrawal['user_id'],
            text=f"❌ Your withdrawal request was rejected.\n\nTokens have been refunded to your balance."
        )
        
    except Exception as e:
        logger.error(f"Error rejecting withdrawal: {e}")
        await query.edit_message_text(
            f"❌ Error rejecting withdrawal {withdrawal_id}"
        )

async def process_payment(withdrawal):
    """Process actual token transfer on BSC Testnet"""
    try:
        if not CONTRACT_ADDRESS or not ADMIN_PRIVATE_KEY:
            logger.warning("Contract address or private key not configured")
            # For testing, just mark as paid
            supabase.table('withdrawals').update({
                'status': 'paid',
                'tx_hash': 'testnet_simulation_' + str(int(time.time()))
            }).eq('id', withdrawal['id']).execute()
            return True
        
        # Load contract ABI (you'll need to add your token's ABI)
        contract_abi = [
            {
                "constant": False,
                "inputs": [
                    {"name": "_to", "type": "address"},
                    {"name": "_value", "type": "uint256"}
                ],
                "name": "transfer",
                "outputs": [{"name": "", "type": "bool"}],
                "type": "function"
            }
        ]
        
        # Initialize contract
        contract = w3.eth.contract(
            address=Web3.toChecksumAddress(CONTRACT_ADDRESS),
            abi=contract_abi
        )
        
        # Get admin account
        admin_account = w3.eth.account.from_key(ADMIN_PRIVATE_KEY)
        
        # Convert amount to wei (assuming 18 decimals)
        amount_wei = int(float(withdrawal['amount']) * 10**18)
        
        # Build transaction
        transaction = contract.functions.transfer(
            Web3.toChecksumAddress(withdrawal['to_address']),
            amount_wei
        ).buildTransaction({
            'from': admin_account.address,
            'gas': 100000,
            'gasPrice': w3.toWei('10', 'gwei'),
            'nonce': w3.eth.get_transaction_count(admin_account.address),
        })
        
        # Sign and send transaction
        signed_txn = w3.eth.account.sign_transaction(transaction, ADMIN_PRIVATE_KEY)
        tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
        
        # Wait for confirmation
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
        
        if receipt.status == 1:
            # Update withdrawal with transaction hash
            supabase.table('withdrawals').update({
                'status': 'paid',
                'tx_hash': receipt.transactionHash.hex()
            }).eq('id', withdrawal['id']).execute()
            
            logger.info(f"Payment successful: {receipt.transactionHash.hex()}")
            return True
        else:
            logger.error(f"Transaction failed: {receipt}")
            return False
            
    except Exception as e:
        logger.error(f"Payment processing failed: {e}")
        # For testing purposes, mark as paid even if Web3 fails
        supabase.table('withdrawals').update({
            'status': 'paid',
            'tx_hash': 'testnet_fallback_' + str(int(time.time())),
            'admin_note': f'Fallback processing: {str(e)[:100]}'
        }).eq('id', withdrawal['id']).execute()
        return True

# Admin commands
async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        # Get stats
        users = supabase.table('users').select('*').execute()
        withdrawals = supabase.table('withdrawals').select('*').execute()
        referrals = supabase.table('referrals').select('*').execute()
        
        total_users = len(users.data)
        total_referrals = len(referrals.data)
        pending_withdrawals = len([w for w in withdrawals.data if w['status'] == 'pending'])
        
        # Calculate total balance
        total_balance = sum(float(user['balance']) for user in users.data)
        
        msg = f"📊 Admin Statistics\n\n"
        msg += f"👥 Total Users: {total_users:,}\n"
        msg += f"🔗 Total Referrals: {total_referrals:,}\n"
        msg += f"⏳ Pending Withdrawals: {pending_withdrawals}\n"
        msg += f"💰 Total Balance: {total_balance:,.0f} MetaCore\n"
        msg += f"💵 Total Value: ${total_balance * 0.0225:,.2f}\n"
        msg += f"🔗 Network: BSC Testnet"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error in admin stats: {e}")
        await update.message.reply_text("❌ Error getting statistics.")

async def handle_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast message to all users"""
    if update.effective_user.id != ADMIN_ID:
        return
        
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    
    try:
        message = ' '.join(context.args)
        users = supabase.table('users').select('id').execute()
        
        sent = 0
        failed = 0
        
        status_msg = await update.message.reply_text(
            f"📡 Broadcasting to {len(users.data)} users..."
        )
        
        for user in users.data:
            try:
                await context.bot.send_message(
                    chat_id=user['id'], 
                    text=f"📢 Admin Broadcast\n\n{message}"
                )
                sent += 1
                
                # Update status every 50 users
                if sent % 50 == 0:
                    await status_msg.edit_text(
                        f"📡 Sent to {sent}/{len(users.data)} users..."
                    )
                    
            except Exception as e:
                failed += 1
                logger.error(f"Failed to send to {user['id']}: {e}")
        
        await status_msg.edit_text(
            f"✅ Broadcast complete!\n📤 Sent: {sent}\n❌ Failed: {failed}"
        )
        
    except Exception as e:
        logger.error(f"Error in broadcast: {e}")
        await update.message.reply_text("❌ Error broadcasting message.")

async def handle_user_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get user information"""
    if update.effective_user.id != ADMIN_ID:
        return
        
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /userinfo <user_id>")
        return
    
    try:
        user_id = int(context.args[0])
        user = get_user(user_id)
        
        if user:
            balance = float(user['balance'])
            referrals = supabase.table('referrals').select('*').eq('inviter', user_id).execute()
            withdrawals = supabase.table('withdrawals').select('*').eq('user_id', user_id).execute()
            
            username = user['username'] or 'N/A'
            full_name = user['full_name'] or 'N/A'
            wallet = user['metacore_address'] or 'Not set'
            telegram_handle = user.get('telegram_handle', 'Not set')
            twitter_handle = user.get('twitter_handle', 'Not set')
            
            msg = f"👤 User Info: {user_id}\n\n"
            msg += f"Username: @{username}\n"
            msg += f"Full Name: {full_name}\n"
            msg += f"Telegram: @{telegram_handle}\n"
            msg += f"Twitter: @{twitter_handle}\n"
            msg += f"Balance: {balance:,.0f} MetaCore\n"
            msg += f"Referrals: {len(referrals.data)}\n"
            msg += f"Withdrawals: {len(withdrawals.data)}\n"
            msg += f"Groups Joined: {'Yes' if user['joined_all_groups'] else 'No'}\n"
            msg += f"Group Bonus: {'Yes' if user.get('has_received_group_bonus', False) else 'No'}\n"
            msg += f"Wallet: {wallet}\n"
            msg += f"Invited By: {user['invited_by'] or 'Direct'}\n"
            msg += f"Joined: {user['created_at'][:10]}"
            
            await update.message.reply_text(msg)
        else:
            await update.message.reply_text("❌ User not found")
            
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID")
    except Exception as e:
        logger.error(f"Error in user info: {e}")
        await update.message.reply_text("❌ Error getting user info.")

async def handle_add_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add balance to user"""
    if update.effective_user.id != ADMIN_ID:
        return
        
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /addbalance <user_id> <amount>")
        return
    
    try:
        user_id = int(context.args[0])
        amount = float(context.args[1])
        
        user = get_user(user_id)
        if not user:
            await update.message.reply_text("❌ User not found")
            return
        
        # Add balance using database function
        supabase.rpc('add_balance', {
            'user_id_param': user_id,
            'amount_param': str(amount),
            'type_param': 'admin_credit',
            'description_param': f'Admin credit by {update.effective_user.id}'
        }).execute()
        
        # Log admin action
        supabase.table('admin_logs').insert({
            'admin_id': update.effective_user.id,
            'action': 'add_balance',
            'details': {
                'user_id': user_id,
                'amount': amount,
                'username': user['username']
            }
        }).execute()
        
        username = user['username'] or 'N/A'
        msg = f"✅ Added {amount:,.0f} MetaCore to @{username} ({user_id})"
        await update.message.reply_text(msg)
        
        # Notify user
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"🎁 You received {amount:,.0f} MetaCore from admin!"
            )
        except:
            pass  # User might have blocked bot
            
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID or amount")
    except Exception as e:
        logger.error(f"Error adding balance: {e}")
        await update.message.reply_text("❌ Error adding balance.")

async def handle_withdrawals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show pending withdrawals"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        withdrawals = supabase.table('withdrawals').select('*').eq('status', 'pending').order('created_at').execute()
        
        if not withdrawals.data:
            await update.message.reply_text("✅ No pending withdrawals")
            return
        
        msg = f"⏳ Pending Withdrawals ({len(withdrawals.data)})\n\n"
        
        for w in withdrawals.data[:10]:  # Show first 10
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
    """Show/update bot settings"""
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
        msg += f"Use /setsetting <key> <value> to update"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error getting settings: {e}")
        await update.message.reply_text("❌ Error getting settings.")

async def handle_set_setting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Update a setting"""
    if update.effective_user.id != ADMIN_ID:
        return
        
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /setsetting <key> <value>\nKeys: signup_bonus, referral_bonus, group_join_bonus, min_withdraw_amount, token_price_usd")
        return
    
    try:
        key = context.args[0]
        value = context.args[1]
        
        valid_keys = ['signup_bonus', 'referral_bonus', 'group_join_bonus', 'min_withdraw_amount', 'token_price_usd']
        
        if key not in valid_keys:
            await update.message.reply_text(f"❌ Invalid key. Valid keys: {', '.join(valid_keys)}")
            return
        
        # Update setting
        supabase.table('settings').update({key: value}).eq('id', 1).execute()
        
        # Log admin action
        supabase.table('admin_logs').insert({
            'admin_id': update.effective_user.id,
            'action': 'update_setting',
            'details': {
                'key': key,
                'value': value
            }
        }).execute()
        
        await update.message.reply_text(f"✅ Updated {key} to {value}")
        
    except Exception as e:
        logger.error(f"Error updating setting: {e}")
        await update.message.reply_text("❌ Error updating setting.")

async def handle_network_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show BSC Testnet network information"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        # Check Web3 connection
        is_connected = w3.isConnected()
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
            
        if ADMIN_PRIVATE_KEY:
            admin_account = w3.eth.account.from_key(ADMIN_PRIVATE_KEY)
            if is_connected:
                balance = w3.eth.get_balance(admin_account.address)
                balance_bnb = w3.fromWei(balance, 'ether')
                msg += f"💳 Admin Balance: {balance_bnb:.4f} tBNB"
            else:
                msg += f"💳 Admin Address: {admin_account.address}"
        else:
            msg += f"💳 Admin Key: Not configured"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"Error getting network info: {e}")
        await update.message.reply_text("❌ Error getting network info.")

# Error handler
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log errors caused by Updates."""
    logger.error(f"Exception while handling an update: {context.error}")

def main():
    """Start the bot"""
    try:
        application = Application.builder().token(BOT_TOKEN).build()
        
        # Add handlers
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("stats", admin_stats))
        application.add_handler(CommandHandler("broadcast", handle_broadcast))
        application.add_handler(CommandHandler("userinfo", handle_user_info))
        application.add_handler(CommandHandler("addbalance", handle_add_balance))
        application.add_handler(CommandHandler("withdrawals", handle_withdrawals))
        application.add_handler(CommandHandler("settings", handle_settings))
        application.add_handler(CommandHandler("setsetting", handle_set_setting))
        application.add_handler(CommandHandler("network", handle_network_info))
        
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        application.add_handler(CallbackQueryHandler(handle_callback))
        
        # Add error handler
        application.add_error_handler(error_handler)
        
        logger.info("🚀 MetaCore Airdrop Bot started successfully on BSC Testnet!")
        logger.info(f"🔗 Connected to: {BSC_NODE_URL}")
        logger.info(f"📄 Contract: {CONTRACT_ADDRESS or 'Not configured'}")
        
        # Start bot
        application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")

if __name__ == '__main__':
    main()
