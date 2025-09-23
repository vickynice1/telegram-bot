import os
from web3 import Web3
from supabase import create_client
import json
import time
import logging
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from dateutil import parser  # Add this for robust datetime parsing

# Setup logging for GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BSC_NODE_URL = os.getenv("BSC_NODE_URL", "https://data-seed-prebsc-1-s1.binance.org:8545/")
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS")
ADMIN_PRIVATE_KEY = os.getenv("ADMIN_PRIVATE_KEY")
ADMIN_ADDRESS = os.getenv("ADMIN_ADDRESS")

# Determine network
IS_TESTNET = "testnet" in BSC_NODE_URL or "prebsc" in BSC_NODE_URL
CHAIN_ID = 97 if IS_TESTNET else 56
NETWORK_NAME = "BSC Testnet" if IS_TESTNET else "BSC Mainnet"

logger.info(f"🔗 Network: {NETWORK_NAME} (Chain ID: {CHAIN_ID})")

# Validate required environment variables
required_vars = [SUPABASE_URL, SUPABASE_KEY, CONTRACT_ADDRESS, ADMIN_PRIVATE_KEY]
missing_vars = [var for var in required_vars if not var]

if missing_vars:
    logger.error(f"❌ Missing required environment variables: {missing_vars}")
    exit(1)

# Initialize
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    w3 = Web3(Web3.HTTPProvider(BSC_NODE_URL))
    
    if not w3.is_connected():
        logger.error("❌ Failed to connect to BSC node")
        exit(1)
    
    logger.info(f"✅ Connected to {NETWORK_NAME}. Latest block: {w3.eth.block_number}")
    
except Exception as e:
    logger.error(f"❌ Initialization failed: {e}")
    exit(1)

# ERC20 ABI
ERC20_ABI = json.loads('''[
    {
        "constant": false,
        "inputs": [
            {"name": "_to", "type": "address"},
            {"name": "_value", "type": "uint256"}
        ],
        "name": "transfer",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    }
]''')

def get_contract():
    """Get contract instance"""
    try:
        return w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDRESS), 
            abi=ERC20_ABI
        )
    except Exception as e:
        logger.error(f"Error getting contract: {e}")
        return None

def get_admin_account():
    """Get admin account from private key"""
    try:
        return w3.eth.account.from_key(ADMIN_PRIVATE_KEY)
    except Exception as e:
        logger.error(f"Error getting admin account: {e}")
        return None

def check_contract_balance():
    """Check admin wallet token balance"""
    try:
        contract = get_contract()
        admin_account = get_admin_account()
        
        if not contract or not admin_account:
            return 0
        
        balance = contract.functions.balanceOf(admin_account.address).call()
        decimals = contract.functions.decimals().call()
        token_balance = balance / (10 ** decimals)
        
        logger.info(f"💰 Admin wallet balance: {token_balance:,.2f} tokens")
        return token_balance
        
    except Exception as e:
        logger.error(f"Error checking balance: {e}")
        return 0

def send_tokens(to_address, amount_tokens):
    """Send tokens via Web3"""
    try:
        logger.info(f"💰 Sending {amount_tokens} tokens to {to_address}")
        
        contract = get_contract()
        admin_account = get_admin_account()
        
        if not contract or not admin_account:
            logger.error("Failed to get contract or admin account")
            return None
        
        # Convert to checksum address
        to_address = Web3.to_checksum_address(to_address)
        
        # Get token decimals
        decimals = contract.functions.decimals().call()
        amount_wei = int(Decimal(str(amount_tokens)) * (10 ** decimals))
        
        # Check balance
        admin_balance = contract.functions.balanceOf(admin_account.address).call()
        if admin_balance < amount_wei:
            logger.error(f"Insufficient balance. Need: {amount_wei}, Have: {admin_balance}")
            return None
        
        # Get transaction parameters
        nonce = w3.eth.get_transaction_count(admin_account.address, 'pending')
        gas_price = w3.eth.gas_price
        
        # Build transaction
        transaction = contract.functions.transfer(
            to_address, amount_wei
        ).build_transaction({
            'chainId': CHAIN_ID,
            'gas': 200000,
            'gasPrice': int(gas_price * 1.2),
            'nonce': nonce,
        })
        
        # Sign and send
        signed_txn = w3.eth.account.sign_transaction(transaction, ADMIN_PRIVATE_KEY)
        tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
        tx_hash_hex = tx_hash.hex()
        
        logger.info(f"📤 Transaction sent: {tx_hash_hex}")
        
        # Wait for confirmation
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
        
        if receipt and receipt.status == 1:
            logger.info(f"✅ Transaction confirmed: {tx_hash_hex}")
            return tx_hash_hex
        else:
            logger.error(f"❌ Transaction failed: {tx_hash_hex}")
            return None
            
    except Exception as e:
        logger.error(f"Error sending tokens: {e}")
        return None

def validate_withdrawal_request(withdrawal):
    """Validate withdrawal before processing"""
    try:
        # Check if address is valid
        Web3.to_checksum_address(withdrawal['to_address'])
        
        # Check amount is positive
        amount = float(withdrawal['amount'])
        if amount <= 0:
            return False, "Invalid amount"
        
        # Check admin balance
        admin_balance = check_contract_balance()
        if admin_balance < amount:
            return False, f"Insufficient admin balance: {admin_balance}"
        
        return True, "Valid"
        
    except Exception as e:
        return False, f"Validation error: {e}"

def process_single_batch():
    """Process a single batch of withdrawals (GitHub Actions optimized)"""
    try:
        logger.info("🔍 Checking for approved withdrawals...")
        
        # Get up to 5 approved withdrawals
        withdrawals = supabase.table('withdrawals').select('*').eq('status', 'approved').order('created_at').limit(5).execute()
        
        if not withdrawals.data:
            logger.info("✅ No approved withdrawals to process")
            return
        
        logger.info(f"📋 Processing {len(withdrawals.data)} withdrawals")
        
        # Check admin balance first
        admin_balance = check_contract_balance()
        total_needed = sum(float(w['amount']) for w in withdrawals.data)
        
        if admin_balance < total_needed:
            logger.warning(f"⚠️ Insufficient balance. Need: {total_needed}, Have: {admin_balance}")
        
        successful = 0
        failed = 0
        
        for withdrawal in withdrawals.data:
            try:
                withdrawal_id = withdrawal['id']
                to_address = withdrawal['to_address']
                amount = float(withdrawal['amount'])
                user_id = withdrawal['user_id']
                
                logger.info(f"💳 Processing withdrawal {withdrawal_id}: {amount} tokens to {to_address}")
                
                # Validate withdrawal
                is_valid, validation_msg = validate_withdrawal_request(withdrawal)
                if not is_valid:
                    logger.error(f"❌ Validation failed for {withdrawal_id}: {validation_msg}")
                    
                    # Mark as failed and refund
                    supabase.table('withdrawals').update({
                        'status': 'failed',
                        'admin_note': f'Validation failed: {validation_msg}'
                    }).eq('id', withdrawal_id).execute()
                    
                    # Refund user
                    supabase.rpc('add_balance', {
                        'user_id_param': user_id,
                        'amount_param': str(amount),
                        'type_param': 'refund',
                        'description_param': f'Refund for failed withdrawal #{withdrawal_id}'
                    }).execute()
                    
                    failed += 1
                    continue
                
                # Mark as processing
                supabase.table('withdrawals').update({
                    'status': 'processing',
                    'processed_at': 'now()'
                }).eq('id', withdrawal_id).execute()
                
                # Send actual transaction
                tx_hash = send_tokens(to_address, amount)
                
                if tx_hash:
                    # Mark as paid
                    supabase.table('withdrawals').update({
                        'status': 'paid',
                        'tx_hash': tx_hash,
                        'processed_at': 'now()',
                        'network': NETWORK_NAME
                    }).eq('id', withdrawal_id).execute()
                    
                    # Log transaction
                    supabase.table('transactions').insert({
                        'user_id': user_id,
                        'type': 'withdrawal_paid',
                        'amount': str(-amount),
                        'description': f'Withdrawal paid on {NETWORK_NAME} - TX: {tx_hash}',
                        'reference_id': withdrawal_id
                    }).execute()
                    
                    logger.info(f"✅ Withdrawal {withdrawal_id} processed successfully: {tx_hash}")
                    successful += 1
                else:
                    # Mark as failed and refund
                    supabase.table('withdrawals').update({
                        'status': 'failed',
                        'admin_note': f'Transaction failed on {NETWORK_NAME}'
                    }).eq('id', withdrawal_id).execute()
                    
                    # Refund user balance
                    supabase.rpc('add_balance', {
                        'user_id_param': user_id,
                        'amount_param': str(amount),
                        'type_param': 'refund',
                        'description_param': f'Refund for failed withdrawal #{withdrawal_id}'
                    }).execute()
                    
                    logger.error(f"❌ Failed to process withdrawal {withdrawal_id}")
                    failed += 1
                
                # Delay between transactions
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"❌ Error processing withdrawal {withdrawal['id']}: {e}")
                
                # Mark as failed and refund
                try:
                    supabase.table('withdrawals').update({
                        'status': 'failed',
                        'admin_note': f'Processing error: {str(e)[:200]}'
                    }).eq('id', withdrawal['id']).execute()
                    
                    supabase.rpc('add_balance', {
                        'user_id_param': withdrawal['user_id'],
                        'amount_param': str(withdrawal['amount']),
                        'type_param': 'refund',
                        'description_param': f'Refund for failed withdrawal #{withdrawal["id"]}'
                    }).execute()
                except Exception as refund_error:
                    logger.error(f"Error refunding: {refund_error}")
                
                failed += 1
        
        logger.info(f"📊 Batch complete: {successful} successful, {failed} failed")
        
    except Exception as e:
        logger.error(f"❌ Error in process_single_batch: {e}")

def cleanup_stuck_withdrawals():
    """Clean up withdrawals stuck in processing"""
    try:
        # Import timezone for proper datetime handling
        from datetime import timezone
        
        # Create timezone-aware cutoff time
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        
        result = supabase.table('withdrawals').select('*').eq('status', 'processing').execute()
        
        cleaned = 0
        for withdrawal in result.data:
            try:
                processed_at = withdrawal.get('processed_at')
                if processed_at:
                    # Handle different timestamp formats from Supabase
                    if isinstance(processed_at, str):
                        # Remove 'Z' and add timezone info, or handle +00:00 format
                        if processed_at.endswith('Z'):
                            processed_at = processed_at.replace('Z', '+00:00')
                        
                        # Parse the timestamp (this will be timezone-aware)
                        from dateutil import parser
                        processed_time = parser.parse(processed_at)
                    else:
                        # If it's already a datetime object, ensure it's timezone-aware
                        processed_time = processed_at
                        if processed_time.tzinfo is None:
                            processed_time = processed_time.replace(tzinfo=timezone.utc)
                    
                    # Now both datetimes are timezone-aware, safe to compare
                    if processed_time < cutoff_time:
                        # Mark as failed and refund
                        supabase.table('withdrawals').update({
                            'status': 'failed',
                            'admin_note': 'Stuck in processing - auto-failed'
                        }).eq('id', withdrawal['id']).execute()
                        
                        # Refund balance
                        supabase.rpc('add_balance', {
                            'user_id_param': withdrawal['user_id'],
                            'amount_param': str(withdrawal['amount']),
                            'type_param': 'refund',
                            'description_param': f'Auto-refund for stuck withdrawal #{withdrawal["id"]}'
                        }).execute()
                        
                        cleaned += 1
                        logger.info(f"🧹 Cleaned stuck withdrawal {withdrawal['id']}")
                        
            except Exception as e:
                logger.error(f"Error cleaning withdrawal {withdrawal['id']}: {e}")
        
        if cleaned > 0:
            logger.info(f"🧹 Cleaned up {cleaned} stuck withdrawals")
        else:
            logger.info("✅ No stuck withdrawals found")
        
    except Exception as e:
        logger.error(f"Error in cleanup: {e}")

if __name__ == "__main__":
    logger.info("🚀 Payment processor started (GitHub Actions mode)")
    
    try:
        # Process current batch
        process_single_batch()
        
        # Cleanup stuck transactions
        cleanup_stuck_withdrawals()
        
        logger.info("✅ Payment processor completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Payment processor failed: {e}")
        exit(1)
