import os
from web3 import Web3
from supabase import create_client
import json
import time
import logging
from decimal import Decimal
from datetime import datetime, timedelta

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

# Your existing ERC20_ABI and functions here...
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

# Copy all your existing functions here (get_contract, get_admin_account, etc.)
def get_contract():
    try:
        return w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDRESS), 
            abi=ERC20_ABI
        )
    except Exception as e:
        logger.error(f"Error getting contract: {e}")
        return None

def get_admin_account():
    try:
        return w3.eth.account.from_key(ADMIN_PRIVATE_KEY)
    except Exception as e:
        logger.error(f"Error getting admin account: {e}")
        return None

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
        
        successful = 0
        failed = 0
        
        for withdrawal in withdrawals.data:
            try:
                withdrawal_id = withdrawal['id']
                to_address = withdrawal['to_address']
                amount = float(withdrawal['amount'])
                user_id = withdrawal['user_id']
                
                logger.info(f"💳 Processing withdrawal {withdrawal_id}: {amount} tokens to {to_address}")
                
                # Mark as processing
                supabase.table('withdrawals').update({
                    'status': 'processing',
                    'processed_at': 'now()'
                }).eq('id', withdrawal_id).execute()
                
                # Simulate payment for now (replace with actual send_tokens function)
                # tx_hash = send_tokens(to_address, amount)
                
                # For testing - simulate successful transaction
                tx_hash = f"0x{''.join(['a' if i % 2 == 0 else 'b' for i in range(64)])}"
                
                if tx_hash:
                    # Mark as paid
                    supabase.table('withdrawals').update({
                        'status': 'paid',
                        'tx_hash': tx_hash,
                        'processed_at': 'now()',
                        'network': NETWORK_NAME
                    }).eq('id', withdrawal_id).execute()
                    
                    logger.info(f"✅ Withdrawal {withdrawal_id} processed successfully")
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
                
                # Small delay between transactions
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ Error processing withdrawal {withdrawal['id']}: {e}")
                failed += 1
        
        logger.info(f"📊 Batch complete: {successful} successful, {failed} failed")
        
    except Exception as e:
        logger.error(f"❌ Error in process_single_batch: {e}")

def cleanup_stuck_withdrawals():
    """Clean up withdrawals stuck in processing"""
    try:
        cutoff_time = datetime.now() - timedelta(minutes=10)
        
        result = supabase.table('withdrawals').select('*').eq('status', 'processing').execute()
        
        cleaned = 0
        for withdrawal in result.data:
            try:
                processed_at = withdrawal.get('processed_at')
                if processed_at:
                    processed_time = datetime.fromisoformat(processed_at.replace('Z', '+00:00'))
                    
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
                        
            except Exception as e:
                logger.error(f"Error cleaning withdrawal {withdrawal['id']}: {e}")
        
        if cleaned > 0:
            logger.info(f"🧹 Cleaned up {cleaned} stuck withdrawals")
        
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
