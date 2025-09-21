import os
from web3 import Web3
from supabase import create_client
import json
import time
import logging
from decimal import Decimal
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('payment_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration - FIXED: Use testnet by default
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
# FIXED: Default to testnet instead of mainnet
BSC_NODE_URL = os.getenv("BSC_NODE_URL", "https://data-seed-prebsc-1-s1.binance.org:8545/")
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS")
ADMIN_PRIVATE_KEY = os.getenv("ADMIN_PRIVATE_KEY")
ADMIN_ADDRESS = os.getenv("ADMIN_ADDRESS")

# FIXED: Determine network based on URL
IS_TESTNET = "testnet" in BSC_NODE_URL or "prebsc" in BSC_NODE_URL
CHAIN_ID = 97 if IS_TESTNET else 56
NETWORK_NAME = "BSC Testnet" if IS_TESTNET else "BSC Mainnet"

logger.info(f"Network: {NETWORK_NAME} (Chain ID: {CHAIN_ID})")

# Validate required environment variables
required_vars = [SUPABASE_URL, SUPABASE_KEY, CONTRACT_ADDRESS, ADMIN_PRIVATE_KEY]
missing_vars = [var for var in required_vars if not var]

if missing_vars:
    logger.error(f"Missing required environment variables: {missing_vars}")
    exit(1)

# Initialize
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    w3 = Web3(Web3.HTTPProvider(BSC_NODE_URL))
    
    # FIXED: Use is_connected() instead of is_connected
    if not w3.is_connected():
        logger.error("Failed to connect to BSC node")
        exit(1)
    
    logger.info(f"Connected to {NETWORK_NAME}. Latest block: {w3.eth.block_number}")
    
except Exception as e:
    logger.error(f"Initialization failed: {e}")
    exit(1)

# Extended ERC-20 ABI
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
    },
    {
        "constant": true,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    }
]''')

def get_contract():
    """Get contract instance"""
    try:
        # FIXED: Use Web3.to_checksum_address instead of Web3.toChecksumAddress
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
        
        # Convert from wei to tokens
        token_balance = balance / (10 ** decimals)
        
        logger.info(f"Admin wallet balance: {token_balance:,.2f} tokens")
        return token_balance
        
    except Exception as e:
        logger.error(f"Error checking balance: {e}")
        return 0

def get_gas_price():
    """Get current gas price with safety margin"""
    try:
        gas_price = w3.eth.gas_price
        # FIXED: Different gas prices for testnet vs mainnet
        if IS_TESTNET:
            # Testnet: use higher gas price for faster confirmation
            return max(int(gas_price * 1.5), w3.to_wei('10', 'gwei'))
        else:
            # Mainnet: be more conservative
            return int(gas_price * 1.2)
    except Exception as e:
        logger.error(f"Error getting gas price: {e}")
        # FIXED: Different fallback for testnet
        fallback_price = w3.to_wei('10', 'gwei') if IS_TESTNET else w3.to_wei('5', 'gwei')
        return fallback_price

def estimate_gas(contract, admin_address, to_address, amount):
    """Estimate gas for transaction"""
    try:
        gas_estimate = contract.functions.transfer(
            to_address, amount
        ).estimate_gas({'from': admin_address})
        
        # Add 30% margin for testnet, 20% for mainnet
        margin = 1.3 if IS_TESTNET else 1.2
        return int(gas_estimate * margin)
    except Exception as e:
        logger.error(f"Error estimating gas: {e}")
        # Higher fallback for testnet
        return 300000 if IS_TESTNET else 200000

def wait_for_transaction_receipt(tx_hash, timeout=300):
    """Wait for transaction confirmation"""
    try:
        # FIXED: Longer timeout for testnet
        actual_timeout = timeout * 2 if IS_TESTNET else timeout
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=actual_timeout)
        return receipt
    except Exception as e:
        logger.error(f"Error waiting for receipt: {e}")
        return None

def send_tokens(to_address, amount_tokens):
    """Send tokens via Web3"""
    try:
        logger.info(f"Sending {amount_tokens} tokens to {to_address} on {NETWORK_NAME}")
        
        # Get contract and admin account
        contract = get_contract()
        admin_account = get_admin_account()
        
        if not contract or not admin_account:
            logger.error("Failed to get contract or admin account")
            return None
        
        # Convert to checksum address
        to_address = Web3.to_checksum_address(to_address)
        
        # Get token decimals
        decimals = contract.functions.decimals().call()
        
        # Convert amount to wei (smallest unit)
        amount_wei = int(Decimal(str(amount_tokens)) * (10 ** decimals))
        
        logger.info(f"Amount in wei: {amount_wei}")
        
        # Check admin balance
        admin_balance = contract.functions.balanceOf(admin_account.address).call()
        if admin_balance < amount_wei:
            logger.error(f"Insufficient balance. Need: {amount_wei}, Have: {admin_balance}")
            return None
        
        # FIXED: Get nonce with proper parameter
        nonce = w3.eth.get_transaction_count(admin_account.address, 'pending')
        
        # Get gas price and estimate gas
        gas_price = get_gas_price()
        gas_limit = estimate_gas(contract, admin_account.address, to_address, amount_wei)
        
        logger.info(f"Gas price: {w3.from_wei(gas_price, 'gwei')} gwei, Gas limit: {gas_limit}")
        
        # FIXED: Use correct chain ID
        transaction = contract.functions.transfer(
            to_address,
            amount_wei
        ).build_transaction({
            'chainId': CHAIN_ID,  # Use dynamic chain ID
            'gas': gas_limit,
            'gasPrice': gas_price,
            'nonce': nonce,
        })
        
        logger.info(f"Transaction built for {NETWORK_NAME}")
        
        # Sign transaction
        signed_txn = w3.eth.account.sign_transaction(transaction, ADMIN_PRIVATE_KEY)
        
        # Send transaction
        tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
        tx_hash_hex = tx_hash.hex()
        
        logger.info(f"Transaction sent: {tx_hash_hex}")
        
        # Wait for confirmation
        receipt = wait_for_transaction_receipt(tx_hash, timeout=600)  # 10 minutes
        
        if receipt and receipt.status == 1:
            logger.info(f"✅ Transaction confirmed: {tx_hash_hex}")
            logger.info(f"Gas used: {receipt.gasUsed}/{gas_limit}")
            return tx_hash_hex
        else:
            logger.error(f"❌ Transaction failed: {tx_hash_hex}")
            return None
        
    except Exception as e:
        logger.error(f"Error sending tokens: {e}")
        return None

def process_approved_withdrawals():
    """Process all approved withdrawals"""
    try:
        # Get approved withdrawals ordered by creation time
        withdrawals = supabase.table('withdrawals').select('*').eq('status', 'approved').order('created_at').execute()
        
        if not withdrawals.data:
            logger.info("No approved withdrawals to process")
            return
        
        logger.info(f"Processing {len(withdrawals.data)} approved withdrawals")
        
        # Check admin balance before processing
        admin_balance = check_contract_balance()
        total_needed = sum(float(w['amount']) for w in withdrawals.data)
        
        if admin_balance < total_needed:
            logger.warning(f"⚠️ Insufficient admin balance. Need: {total_needed:,.2f}, Have: {admin_balance:,.2f}")
            # Continue processing what we can
        
        successful = 0
        failed = 0
        
        for withdrawal in withdrawals.data:
            try:
                withdrawal_id = withdrawal['id']
                to_address = withdrawal['to_address']
                amount = float(withdrawal['amount'])
                user_id = withdrawal['user_id']
                
                logger.info(f"Processing withdrawal {withdrawal_id}: {amount} tokens to {to_address}")
                
                # FIXED: Check if we have enough balance for this specific withdrawal
                current_balance = check_contract_balance()
                if current_balance < amount:
                    logger.warning(f"Insufficient balance for withdrawal {withdrawal_id}. Skipping.")
                    continue
                
                # Mark as processing
                supabase.table('withdrawals').update({
                    'status': 'processing',
                    'processed_at': 'now()'
                }).eq('id', withdrawal_id).execute()
                
                # Send transaction
                tx_hash = send_tokens(to_address, amount)
                
                if tx_hash:
                    # Mark as paid
                    supabase.table('withdrawals').update({
                        'status': 'paid',
                        'tx_hash': tx_hash,
                        'processed_at': 'now()',
                        'network': NETWORK_NAME
                    }).eq('id', withdrawal_id).execute()
                    
                    # Log successful transaction
                    supabase.table('transactions').insert({
                        'user_id': user_id,
                        'type': 'withdrawal_paid',
                        'amount': str(-amount),  # Negative for outgoing
                        'description': f'Withdrawal paid on {NETWORK_NAME} - TX: {tx_hash}',
                        'reference_id': withdrawal_id
                    }).execute()
                    
                    logger.info(f"✅ Withdrawal {withdrawal_id} processed successfully: {tx_hash}")
                    successful += 1
                    
                    # FIXED: Longer delay between transactions for testnet
                    delay = 10 if IS_TESTNET else 5
                    time.sleep(delay)
                    
                else:
                    # Mark as failed and refund
                    supabase.table('withdrawals').update({
                        'status': 'failed',
                        'admin_note': f'Transaction failed on {NETWORK_NAME} - tokens refunded'
                    }).eq('id', withdrawal_id).execute()
                    
                    # Refund user balance
                    supabase.rpc('add_balance', {
                        'user_id_param': user_id,
                        'amount_param': str(amount),
                        'type_param': 'refund',
                        'description_param': f'Refund for failed withdrawal #{withdrawal_id}'
                    }).execute()
                    
                    logger.error(f"❌ Failed to process withdrawal {withdrawal_id} - refunded")
                    failed += 1
                    
            except Exception as e:
                logger.error(f"Error processing withdrawal {withdrawal['id']}: {e}")
                failed += 1
                
                # Mark as failed and refund
                try:
                    supabase.table('withdrawals').update({
                        'status': 'failed',
                        'admin_note': f'Processing error: {str(e)[:200]}'
                    }).eq('id', withdrawal['id']).execute()
                    
                    # Refund user balance
                    supabase.rpc('add_balance', {
                        'user_id_param': withdrawal['user_id'],
                        'amount_param': str(withdrawal['amount']),
                        'type_param': 'refund',
                        'description_param': f'Refund for failed withdrawal #{withdrawal["id"]}'
                    }).execute()
                    
                except Exception as refund_error:
                    logger.error(f"Error refunding withdrawal {withdrawal['id']}: {refund_error}")
                
                # Continue with next withdrawal
                continue
        
        logger.info(f"📊 Batch complete: {successful} successful, {failed} failed")
                
    except Exception as e:
        logger.error(f"Error in process_approved_withdrawals: {e}")

def cleanup_old_processing():
    """Clean up old processing withdrawals (stuck transactions)"""
    try:
        # FIXED: Proper timestamp handling
        cutoff_time = datetime.now() - timedelta(minutes=30)
        
        # Get withdrawals stuck in processing
        result = supabase.table('withdrawals').select('*').eq('status', 'processing').execute()
        
        stuck_count = 0
        for withdrawal in result.data:
            try:
                # Parse the processed_at timestamp
                processed_at = withdrawal.get('processed_at')
                if processed_at:
                    # Convert ISO timestamp to datetime
                    processed_time = datetime.fromisoformat(processed_at.replace('Z', '+00:00'))
                    
                    if processed_time < cutoff_time:
                        # Mark as failed and refund
                        supabase.table('withdrawals').update({
                            'status': 'failed',
                            'admin_note': 'Stuck in processing - auto-failed and refunded'
                        }).eq('id', withdrawal['id']).execute()
                        
                        # Refund balance
                        supabase.rpc('add_balance', {
                            'user_id_param': withdrawal['user_id'],
                            'amount_param': str(withdrawal['amount']),
                            'type_param': 'refund',
                            'description_param': f'Auto-refund for stuck withdrawal #{withdrawal["id"]}'
                        }).execute()
                        
                        stuck_count += 1
                        logger.warning(f"Cleaned up stuck withdrawal {withdrawal['id']}")
                        
            except Exception as e:
                logger.error(f"Error cleaning withdrawal {withdrawal['id']}: {e}")
        
        if stuck_count > 0:
            logger.info(f"🧹 Cleaned up {stuck_count} stuck withdrawals")
        
    except Exception as e:
        logger.error(f"Error in cleanup: {e}")

def health_check():
    """Perform health checks"""
    try:
        # Check Web3 connection
        if not w3.is_connected():
            logger.error("❌ Web3 connection lost")
            return False
        
        # Check latest block (should be recent)
        latest_block = w3.eth.block_number
        logger.info(f"📊 Latest block: {latest_block}")
        
        # Check admin balance
        balance = check_contract_balance()
        if balance < 1000:  # Alert if less than 1000 tokens
            logger.warning(f"⚠️ Low admin balance: {balance:,.2f} tokens")
        
        # Check database connection
        supabase.table('settings').select('id').limit(1).execute()
        
        # FIXED: Check admin wallet BNB balance for gas
        admin_account = get_admin_account()
        if admin_account:
            bnb_balance = w3.eth.get_balance(admin_account.address)
            bnb_balance_ether = w3.from_wei(bnb_balance, 'ether')
            
            min_bnb = 0.01 if IS_TESTNET else 0.1
            if bnb_balance_ether < min_bnb:
                logger.warning(f"⚠️ Low BNB balance for gas: {bnb_balance_ether:.4f} BNB")
            else:
                logger.info(f"💰 Admin BNB balance: {bnb_balance_ether:.4f} BNB")
        
        logger.info("✅ Health check passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        return False

def main():
    """Main processing loop"""
    logger.info(f"🚀 Payment processor started on {NETWORK_NAME}")
    
    # Initial health check
    if not health_check():
        logger.error("Initial health check failed")
        exit(1)
    
    # Display contract info
    try:
        contract = get_contract()
        if contract:
            symbol = contract.functions.symbol().call()
            decimals = contract.functions.decimals().call()
            name = contract.functions.name().call()
            logger.info(f"🪙 Token: {name} ({symbol}), Decimals: {decimals}")
        else:
            logger.warning("⚠️ Contract not available - running in simulation mode")
    except Exception as e:
        logger.error(f"Error getting contract info: {e}")
    
    consecutive_errors = 0
    max_consecutive_errors = 5
    cycle_count = 0
    
    while True:
        try:
            cycle_count += 1
            logger.info(f"🔄 Cycle #{cycle_count}: Checking for approved withdrawals...")
            
            # Process withdrawals
            process_approved_withdrawals()
            
            # Cleanup old processing every 5 cycles
            if cycle_count % 5 == 0:
                cleanup_old_processing()
            
            # Health check every 10 cycles
            if cycle_count % 10 == 0:
                health_check()
            
            consecutive_errors = 0
            
            # FIXED: Different intervals for testnet vs mainnet
            wait_time = 30 if IS_TESTNET else 60
            logger.info(f"✅ Cycle completed. Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            
        except KeyboardInterrupt:
            logger.info("👋 Shutting down payment processor...")
            break
            
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"❌ Error in main loop (#{consecutive_errors}): {e}")
            
            if consecutive_errors >= max_consecutive_errors:
                logger.error(f"💥 Too many consecutive errors ({consecutive_errors}). Shutting down.")
                break
            
            # Wait longer on errors
            error_wait = 180 if IS_TESTNET else 120
            logger.info(f"⏳ Waiting {error_wait} seconds before retry...")
            time.sleep(error_wait)

if __name__ == "__main__":
    main()
