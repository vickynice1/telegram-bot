import os
from web3 import Web3
from supabase import create_client
import json
import time
import logging
from decimal import Decimal

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

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BSC_NODE_URL = os.getenv("BSC_NODE_URL", "https://bsc-dataseed.binance.org/")
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS")
ADMIN_PRIVATE_KEY = os.getenv("ADMIN_PRIVATE_KEY")
ADMIN_ADDRESS = os.getenv("ADMIN_ADDRESS")

# Validate required environment variables
required_vars = [SUPABASE_URL, SUPABASE_KEY, CONTRACT_ADDRESS, ADMIN_PRIVATE_KEY]
if not all(required_vars):
    logger.error("Missing required environment variables")
    exit(1)

# Initialize
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    w3 = Web3(Web3.HTTPProvider(BSC_NODE_URL))
    
    # Check Web3 connection
    if not w3.is_connected():
        logger.error("Failed to connect to BSC node")
        exit(1)
    
    logger.info(f"Connected to BSC. Latest block: {w3.eth.block_number}")
    
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
        # Add 20% margin for faster confirmation
        return int(gas_price * 1.2)
    except Exception as e:
        logger.error(f"Error getting gas price: {e}")
        return w3.to_wei('5', 'gwei')  # Fallback

def estimate_gas(contract, admin_address, to_address, amount):
    """Estimate gas for transaction"""
    try:
        gas_estimate = contract.functions.transfer(
            to_address, amount
        ).estimate_gas({'from': admin_address})
        
        # Add 20% margin
        return int(gas_estimate * 1.2)
    except Exception as e:
        logger.error(f"Error estimating gas: {e}")
        return 200000  # Fallback

def wait_for_transaction_receipt(tx_hash, timeout=300):
    """Wait for transaction confirmation"""
    try:
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
        return receipt
    except Exception as e:
        logger.error(f"Error waiting for receipt: {e}")
        return None

def send_tokens(to_address, amount_tokens):
    """Send tokens via Web3"""
    try:
        logger.info(f"Sending {amount_tokens} tokens to {to_address}")
        
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
        
        # Get current nonce
        nonce = w3.eth.get_transaction_count(admin_account.address, 'pending')
        
        # Get gas price and estimate gas
        gas_price = get_gas_price()
        gas_limit = estimate_gas(contract, admin_account.address, to_address, amount_wei)
        
        logger.info(f"Gas price: {gas_price}, Gas limit: {gas_limit}")
        
        # Build transaction
        transaction = contract.functions.transfer(
            to_address,
            amount_wei
        ).build_transaction({
            'chainId': 56,  # BSC Mainnet
            'gas': gas_limit,
            'gasPrice': gas_price,
            'nonce': nonce,
        })
        
        logger.info(f"Transaction built: {transaction}")
        
        # Sign transaction
        signed_txn = w3.eth.account.sign_transaction(transaction, ADMIN_PRIVATE_KEY)
        
        # Send transaction
        tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
        tx_hash_hex = tx_hash.hex()
        
        logger.info(f"Transaction sent: {tx_hash_hex}")
        
        # Wait for confirmation
        receipt = wait_for_transaction_receipt(tx_hash, timeout=300)
        
        if receipt and receipt.status == 1:
            logger.info(f"Transaction confirmed: {tx_hash_hex}")
            return tx_hash_hex
        else:
            logger.error(f"Transaction failed: {tx_hash_hex}")
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
            logger.warning(f"Insufficient admin balance. Need: {total_needed:,.2f}, Have: {admin_balance:,.2f}")
            # Continue processing what we can
        
        for withdrawal in withdrawals.data:
            try:
                withdrawal_id = withdrawal['id']
                to_address = withdrawal['to_address']
                amount = float(withdrawal['amount'])
                user_id = withdrawal['user_id']
                
                logger.info(f"Processing withdrawal {withdrawal_id}: {amount} tokens to {to_address}")
                
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
                        'processed_at': 'now()'
                    }).eq('id', withdrawal_id).execute()
                    
                    # Log successful transaction
                    supabase.table('transactions').insert({
                        'user_id': user_id,
                        'type': 'withdrawal_paid',
                        'amount': str(-amount),  # Negative for outgoing
                        'description': f'Withdrawal paid - TX: {tx_hash}',
                        'reference_id': withdrawal_id
                    }).execute()
                    
                    logger.info(f"✅ Withdrawal {withdrawal_id} processed successfully: {tx_hash}")
                    
                    # Small delay between transactions
                    time.sleep(5)
                    
                else:
                    # Mark as failed and refund
                    supabase.table('withdrawals').update({
                        'status': 'failed',
                        'admin_note': 'Transaction failed - tokens refunded'
                    }).eq('id', withdrawal_id).execute()
                    
                    # Refund user balance
                    supabase.rpc('add_balance', {
                        'user_id_param': user_id,
                        'amount_param': str(amount),
                        'type_param': 'refund',
                        'description_param': f'Refund for failed withdrawal #{withdrawal_id}'
                    }).execute()
                    
                    logger.error(f"❌ Failed to process withdrawal {withdrawal_id} - refunded")
                    
            except Exception as e:
                logger.error(f"Error processing withdrawal {withdrawal['id']}: {e}")
                
                # Mark as failed and refund
                try:
                    supabase.table('withdrawals').update({
                        'status': 'failed',
                        'admin_note': f'Processing error: {str(e)}'
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
                
    except Exception as e:
        logger.error(f"Error in process_approved_withdrawals: {e}")

def cleanup_old_processing():
    """Clean up old processing withdrawals (stuck transactions)"""
    try:
        # Find withdrawals stuck in processing for more than 30 minutes
        result = supabase.table('withdrawals').select('*').eq('status', 'processing').execute()
        
        current_time = time.time()
        
        for withdrawal in result.data:
            # Check if processing for more than 30 minutes
            processed_at = withdrawal.get('processed_at')
            if processed_at:
                # Convert to timestamp and check
                # This is a simplified check - you might need to parse the timestamp properly
                # For now, mark them as failed after some time
                pass
        
    except Exception as e:
        logger.error(f"Error in cleanup: {e}")

def health_check():
    """Perform health checks"""
    try:
        # Check Web3 connection
        if not w3.is_connected():
            logger.error("Web3 connection lost")
            return False
        
        # Check admin balance
        balance = check_contract_balance()
        if balance < 1000:  # Alert if less than 1000 tokens
            logger.warning(f"Low admin balance: {balance:,.2f} tokens")
        
        # Check database connection
        supabase.table('settings').select('id').limit(1).execute()
        
        return True
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False

def main():
    """Main processing loop"""
    logger.info("🚀 Payment processor started")
    
    # Initial health check
    if not health_check():
        logger.error("Initial health check failed")
        exit(1)
    
    # Display contract info
    try:
        contract = get_contract()
        symbol = contract.functions.symbol().call()
        decimals = contract.functions.decimals().call()
        logger.info(f"Token: {symbol}, Decimals: {decimals}")
    except Exception as e:
        logger.error(f"Error getting contract info: {e}")
    
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    while True:
        try:
            logger.info("🔄 Checking for approved withdrawals...")
            
            # Process withdrawals
            process_approved_withdrawals()
            
            # Cleanup old processing
            cleanup_old_processing()
            
            # Health check every 10 cycles
            if consecutive_errors == 0:
                health_check()
            
            consecutive_errors = 0
            
            logger.info("✅ Cycle completed. Waiting 60 seconds...")
            time.sleep(60)  # Check every minute
            
        except KeyboardInterrupt:
            logger.info("👋 Shutting down payment processor...")
            break
            
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"Error in main loop (#{consecutive_errors}): {e}")
            
            if consecutive_errors >= max_consecutive_errors:
                logger.error(f"Too many consecutive errors ({consecutive_errors}). Shutting down.")
                break
            
            # Wait longer on errors
            time.sleep(120)

if __name__ == "__main__":
    main()
