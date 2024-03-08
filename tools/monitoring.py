    
        
import requests, time, subprocess
  
# Function to run the health check script and get its output
def run_health_check():
    try:
        result = subprocess.run(['python3', 'tools/HEALTHCHECK_CORE.py'], capture_output=True)
        return result.returncode  # Capture the exit code of the subprocess
    except Exception as e:
        print(f"An error occurred: {e}")
        return -1

def send_discord_webhook(webhook_url, message):
    payload = {
        "content": message
    }
    response = requests.post(webhook_url, json=payload)
    if response.status_code in [200, 204]:  # Check if status code is 200 or 204
        print("Webhook sent successfully!")
    else:
        print(f"Failed to send webhook. Status code: {response.status_code}")

if __name__ == '__main__':
    discord_webhook_url = "https://discord.com/api/webhooks/1211485695160483840/GP01m08pNtDzMCKhyWE5SfW1R1kbYE-Jhk6GUy6VzD2tUuBPY3kG8_DndzSFCHvnooge"
    
    while True:
        health_check_result = run_health_check()
        
        if health_check_result == 1: 
            message = "Health check failed. An error occurred."
            send_discord_webhook(discord_webhook_url, message)
        elif health_check_result == 0:
            print("Health check passed. No action needed.")

        # Wait for 5 minutes before running the next check
        time.sleep(300) 