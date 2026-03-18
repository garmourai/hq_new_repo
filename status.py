import requests
import sys
import socket

def get_local_ip():
    """
    Attempts to get the local machine's IP address.
    This method creates a dummy socket connection to an external address
    (which doesn't actually send data) to determine the local IP used for
    outbound connections.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception as e:
        print(f"Could not determine local IP, defaulting to 127.0.0.1. Error: {e}", file=sys.stderr)
        return "127.0.0.1"

def check_device_status(ip_address, port=8080):
    """
    Fetches the current status of a device.
    It expects a JSON response with a 'state' key from the /status endpoint.

    Args:
        ip_address (str): The IP address of the device to check.
        port (int): The port number where the status service is running.

    Returns:
        str: The state of the device (e.g., "IDLE", "RUNNING", "UNREACHABLE_...").
    """
    base_url = f"http://{ip_address}:{port}"
    try:
        response = requests.get(f"{base_url}/status", timeout=5)
        
        if response.ok:
            state = response.json().get("state", "UNKNOWN")
            return state if state else "UNKNOWN"
        else:
            return "UNREACHABLE_SERVICE_ERROR"
    except requests.exceptions.ConnectionError:
        return "UNREACHABLE_CONNECTION_ERROR"
    except requests.exceptions.Timeout:
        return "UNREACHABLE_TIMEOUT"
    except requests.RequestException as e:
        print(f"An unexpected request error occurred: {e}", file=sys.stderr)
        return "UNREACHABLE_GENERIC_ERROR"
    except ValueError:
        print(f"Received non-JSON response from {base_url}/status", file=sys.stderr)
        return "UNREACHABLE_INVALID_RESPONSE"

def main():
    """
    Main function to get the local IP and check its status.
    """
    ip_to_check = get_local_ip()

    # print(f"Checking status of device at local IP: {ip_to_check}...")

    status = check_device_status(ip_to_check)
    
    print(f"{status}")

if __name__ == "__main__":
    main()