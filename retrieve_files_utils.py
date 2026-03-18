import subprocess
import os
import json

def read_remote_file(hostname, username, password, port, remote_file_path):
    """
    Read the contents of a remote file via SSH
    
    Args:
    - hostname: IP address or hostname of the remote server
    - username: SSH username
    - password: SSH password
    - port: SSH port number
    - remote_file_path: Full path to the file on the remote server
    
    Returns:
    - File content as string or None if failed
    """
    try:
        # Construct SSH command to read file
        read_command = [
            "sshpass", "-p", password, 
            "ssh", 
            f"{username}@{hostname}", 
            f"cat {remote_file_path}"
        ]
        # Execute read command
        result = subprocess.run(
            read_command, 
            capture_output=True, 
            text=True, 
            check=True
        )
        
        
        return result.stdout
    
    except subprocess.CalledProcessError as e:
        print(f"SSH Read Error: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error reading file: {e}")
        return None

def write_remote_file(hostname, username, password, port, remote_file_path, new_content):
    """
    Overwrite a remote file via SSH
    
    Args:
    - hostname: IP address or hostname of the remote server
    - username: SSH username
    - password: SSH password
    - port: SSH port number
    - remote_file_path: Full path to the file on the remote server
    - new_content: Content to write to the file
    
    Returns:
    - Boolean indicating successful operation
    """
    try:
        # Create a temporary local file for scp transfer
        local_file = f"/home/pi/source_code/track_video_index.txt"
        
       
        
        # Copy new file to remote server using SCP
        scp_command = [
            "sshpass", "-p", password, 
            "scp", 
            local_file, 
            f"{username}@{hostname}:{remote_file_path}"
        ]
        
        # Execute SCP command
        subprocess.run(
            scp_command, 
            check=True
        )
        
       
        return True
    
    except subprocess.CalledProcessError as e:
        print(f"SSH/SCP Write Error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error writing file: {e}")
        return False

def write_remote_json(hostname, username, password, remote_file_path, new_content):
    """
    Overwrite a remote file via SSH
    
    Args:
    - hostname: IP address or hostname of the remote server
    - username: SSH username
    - password: SSH password
    - port: SSH port number
    - remote_file_path: Full path to the file on the remote server
    - new_content: Content to write to the file
    
    Returns:
    - Boolean indicating successful operation
    """
    try:
        # Create a temporary local file for scp transfer
        temp_dir = "/home/pi/source_code/temp"
        local_file = f"/home/pi/source_code/temp/track_video_index.json"
        
        os.makedirs(temp_dir, exist_ok=True)
        # Write new content to local file
        with open(local_file, "w") as file:
            json.dump(new_content, file, indent=4)
        
        # Copy new file to remote server using SCP
        scp_command = [
            "sshpass", "-p", password, 
            "scp", 
            local_file, 
            f"{username}@{hostname}:{remote_file_path}"
        ]
        
        # Execute SCP command
        subprocess.run(
            scp_command, 
            check=True
        )
        
        # Remove temporary local file
        os.remove(local_file)
        os.removedirs(temp_dir)
        
        return True
    
    except subprocess.CalledProcessError as e:
        print(f"SSH/SCP Write Error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error writing file: {e}")
        return False

def execute_remote_command(hostname, username, password, port, command):
    """
    Execute a command on a remote server via SSH
    
    Args:
    - hostname: IP address or hostname of the remote server
    - username: SSH username
    - password: SSH password
    - port: SSH port number
    - command: Command to execute on the remote server
    
    Returns:
    - Command output as string or None if failed
    """
    try:
        # Construct SSH command to execute remote command
        ssh_command = [
            "sshpass", "-p", password, 
            "ssh", 
            f"{username}@{hostname}", 
            command
        ]
        
        # Execute SSH command
        result = subprocess.run(
            ssh_command, 
            capture_output=True, 
            text=True, 
            check=True
        )
        
        return result.stdout
    
    except subprocess.CalledProcessError as e:
        print(f"SSH Command Execution Error: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error executing command: {e}")
        return None