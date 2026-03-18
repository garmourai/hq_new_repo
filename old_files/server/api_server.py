from flask import Flask, jsonify
import os
import signal

app = Flask(__name__)

def get_pid():
    try:
        with open('/tmp/capture_pid', 'r') as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return None

def send_signal(sig):
    pid = get_pid()
    if pid is None:
        return False
    try:
        os.kill(pid, sig)
        return True
    except ProcessLookupError:
        return False

@app.route('/start', methods=['POST'])
def start_capture():
    if send_signal(signal.SIGUSR1):
        return jsonify({"status": "start signal sent"}), 200
    return jsonify({"error": "process not found"}), 404

@app.route('/stop', methods=['POST'])
def stop_capture():
    if send_signal(signal.SIGUSR2):
        return jsonify({"status": "stop signal sent"}), 200
    return jsonify({"error": "process not found"}), 404

@app.route('/status', methods=['GET'])
def status():
    return jsonify({"running": bool(get_pid())}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=False, processes=1)