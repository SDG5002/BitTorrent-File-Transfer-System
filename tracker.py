import time
import random
import threading
from flask import Flask, request, jsonify

MAX_PEERS_RETURNED=100

app = Flask(__name__)

# I am Using in memory tracker for simplicity
#
#  Structure:
#
#  registry = {
#    "<info_hash>": {
#      "filename":     "movie.mp4",
#      "total_chunks": 297,
#      "peer_ids":     {"peer1", "peer2"}
#    }
#  }
#
#  peers = {
#    "<peer_id>": {
#        "ip": "x",
#        "port": 6881,
#        "last_seen": 1234
#    }
#  }


registry = {}   
peers    = {} 
lock     = threading.Lock()

PEER_TTL = 60           # remove peer if no heartbeat for this many seconds

MAX_PEERS_RETURNED = 10    # max peers returned per peers request





#  POST /announce
#  Called by a peer when it joins or finishes a download means it becomes a seeder from this instant.

#  It looks like :
#  {
#    "peer_id":      "uuid-string",
#    "ip":           "192.168.1.5",
#    "port":         6881,
#    "info_hash":    "a3f9c2...",
#    "filename":     "movie.mp4",
#    "total_chunks": 297,
# 
# }


# ANNOUNCEMENT when peer will have chunks: POST  /announce

@app.route("/announce", methods=["POST"])
def announce():
    body = request.get_json()

    required = ["peer_id", "ip", "port", "info_hash",
                "filename", "total_chunks"]
    for field in required:
        if field not in body:
            return jsonify({"error": f"missing field: {field}"}), 400

    peer_id  = body["peer_id"]
    ip   = body["ip"]
    port  = body["port"]
    info_hash = body["info_hash"]
    filename   = body["filename"]
    total_chunks = body["total_chunks"]
    now  = time.time()

    if not (1 <= port <= 65535):
        return jsonify({"error": "invalid port"}), 400

    if not ip:
        return jsonify({"error": "invalid ip"}), 400

    with lock:
        peers[peer_id] = {
            "ip": ip,
            "port": port,
            "last_seen": now,
        }

       
        if info_hash not in registry:
            registry[info_hash] = {
                "filename":    filename,
                "total_chunks": total_chunks,
                "peer_ids":   set(),
            }
        elif registry[info_hash]["total_chunks"] != total_chunks:
            return jsonify({"error": "Something Broken as chunks are not matching"}), 409

        registry[info_hash]["peer_ids"].add(peer_id)

    return jsonify({
        "status": "ok",
        "info_hash": info_hash
    })



#  POST /heartbeat

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    body    = request.get_json()
    peer_id = body.get("peer_id")
    if not peer_id:
        return jsonify({"error": "missing peer_id"}), 400

    with lock:
        if peer_id in peers:
            peers[peer_id]["last_seen"] = time.time()
            return jsonify({
                "status": "ok",
            })
        else:
            return jsonify({"status": "reannounce"}), 410


# When Peer leaves unexpectedly then we should remove it from tracker. 
# POST /leave

@app.route("/leave", methods=["POST"])
def leave():
    body    = request.get_json()
    peer_id = body.get("peer_id")
    if not peer_id:
        return jsonify({"error": "missing peer_id"}), 400

    with lock:
        peers.pop(peer_id, None)

        for meta in registry.values():
            meta["peer_ids"].discard(peer_id)

    print(f"[tracker] Peer {peer_id} left")
    return jsonify({"status": "ok"})



# GET /peers?info_hash=<hash>

# Response:
# {
#   "info_hash": "...",
#   "total_chunks": 297,
#   "peers": [
#      {
#         "peer_id": "...",
#         "ip": "...",
#         "port": 6881
#      }
#   ]
# }

@app.route("/peers", methods=["GET"])
def get_peers():
    info_hash = request.args.get("info_hash")

    if not info_hash:
        return jsonify({"error": "info_hash is required"}), 400

    with lock:
        if info_hash not in registry:
            return jsonify({"error": "file not found"}), 404

        meta = registry[info_hash]
        total = meta["total_chunks"]

        result = []

        for pid in meta["peer_ids"]:
            if pid not in peers:
                continue

            info = peers[pid]

            result.append({
                "peer_id":  pid,
                "ip":       info["ip"],
                "port":     info["port"]
            })

        if len(result) > MAX_PEERS_RETURNED:
            selected = random.sample(result, MAX_PEERS_RETURNED)
        else:
            selected = result

    return jsonify({
        "info_hash":    info_hash,
        "total_chunks": total,
        "peers":        selected,
    })




def cleanup_stale_peers():
    while True:
        time.sleep(30)
        now = time.time()

        with lock:
            dead = []

            # Find dead peers
            for pid, info in peers.items():
                if now - info["last_seen"] > PEER_TTL:
                    dead.append(pid)

            # Remove dead peers
            for pid in dead:
                peers.pop(pid, None) #if pid doesnt exist return None instead of error so program will not crash
                for meta in registry.values():
                    meta["peer_ids"].discard(pid)

            if len(dead) > 0:
                print(f"[tracker] Removed {len(dead)} stale peer(s): {dead}")


#START

if __name__ == "__main__":
    t = threading.Thread(target=cleanup_stale_peers, daemon=True)
    t.start()
    print("[tracker] Starting on http://0.0.0.0:8000")
    app.run(host="0.0.0.0", port=8000, debug=False)