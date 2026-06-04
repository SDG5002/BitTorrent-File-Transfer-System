import os
import json
import socket
import hashlib
import threading
import time
import sys
import random
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from reassembler import reassemble



CHUNK_SIZE         = 512 * 1024          # must match chunker
STORE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "p2p_store"
)

TRACKER_URL = "http://10.196.55.147:8000"

HEARTBEAT_INTERVAL = 30 #In Seconds            
MAX_RETRIES     = 3  # per chunk, try this many peers before giving up
PARALLEL_WORKERS   = 32 # Increased to hide TCP connection overhead
SERVER_HOST       = "0.0.0.0"           # bind address for the chunk server
SERVER_PORT    = 6881                # default TCP port for the chunk server
MAX_UPLOADS   = 100        # max simultaneous upload threads
MAX_CHUNK_FAILURES = 5   # give up on a chunk after this many download failures
NETWORK_DELAY_S    = 0   # simulated network latency per chunk

##### HELPER  FUNCTIONS

def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def chunk_path(info_hash: str, idx: int) -> str:
    return os.path.join(STORE_DIR, info_hash, f"chunk_{idx:04d}.bin")


def meta_path(info_hash: str) -> str:
    return os.path.join(STORE_DIR, info_hash, "meta.json")


def load_meta(info_hash: str) -> dict:
    with open(meta_path(info_hash)) as f:
        return json.load(f)


def validate_meta(meta: dict, expected_info_hash: str) -> bool:
    
    required = ["filename", "info_hash", "total_chunks", "chwunk_hashes"]

    for field in required:
        if field not in meta:
            return False

    if meta["info_hash"] != expected_info_hash:
        return False

    if not isinstance(meta["total_chunks"], int):
        return False

    if len(meta["chunk_hashes"]) != meta["total_chunks"]:
        return False

    return True

# We have to send the bitfield to the leacher who requested for the chunks as what we have they should know.

def build_bitfield(meta: dict) -> list[bool]:
   
    bf = []
    for i in range(meta["total_chunks"]):
        path = chunk_path(meta["info_hash"], i)
        if os.path.exists(path):#Leecher cann download random chunks so we have to check does thatt chunk exists and is not corrupt
            with open(path, "rb") as f:
                data = f.read()
            if sha256(data) == meta["chunk_hashes"][i]:
                bf.append(True)
            else:
                # Hash mismatch then delete the corrupt chunk so it doesn't
                # confuse the chunk server into serving bad data.
                os.remove(path)
                print(f"[bitfield] chunk {i} corrupt — will re-download")
                bf.append(False)
        else:
            bf.append(False)
    return bf


def bitfield_to_hex(bitfield: list) -> str:
     
    # For efficiency we pack boolean chunk states into bits.
    #
    # Example:
    # [True, True, False]
    #
    # JSON form:
    # "[true, true, false]"  -> many bytes(each char require 1 byte so ~21 bytes)over network
    #
    # Packed binary:
    # 11000000-> c0 in hex  -> 2 byte
    #
    # Hex form:
    # "c0" -> only 2 characters
    #
    # So packed bitfields are far smaller than JSON boolean arrays.
     
    remainder = len(bitfield) % 8

    if remainder == 0:
        padded = bitfield
    else:
        padding_needed = 8 - remainder
        padded = bitfield + [False] * padding_needed

    byte_list = []
    for i in range(0, len(padded), 8):
        byte_val = 0
        for j in range(8):
            if padded[i + j]:
                byte_val |= (1 << (7 - j))
        byte_list.append(byte_val)
    return bytes(byte_list).hex()


def hex_to_bitfield(hex_str: str, total_chunks: int) -> list:
    #Here revesre of the above process is done.
    #Example "c0"->11-> [True,True]
    data = bytes.fromhex(hex_str)
    bits = []
    for byte_val in data:
        for j in range(8):
            bits.append(bool(byte_val & (1 << (7 - j))))
    return bits[:total_chunks]



#  TCP chunk server
#  Listens for incoming chunk requests from other peers.
#
#  Protocol:
#    Request  (client → us): GET <bitfield> <info_hash>\n   #-------------->GET BITFIELD
#                            GET <info_hash> <chunk_idx>\n  #-------------->GEt CHUNK chunk_idx from peer 
#                            GET_META <info_hash>\n         #-------------->GET META info_hash from peer

#    Response (us → client):   OK <num_bytes>\n<raw bytes>\n SEE HERE first newline is used for making the data and header separate
#                              ERR <reason>\n



def handle_peer(conn: socket.socket, addr):
    try:
        buf = b""

        while True:
            chunk = conn.recv(1024)
            if not chunk:
                break

            buf += chunk

            #\n will be there at end of req
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)  # split at first real newline
                line = line.decode().strip()

                parts = line.split()

                if parts[0] == "GET_BITFIELD":
                    if len(parts) != 2:
                        conn.sendall(b"ERR bad request\n")
                        continue
                    _, info_hash = parts

                    info_hash = info_hash.strip()
                    try:
                        meta = load_meta(info_hash)
                        bitfield = build_bitfield(meta)
                        bitfield_hex = bitfield_to_hex(bitfield)
                        conn.sendall(f"BITFIELD {bitfield_hex}\n".encode())
                    except Exception:
                        conn.sendall(b"ERR not found\n")
                    continue

                if parts[0] == "GET_META":
                    if len(parts) != 2:
                        conn.sendall(b"ERR bad meta request\n")
                        continue

                    _, info_hash = parts

                    meta_file = meta_path(info_hash)
                    if not os.path.exists(meta_file):
                        conn.sendall(b"ERR meta not found\n")
                        continue

                    with open(meta_file, "rb") as f:
                        data = f.read()

                    header = f"OK {len(data)}\n".encode()
                    conn.sendall(header)
                    conn.sendall(data)
                    continue

                #parts = ["GET", "info_hash", "chunk_idx"]
                if len(parts) != 3 or parts[0] != "GET":
                    conn.sendall(b"ERR bad request\n")
                    continue

                _, info_hash, idx_str = parts
                idx = int(idx_str)

                path = chunk_path(info_hash, idx)
                if not os.path.exists(path):
                    conn.sendall(b"ERR chunk not found\n")
                    continue

                with open(path, "rb") as f:
                    data = f.read()

               
                header = f"OK {len(data)}\n".encode()
                conn.sendall(header)
                conn.sendall(data)

    except Exception as e:
        try:
            conn.sendall(f"ERR {e}\n".encode())
        except:
            pass
    finally:
        conn.close()


# Actual Connections for seeding the chunks happens here.After connecting handlePeer(A) is called to handle single peer. 

def start_chunk_server(host: str = SERVER_HOST, port: int = SERVER_PORT) -> int:

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   
    server.bind((host, port))
    server.listen(100)  # Up to 100 connections can queue while upload slots are full

    # Semaphore limits simultaneous active upload threads to MAX_UPLOADS.
    # Connections that arrive when all slots are taken are rejected immediately
    # with ERR instead of being queued indefinitely.
    upload_semaphore = threading.Semaphore(MAX_UPLOADS)

    def serve():
        print(f"[chunk-server] Listening on {host}:{port} (max {MAX_UPLOADS} concurrent uploads)")
        while True:
            conn, addr = server.accept()

            # Try to acquire an upload slot measn do count -=1
            if not upload_semaphore.acquire(blocking=False): #With blocking=False then dont wait in queue drop this req → return immediately
                conn.sendall(b"ERR server busy\n")
                conn.close()
                continue

            def worker(c=conn, a=addr):
                try:
                    handle_peer(c, a)
                finally:
                    upload_semaphore.release()

            threading.Thread(target=worker, daemon=True).start()

    threading.Thread(target=serve, daemon=True).start()
    return port



# TCP CLIENT (LEECHER)-----------------------------------------------------------------------------------------------------------------

# Get Chunk From the peer

def fetch_chunk_tcp(ip: str, port: int, info_hash: str, idx: int) -> bytes:

    with socket.create_connection((ip, port), timeout=10) as s:  #Try to connect this ip but wait only for 10 seconds to get conn.If no conn in this time exit
        # Send request

        s.sendall(f"GET {info_hash} {idx}\n".encode())

        # Read response header
        buf = b""

        while b"\n" not in buf:
            chunk = s.recv(1024)
            if not chunk:
                raise ConnectionError("connection closed before header")
            buf += chunk

        # Response from server will be like "OK <num_bytes>\n<raw bytes>"    
        # If error "ERR <reason>\n"

        header, rest = buf.split(b"\n", 1) # This tells split at first newline only(new line char is removed)
        header = header.decode().strip()

        if header.startswith("ERR"):
            raise ValueError(f"peer error: {header}")

        # Now header is like "OK <num_bytes>" 
        num_bytes = int(header.split(" ")[1])

        # Read remaining data
        chunks = [rest]
        bytes_received = len(rest)

        # read until enough bytes
        while bytes_received < num_bytes:
            chunk = s.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed mid-transfer")
            chunks.append(chunk)
            bytes_received += len(chunk)

        # extract exactly required data as u can get garbage data after the required data

        buf = b"".join(chunks)
        return buf[:num_bytes]


# Get the meta file from the peer

def fetch_meta(ip: str, port: int, info_hash: str) -> bytes:

    #With make sure to clean the socket while leaving 
    with socket.create_connection((ip, port), timeout=10) as s:
        s.sendall(f"GET_META {info_hash}\n".encode())

        #Res format will be OK <num_bytes>\n<meta.json>
        #If error "ERR <reason>\n"

        # Json looks like {
        #  "filename": "video.mp4",
        #  "info_hash": "abc123",
        #  "total_chunks": 3,
        #  "chunk_hashes": ["h1", "h2", "h3"]
        #  }

        buf = b""
        while b"\n" not in buf:
            buf += s.recv(1024)

        header, rest = buf.split(b"\n", 1)
        header = header.decode()

        if header.startswith("ERR"):
            raise Exception(header)

        size = int(header.split(" ")[1])

        buf = rest
        while len(buf) < size:
            buf += s.recv(65536)

        return buf[:size]

# Get the bitfield from the peer

def fetch_bitfield_tcp(ip: str, port: int, info_hash: str, total_chunks: int) -> list[bool]:
    
    #Response Format: BITFIELD <hex_string>\n
    
    with socket.create_connection((ip, port), timeout=10) as s:
        s.sendall(f"GET_BITFIELD {info_hash}\n".encode())
        buf = b""

        while b"\n" not in buf:
            buf += s.recv(1024)

        line = buf.split(b"\n", 1)[0].decode().strip() #Removing \n from line

        if not line.startswith("BITFIELD"):
            #It means response is ERR not found
            return [False] * total_chunks

        hex_str = line.split()[1]
        return hex_to_bitfield(hex_str, total_chunks)



#  Tracker API calls --------------------------------------------------------------------------------------------------------------

# Announce to tracker whenever you have atleast single chunk of a file
def announce(peer_id: str, ip: str, port: int, meta: dict):
    resp = requests.post(f"{TRACKER_URL}/announce", json={
        "peer_id":peer_id,
        "ip": ip,
        "port": port,
        "info_hash":meta["info_hash"],
        "filename":meta["filename"],
        "total_chunks": meta["total_chunks"],
    })
    resp.raise_for_status() #This will raise an exception if the request was unsuccessful.like 200,404,500 etc
    return resp.json()


#Depending on the infoHash bring all peers who have chunksin the file
def get_all_peers(info_hash: str) -> list[dict]:

    resp = requests.get(f"{TRACKER_URL}/peers",params={"info_hash": info_hash})
    resp.raise_for_status()


    #Response: {"peers": [...], "total_chunks": N}  

    data = resp.json()

    total_chunks = data["total_chunks"]
    peers = data["peers"]

    for p in peers:
        # Client asks peer directly for bitfield
        try:
            p["bitfield"] = fetch_bitfield_tcp(p["ip"], p["port"], info_hash, total_chunks)
            have = sum(p["bitfield"])
            print(f"[peers] {p['ip']}:{p['port']} — bitfield OK, has {have}/{total_chunks} chunks")
        except Exception as e:
            # If peer crashes or closes connection, treat as having no chunks
            p["bitfield"] = [False] * total_chunks
            print(f"[peers] {p['ip']}:{p['port']} — UNREACHABLE: {e}")
            
    return peers


def peers_for_particular_chunk(all_peers, idx, my_ip, my_port):

    result = []

    for p in all_peers:

        has_chunk = p["bitfield"][idx]

        is_me = (p["ip"] == my_ip and p["port"] == my_port)

        if has_chunk and not is_me:
            result.append(p)

    return result



def heartbeat(peer_id: str):
    try:
        resp = requests.post(f"{TRACKER_URL}/heartbeat", json={"peer_id": peer_id})
        if resp.json().get("status") == "reannounce":
            return "reannounce"
    except Exception as e:
        print(f"[heartbeat] failed: {e}")
    return "ok"



#Leaving from swarm
def leave(peer_id: str):
  
    try:
        requests.post(f"{TRACKER_URL}/leave", json={"peer_id": peer_id}, timeout=5)
        print("[leave] Deregistered from tracker")
    except Exception as e:
        print(f"[leave] Failed: {e}")



#  Heartbeat thread
#  Keeps our presence alive on the tracker.

def start_heartbeat(peer_id: str, ip: str, port: int, meta: dict):
    stop_event = threading.Event()

    def loop():
        while not stop_event.is_set():
            stop_event.wait(timeout=HEARTBEAT_INTERVAL)
            if stop_event.is_set():
                break
            status = heartbeat(peer_id)
            if status == "reannounce":
                print("[heartbeat] tracker lost us - re-announcing")
                announce(peer_id, ip, port, meta)

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()

    return stop_event, thread




#  Single chunk downloader
#  Tries up to MAX_RETRIES peers for one chunk.

# Lock: Only one thread can enter this section right now. Everyone else wait outside

def download_chunk(peer_id: str, ip: str, port: int,
                   meta: dict, bitfield: list[bool],#bitfield is my local bit field that i have
                   bitfield_lock: threading.Lock,
                   all_peers_ref: list,# These are all peers
                   all_peers_lock: threading.Lock,
                   idx: int,
                   progress) -> bool:

    info_hash = meta["info_hash"]

    for attempt in range(MAX_RETRIES):
        # Grab the latest peer list from the shared ref
        with all_peers_lock:
            #This we are doing in refresh lock (all_peers_ref[0] = new_peers) So to avoid both things to
            #happen together we are using this lock

            #As refresh thread can change peer list so at every retry we should get latest peer list 
            current_peers = all_peers_ref[0]

        # Filter locally — who has this chunk? (excludes ourselves)
        peers = peers_for_particular_chunk(current_peers, idx, ip, port)

        if not peers:
            print(f"[chunk {idx}] no peers available, retrying in 2s...")
            time.sleep(2)
            continue

        # Peer rotation: spread load across all peers
        # Use (idx + attempt) so different chunks go to different
        # peers on the first try. Shuffle so we
        # don't always hit the same peer for a given idx.

        ordered_peers = list(peers)
        random.shuffle(ordered_peers)
        peer = ordered_peers[(idx + attempt) % len(ordered_peers)]

        try:
            t0 = time.time()
            data = fetch_chunk_tcp(peer["ip"], peer["port"], info_hash, idx)

            elapsed_ms = (time.time() - t0) * 1000

            # Checking does chunk is currupted
            if sha256(data) != meta["chunk_hashes"][idx]:
                print(f"[chunk {idx}] hash mismatch from {peer['ip']} - skipping peer")
                continue

            # Write to disk
            with open(chunk_path(info_hash, idx), "wb") as f:
                f.write(data)


            # Log the transfer
            print(f"[transfer] chunk={idx} from={peer['ip']}:{peer['port']} "
                  f"size={len(data)} time={elapsed_ms:.0f}ms")

            # Update shared bitfield
            with bitfield_lock:
                bitfield[idx] = True

          
            return True

        except Exception as e:
            print(f"[chunk {idx}] attempt {attempt + 1} failed: {e}")

    return False





def ensure_meta(info_hash: str) -> dict:

    # Decide whether we need to fetch metadata. As new joiner won't have any metadata.Or it may be corrupted
    if os.path.exists(meta_path(info_hash)):
        try:
            meta = load_meta(info_hash)
            if validate_meta(meta, info_hash):
                print("[meta] valid local metadata found — reusing")
                return meta
            else:
                print("[meta] invalid local metadata")
        except Exception:
            print("[meta] failed to load local metadata")

    print("[meta] fetching metadata from peers...")

    all_peers = get_all_peers(info_hash)

    for p in all_peers:
        try:
            meta_bytes = fetch_meta(p["ip"], p["port"], info_hash)

            os.makedirs(os.path.join(STORE_DIR, info_hash), exist_ok=True)

            with open(meta_path(info_hash), "wb") as f:
                f.write(meta_bytes)

            print("[meta] received successfully")
            break

        except Exception as e:
            print(f"[meta] failed from {p['ip']}: {e}")
    else:
        print("[error] could not fetch metadata from any peer")
        sys.exit(1)

    # Load & validate the freshly fetched metadata
    meta = load_meta(info_hash)
    if not validate_meta(meta, info_hash):
        print("[error] fetched metadata is invalid")
        sys.exit(1)

    return meta



def download(peer_id: str, ip: str, port: int, meta: dict):
    
    start_time = time.time()

    info_hash     = meta["info_hash"]
    bitfield      = build_bitfield(meta)
    bitfield_lock = threading.Lock()
    #bitfield_lock is used wherever your code updates or reads shared local download state like bitfield 

    os.makedirs(os.path.join(STORE_DIR, info_hash), exist_ok=True)

    total   = meta["total_chunks"]

    missing = []

    for i in range(len(bitfield)):
            if bitfield[i] == False:
                missing.append(i)

    print(f"[download] {meta['filename']} - {len(missing)}/{total} chunks needed")


    
    all_peers_ref  = [get_all_peers(info_hash)]
    all_peers_lock = threading.Lock()
    stop_event     = threading.Event()
    reachable = [p for p in all_peers_ref[0] if any(p["bitfield"])]
    print(f"[download] Got {len(all_peers_ref[0])} peers from tracker, {len(reachable)} reachable with chunks")

    
    # Get new peer list from tracker as new downloads may had happened  at interval of 5sec

    def refresh_peers_loop():
        while not stop_event.is_set():
            stop_event.wait(timeout=5)
            if stop_event.is_set():
                break
            try:
                new_peers = get_all_peers(info_hash)
                with all_peers_lock:
                    all_peers_ref[0] = new_peers
                print(f"[peer-refresh] updated -{len(new_peers)} peers")
            except Exception as e:
                print(f"[peer-refresh] failed: {e}")

    refresh_thread = threading.Thread(target=refresh_peers_loop, daemon=True)
    refresh_thread.start()



    # Pick a random available chunk using latest peer list
    remaining_chunks = set(missing)
    remaining_lock   = threading.Lock()
    failure_counts   = {}                 # idx → number of failed attempts
    failed           = []

    # If a chunk fails, increment its failure count.
    # Retry it again later by putting it back into remaining_chunks.
    # Once failure count reaches MAX_CHUNK_FAILURES, Dont add that again to remaining( You will consider it cant be downloadable)
    # Complete other chunks and exit

    def worker():
        while True:
            with remaining_lock:
                if not remaining_chunks:
                    return

                
                with all_peers_lock:
                    current_peers = all_peers_ref[0]

                # Pick a random chunk that is available
                available_chunks = []

                for i in remaining_chunks:
                    for p in current_peers:
                        if p["bitfield"][i]:
                            available_chunks.append(i)
                            break
                
                if not available_chunks:
                    time.sleep(1)
                    continue
                
                idx = random.choice(available_chunks)
                remaining_chunks.remove(idx)

            ok = download_chunk(
                peer_id, ip, port, meta, bitfield, bitfield_lock,
                all_peers_ref, all_peers_lock, idx, None
            )

            if not ok:
                with remaining_lock:
                    if idx not in failure_counts:
                        failure_counts[idx] = 0
                        failure_counts[idx] += 1

                    if failure_counts[idx] < MAX_CHUNK_FAILURES:
                        remaining_chunks.add(idx)
                    else:
                        failed.append(idx)
                        print(f"[chunk {idx}] gave up after {MAX_CHUNK_FAILURES} failures")

    
    
    #Start with parallel chunk downloads .i.e. establish max PARALLEL_WORKERS parallel conn to the peers
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:

        futures = [executor.submit(worker) for i in range(PARALLEL_WORKERS)]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"[worker] unexpected error: {e}")

   

        
    stop_event.set()  #Stop refresh thread
    refresh_thread.join()    # Wait until the function running in refresh_thread returns before continuing.

    if failed:
        print(f"\n[download] FAILED chunks: {failed}")
        sys.exit(1)

    end_time = time.time()
    total_time = end_time - start_time

    file_size = total * CHUNK_SIZE
    speed_mb = (file_size / (1024 * 1024)) / total_time if total_time > 0 else 0

    print(f"\n[download] Complete !— {meta['filename']} saved to {STORE_DIR}/{info_hash}/")
    print(f"[download] Total time: {total_time:.2f} seconds")
    print(f"[download] Avg speed: {speed_mb:.2f} MB/s")

    reassemble(info_hash)



def status(info_hash: str):
    if not os.path.exists(meta_path(info_hash)):
        print(f"[error] No meta.json for {info_hash}")
        sys.exit(1)

    meta     = load_meta(info_hash)
    bitfield = build_bitfield(meta)
    have     = sum(bitfield)
    total    = meta["total_chunks"]
    pct      = have / total * 100

    print(f"File      : {meta['filename']}")
    print(f"info_hash : {info_hash}")
    print(f"Progress  : {have}/{total} chunks ({pct:.1f}%)")
    print(f"Role      : {'seeder' if have == total else 'leecher'}")

    # Visual bitfield — 1 char per chunk
    bar = "".join("█" if h else "░" for h in bitfield)
    print(f"Bitfield  : {bar}")



#  CLI
#  peer.py seed   <info_hash> <ip> <port>
#  peer.py get    <info_hash> <ip> <port>
#  peer.py status <info_hash>


if __name__ == "__main__":
    import uuid

    if len(sys.argv) < 3:
        print("Usage:")
        print("  peer.py seed   <info_hash> <ip> <port>")
        print("  peer.py get    <info_hash> <ip> <port>")
        print("  peer.py status <info_hash>")
        sys.exit(1)

    command  = sys.argv[1]
    peer_id  = str(uuid.uuid4())

    if command == "status":
        status(sys.argv[2])

    elif command in ("seed", "get"):
        if len(sys.argv) < 5:
            print(f"Usage: peer.py {command} <info_hash> <ip> <port>")
            sys.exit(1)

        info_hash = sys.argv[2]
        my_ip     = sys.argv[3]
        my_port   = int(sys.argv[4])

        #  Ensure metadata exists
        meta = ensure_meta(info_hash)

        #  Start long-running services once
        start_chunk_server(port=my_port)
        announce(peer_id, my_ip, my_port, meta)
        start_heartbeat(peer_id, my_ip, my_port, meta)

        #  Download only if chunks missing
        bitfield = build_bitfield(meta)
        if all(bitfield):
            print("Already full seeder")
        else:
            download(peer_id, my_ip, my_port, meta)

        #  Stay alive serving chunks forever
        print("Now acting as full seeder")


        #Now u became a full seeder but programme should run for ever hence looping with sleep .i.e. Dont die 
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")
            leave(peer_id)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
