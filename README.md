# BitTorrent File Transfer Implementation

A from-scratch, multithreaded Python implementation of the BitTorrent Peer-to-Peer (P2P) file-sharing protocol using raw TCP sockets.

---

## 📖 What is BitTorrent?

BitTorrent is a decentralized communication protocol for peer-to-peer file sharing. Instead of downloading a file from a single central server (which can cause a bottleneck if thousands of people try to download at once), BitTorrent distributes the load among the users themselves.

**Key Concepts:**
*   **Seeder:** A user who has 100% of the file and is uploading it to others.
*   **Leecher:** A user who is currently downloading the file. (As soon as a leecher downloads a tiny piece of the file, they immediately start acting as a seeder for that specific piece).

*   **Tracker:** A central server that doesn't host any files, but keeps track of who is in the "swarm" (who has what file, their IP address, and port).

*   **Swarm:** The collective group of all seeders and leechers sharing a specific file.

*   **Chunks / Pieces:** Files are broken down into small, equally sized pieces (e.g., 512 KB). This allows leechers to download different pieces from different seeders simultaneously in parallel.

**Why it's powerful:** 
In a traditional transfer, the server's upload bandwidth is the bottleneck. In BitTorrent, the more people who download the file, the faster the network becomes, because every downloader brings their own upload bandwidth to the swarm.

---

## 🛠️ What This Project Does

This project is a raw implementation of the BitTorrent architecture. It bypasses high-level libraries and implements the core mechanics from the ground up:

1.  **File Chunker (`chunker.py`):** Takes a large file, splits it into 512 KB chunks, and generates a SHA-256 hash for every single chunk to prevent data corruption.
2.  **Central Tracker (`tracker.py`):** A Flask-based API that maintains a registry of peers, handling announcements, heartbeats, and peer discovery.
3.  **P2P Node (`peers.py`):** A dual-purpose multithreaded script. 
    *   It runs a **Multithreaded TCP Server** to accept incoming connections and upload chunks to peers.
    *   It runs a **Multithreaded TCP Client** to connect to multiple seeders simultaneously, requesting missing chunks based on a custom bitfield protocol.
4.  **Reassembler (`reassembler.py`):** Verifies the hashes and stitches the binary chunks back into the original file.

---

## 🚀 How to Run

### Prerequisites
Make sure you have Python installed, along with the required libraries:
```bash
pip install flask requests
```

### 1. Start the Tracker (Central Server)
The tracker must be running first. It keeps track of all peers.
```bash
python tracker.py
```
*Note: The tracker runs on port `8000`. Keep this terminal window open.*

### 2. Prepare a File for Seeding
Take the file you want to share and break it into chunks.
```bash
python chunker.py /path/to/your/video.mp4
```
*This will output an **info_hash** (a long alphanumeric string). Copy this hash, you will need it.*

### 3. Start a Seeder
The seeder will announce itself to the tracker and start listening for leechers.
```bash
# Usage: python peers.py seed <info_hash> <your_ip> <port>
python peers.py seed 12345infohashhere 192.168.1.5 6881
```

### 4. Start a Leecher (On a different machine)
A leecher connects to the tracker, discovers the seeders, and starts downloading chunks in parallel.
```bash
# First, tell the leecher where the tracker is located
# On Windows: set TRACKER_URL=http://<tracker_ip>:8000
# On Linux/Mac: export TRACKER_URL=http://<tracker_ip>:8000

# Start downloading
# Usage: python peers.py get <info_hash> <your_ip> <port>
python peers.py get 12345infohashhere 192.168.1.10 6882
```

### 5. Check Transfer Status
You can check how many chunks a node has downloaded at any time:
```bash
python peers.py status 12345infohashhere
```
