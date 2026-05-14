import os
import json
import hashlib
import sys

CHUNK_SIZE = 512 * 1024  # 512 KB

STORE_DIR = os.getenv("P2P_STORE", os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "p2p_store"
))

def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def chunk_file(filepath: str):
    filename = os.path.basename(filepath)
    total_size = os.path.getsize(filepath)

    chunk_hashes = []

    with open(filepath, "rb") as f:
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            chunk_hashes.append(sha256(data))

    total_chunks = len(chunk_hashes)

    combined = "".join(chunk_hashes).encode()
    info_hash = sha256(combined)

    file_dir = os.path.join(STORE_DIR, info_hash)
    os.makedirs(file_dir, exist_ok=True)

   
    with open(filepath, "rb") as f:
        i = 0
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            chunk_path = os.path.join(file_dir, f"chunk_{i:04d}.bin")
            with open(chunk_path, "wb") as cf:
                cf.write(data)
            i += 1

    meta = {
        "filename":     filename,
        "total_size":   total_size,
        "chunk_size":   CHUNK_SIZE,
        "total_chunks": total_chunks,
        "info_hash":    info_hash,
        "chunk_hashes": chunk_hashes,
    }


    meta_path = os.path.join(file_dir, "meta.json")
    with open(meta_path, "w") as mf:
        json.dump(meta, mf)

    print(f"Done!")
    print(f"File   : {filename}")
    print(f"Size   : {total_size / (1024*1024):.2f} MB")
    print(f"Chunks  : {total_chunks}")
    print(f"info_hash: {info_hash}")
    print(f"Stored at: {file_dir}")

    return info_hash


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python chunker.py <filepath>")
        sys.exit(1)

    chunk_file(sys.argv[1])