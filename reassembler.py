import os
import json
import hashlib
import sys

CHUNK_SIZE = 512 * 1024  # 512 KB
STORE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "p2p_store"
)

def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def chunk_path(info_hash: str, idx: int) -> str:
    return os.path.join(STORE_DIR, info_hash, f"chunk_{idx:04d}.bin")


def meta_path(info_hash: str) -> str:
    return os.path.join(STORE_DIR, info_hash, "meta.json")


def load_meta(info_hash: str) -> dict:
   with open(meta_path(info_hash)) as f:
        return json.load(f)


def reassemble(info_hash: str):
    
    meta = load_meta(info_hash)
    out_path = os.path.join(STORE_DIR, info_hash, meta["filename"])

    with open(out_path, "wb") as out:
         for i in range(meta["total_chunks"]):
            with open(chunk_path(info_hash, i), "rb") as cf:
                out.write(cf.read())

    size_mb = os.path.getsize(out_path) / (1024 * 1024)
    print(f"Reassembled chunks  {meta['filename']} ({size_mb:.2f} MB)")


#  CLI
#  reassembler.py <info_hash>

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: reassembler.py <info_hash>")
        sys.exit(1)

    reassemble(sys.argv[1])
