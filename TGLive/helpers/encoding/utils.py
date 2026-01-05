import os


def get_last_segment_number(hls_dir: str) -> int:
    if not os.path.exists(hls_dir):
        return 1

    ts_files = [
        f for f in os.listdir(hls_dir)
        if f.endswith(".ts") and f[:-3].isdigit()
    ]

    if not ts_files:
        return 1

    return max(int(f[:-3]) for f in ts_files) + 1
