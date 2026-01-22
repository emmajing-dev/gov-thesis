import os
from collections import defaultdict

if __name__ == "__main__":
    # Count files in each session subdirectory
    session_counts = defaultdict(int)
    data_dir = "./data/pdf"

    for session_dir in os.listdir(data_dir):
        session_path = os.path.join(data_dir, session_dir)
        if os.path.isdir(session_path):
            file_count = len([f for f in os.listdir(session_path) if os.path.isfile(os.path.join(session_path, f))])
            session_counts[session_dir] = file_count

    # Write to CSV (truncate if exists)
    with open("./session_counts.csv", "w", newline="") as f:
        f.truncate(0)
        f.write("session,count\n")
        for session, count in sorted(session_counts.items()):
            f.write(f"{session},{count}\n")

    print(dict(session_counts))

