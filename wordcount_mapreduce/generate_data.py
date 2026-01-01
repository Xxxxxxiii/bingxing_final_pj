
import os
import urllib.request


# URL for the source text (Moby Dick from Project Gutenberg)
SOURCE_URL = "https://www.gutenberg.org/files/2701/2701-0.txt"
SOURCE_FILENAME = "corpus.txt"

OUTPUT_DIR = "input"

TARGET_SIZES = {
    "1K": 1 * 1024,          # 1 Kilobyte
    "1M": 1 * 1024 * 1024,     # 1 Megabyte
    "10M": 10 * 1024 * 1024,    # 10 Megabytes
    "100M": 100 * 1024 * 1024,  # 100 Megabytes
}


def download_corpus():
    if not os.path.exists(SOURCE_FILENAME):
        print(f"Downloading source text from {SOURCE_URL}...")
        try:
            urllib.request.urlretrieve(SOURCE_URL, SOURCE_FILENAME)
            print(f"Successfully downloaded and saved as {SOURCE_FILENAME}")
        except Exception as e:
            print(f"Error downloading file: {e}")
            exit(1)
    else:
        print(f"Source text '{SOURCE_FILENAME}' already exists.")

def generate_files():
    """从语料库产生指定大小的数据集"""
    print("\nReading source corpus into memory...")
    try:
        with open(SOURCE_FILENAME, "r", encoding="utf-8") as f:
            source_content = f.read()
    except Exception as e:
        print(f"Error reading source file: {e}")
        exit(1)

    if not os.path.exists(OUTPUT_DIR):
        print(f"Creating output directory: {OUTPUT_DIR}")
        os.makedirs(OUTPUT_DIR)

    print("\nGenerating test files...")
    for name, target_size_bytes in TARGET_SIZES.items():
        output_filename = os.path.join(OUTPUT_DIR, f"input_{name}.txt")
        print(f"  - Creating {output_filename} ({name})...")

        source_content_bytes = source_content.encode('utf-8')
        source_size_bytes = len(source_content_bytes)

        with open(output_filename, "wb") as f:
            num_repeats = target_size_bytes // source_size_bytes
            for _ in range(num_repeats):
                f.write(source_content_bytes)

            remaining_bytes = target_size_bytes % source_size_bytes
            if remaining_bytes > 0:
                f.write(source_content_bytes[:remaining_bytes])

    print("\nSuccessfully generated all test files.")

if __name__ == "__main__":
    download_corpus()
    generate_files()

