import pymupdf.layout  # type: ignore[import-untyped] # noqa: F401
import pymupdf4llm  # type: ignore[import-untyped]
import os
from tqdm import tqdm

pdf_dir = "./data/pdf"
txt_dir = "./data/full-txt"

def convert_pdfs_to_text():
    for subdir in tqdm(os.listdir(pdf_dir), desc="Sessions", position=0):
        subdir_path = os.path.join(pdf_dir, subdir)
        if os.path.isdir(subdir_path):
            # Create corresponding output subdirectory
            output_subdir = os.path.join(txt_dir, subdir)
            os.makedirs(output_subdir, exist_ok=True)

            # Process all PDFs in the subdirectory
            for filename in tqdm(os.listdir(subdir_path), desc=f"PDFs in {subdir}", position=1, leave=False):
                if filename.lower().endswith(".pdf"):
                    pdf_path = os.path.join(subdir_path, filename)
                    txt_filename = os.path.splitext(filename)[0] + ".txt"
                    txt_path = os.path.join(output_subdir, txt_filename)

                    try:
                        text = pymupdf4llm.to_text(
                            pdf_path,
                            write_images=False,      # No image placeholders
                            ignore_images=True,      # Skip image processing entirely
                            header=False,
                            footer=False
                        )
                        if not isinstance(text, str):
                            print(f"Unexpected conversion return type for {pdf_path}")
                            continue
                        with open(txt_path, "w", encoding="utf-8") as f:
                            f.write(text)
                        # print(f"Converted: {pdf_path} -> {txt_path}")
                    except Exception as e:
                        print(f"Error converting {pdf_path}: {e}")

if __name__ == "__main__":
    convert_pdfs_to_text()
