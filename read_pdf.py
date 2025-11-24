
import pypdf
import sys

def read_pdf(path):
    try:
        reader = pypdf.PdfReader(path)
        text = ""
        for i, page in enumerate(reader.pages):
            if i >= 3:
                break
            text += page.extract_text() + "\n"
        print(text)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    read_pdf("AMQI2025_Project.pdf")
