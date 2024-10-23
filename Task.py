import os
from pymongo import MongoClient
from PyPDF2 import PdfReader
from concurrent.futures import ThreadPoolExecutor

# Step 1: Connect to MongoDB Atlas
def connect_to_mongodb():
    """Connect to MongoDB Atlas."""
    # Replace <username>, <password>, and <cluster-url> with your own details
    client = MongoClient("mongodb+srv://<username>:<password>@<cluster-url>/test?retryWrites=true&w=majority")
    db = client['pdf_database']
    collection = db['pdf_documents']
    return collection

# Step 2: Store PDF metadata into MongoDB
def store_pdf_metadata(collection, file_path):
    """Store the initial metadata of the PDF in MongoDB."""
    file_stats = os.stat(file_path)
    document = {
        'file_name': os.path.basename(file_path),
        'file_path': file_path,
        'size': file_stats.st_size,
        'summary': None,
        'keywords': None
    }
    return collection.insert_one(document).inserted_id

# Step 3: Parse PDF to extract text
def parse_pdf(file_path):
    """Extract text content from the PDF."""
    try:
        reader = PdfReader(file_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
        return text
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

# Step 4: Process PDFs in the folder concurrently
def process_pdfs_in_folder(folder_path, collection):
    """Process all PDFs in the folder concurrently."""
    pdf_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.pdf')]
    with ThreadPoolExecutor() as executor:
        for file_path in pdf_files:
            # Store metadata in MongoDB
            store_pdf_metadata(collection, file_path)
            # Extract and process PDF text
            executor.submit(parse_pdf, file_path)

# Usage example:
if __name__ == "__main__":
    folder_path = r"path_to_your_folder"  # Replace with the actual folder path
    collection = connect_to_mongodb()     # Connect to MongoDB

    # Process PDFs in the specified folder
    process_pdfs_in_folder(folder_path, collection)
