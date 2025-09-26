import os
import magic
import chardet
from typing import Dict, Any


class FileAnalyzer:
    """Utility class for basic file analysis"""

    @staticmethod
    def get_file_info(file_path: str) -> Dict[str, Any]:
        """Get basic file information"""

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)
        file_ext = os.path.splitext(file_name)[1].lower()

        # Get MIME type
        try:
            mime_type = magic.from_file(file_path, mime=True)
        except:
            mime_type = "application/octet-stream"

        # Detect encoding for text files
        encoding = "utf-8"
        if mime_type.startswith("text/") or file_ext in ['.csv', '.json', '.xml']:
            try:
                with open(file_path, 'rb') as f:
                    raw_data = f.read(10000)
                    encoding_info = chardet.detect(raw_data)
                    encoding = encoding_info.get('encoding', 'utf-8') or 'utf-8'
            except:
                pass

        return {
            "file_name": file_name,
            "file_size": file_size,
            "file_extension": file_ext,
            "mime_type": mime_type,
            "encoding": encoding,
            "size_mb": round(file_size / (1024 * 1024), 2)
        }