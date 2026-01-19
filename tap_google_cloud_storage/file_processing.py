# tap_google_cloud_storage/file_processing.py
"""
Shared utilities for processing compressed files (ZIP, GZ) and detecting file formats.
Used by both discovery (sampling) and sync operations.
"""

import io
import gzip
import singer
from singer_encodings import compression

LOGGER = singer.get_logger()


def detect_gzip_magic_bytes(data_bytes):
    """
    Check if data starts with gzip magic bytes (1f 8b).

    Args:
        data_bytes: bytes to check

    Returns:
        bool: True if data is gzipped
    """
    return len(data_bytes) >= 2 and data_bytes[0] == 0x1f and data_bytes[1] == 0x8b


def get_delimiter_for_file(file_path, table_spec):
    """
    Get delimiter based on file extension and table spec configuration.

    Args:
        file_path: Path to the file
        table_spec: Table specification dict

    Returns:
        str: Delimiter character (, \t, |, or custom)
    """
    lower = file_path.lower()
    configured = table_spec.get('delimiter')

    if configured and configured not in (None, ''):
        return configured

    if lower.endswith('.tsv'):
        return '\t'
    elif lower.endswith('.psv'):
        return '|'
    else:
        return ','


def extract_files_from_zip(file_bytes, archive_path):
    """
    Extract files from a ZIP archive.

    Args:
        file_bytes: Raw bytes of the ZIP file
        archive_path: Path to the ZIP file (for logging)

    Yields:
        tuple: (filename, file_object, extension) for each file in archive.
               Returns (None, None, None) for skipped files (nested compression).
    """
    try:
        decompressed_files = compression.infer(io.BytesIO(file_bytes), archive_path)

        for decompressed_file in decompressed_files:
            filename = decompressed_file.name
            extension = filename.split(".")[-1].lower()

            # Skip nested compression
            if extension in ['zip', 'gz', 'tar']:
                LOGGER.warning('Skipping "%s/%s" - nested compression not supported', 
                             archive_path, filename)
                yield None, None, None
                continue

            yield filename, decompressed_file, extension

    except Exception as e:
        LOGGER.warning('Failed to process ZIP file %s: %s', archive_path, e)
        yield None, None, None


def extract_file_from_gzip(file_bytes, archive_path, get_filename_func):
    """
    Extract file from a GZIP archive.

    Args:
        file_bytes: Raw bytes of the GZ file
        archive_path: Path to the GZ file (for logging)
        get_filename_func: Function to extract filename from gzip header

    Returns:
        tuple: (filename, file_bytes, extension) or (None, None, None) if extraction fails
    """
    try:
        # Get the original filename from gzip header
        gz_file_name = get_filename_func(fileobj=io.BytesIO(file_bytes))
    except (AttributeError, OSError) as err:
        LOGGER.warning('Skipping "%s" - could not get original file name from gzip header', archive_path)
        return None, None, None

    if not gz_file_name:
        LOGGER.warning('Skipping "%s" - no filename found in gzip header', archive_path)
        return None, None, None

    # Check for nested compression
    if gz_file_name.lower().endswith('.gz'):
        LOGGER.warning('Skipping "%s" - nested compression not supported', archive_path)
        return None, None, None

    # Decompress
    try:
        gz_file_obj = gzip.GzipFile(fileobj=io.BytesIO(file_bytes))
        gz_data = gz_file_obj.read()
        extension = gz_file_name.split(".")[-1].lower()
        return gz_file_name, gz_data, extension
    except Exception as e:
        LOGGER.warning('Failed to process GZ file %s: %s', archive_path, e)
        return None, None, None


def decompress_if_gzipped(data_bytes, file_name, get_filename_func):
    """
    Decompress data if it's gzipped (based on magic bytes), even if file extension is wrong.
    This handles files like gz_stored_as_csv.csv which are gzipped but have .csv extension.

    Args:
        data_bytes: Raw file bytes
        file_name: Original filename
        get_filename_func: Function to extract filename from gzip header

    Returns:
        tuple: (decompressed_data, actual_filename) or original values if not gzipped
    """
    if detect_gzip_magic_bytes(data_bytes):
        LOGGER.info('Detected gzipped content in "%s" despite non-gz extension, decompressing', file_name)

        try:
            # Try to get original filename from gzip header
            original_name = get_filename_func(fileobj=io.BytesIO(data_bytes))
            if original_name:
                LOGGER.info('Gzip header indicates original filename: "%s"', original_name)
                file_name = original_name

            # Decompress
            gz_file_obj = gzip.GzipFile(fileobj=io.BytesIO(data_bytes))
            return gz_file_obj.read(), file_name

        except Exception as e:
            LOGGER.warning('Failed to decompress gzipped file %s: %s', file_name, e)
            return data_bytes, file_name

    return data_bytes, file_name
