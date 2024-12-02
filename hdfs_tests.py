import pytest
from repository import hdfs_repository 

# Define some test paths and content
TEST_DIR = '/data'
TEST_FILE_HDFS = '/data/sample.txt'
TEST_FILE_LOCAL = './tests/local_sample.txt'
TEST_CONTENT = 'Hello, HDFS!'
DOWNLOAD_PATH = 'tests/'

@pytest.fixture(scope="module", autouse=True)
def setup_and_cleanup():
    hdfs_repository.delete(TEST_FILE_HDFS)
    hdfs_repository.delete(TEST_DIR)

    yield


def test_create_directory():
    hdfs_repository.create_directory(TEST_DIR)
    # Verify the directory was created by listing its contents
    contents = hdfs_repository.list_directory(TEST_DIR)
    assert contents == []
    print(f"Directory {TEST_DIR} created successfully.")


def test_upload_file():
    # Create a local file to upload
    with open(TEST_FILE_LOCAL, 'w') as f:
        f.write(TEST_CONTENT)
    
    hdfs_repository.upload_file(TEST_FILE_HDFS, TEST_FILE_LOCAL)
    # Verify the file was uploaded by listing the directory
    contents = hdfs_repository.list_directory(TEST_DIR)
    assert 'sample.txt' in contents
    print(f"File {TEST_FILE_LOCAL} uploaded to HDFS as {TEST_FILE_HDFS}.")


def test_download_file():
    # Download the file back to a different local path
    download_path = f'./{DOWNLOAD_PATH}downloaded_sample.txt'
    hdfs_repository.download_file(TEST_FILE_HDFS, download_path)
    
    # Verify the file was downloaded correctly
    with open(download_path, 'r') as f:
        content = f.read()
    assert content == TEST_CONTENT
    print(f"File downloaded successfully to {download_path}.")


def test_delete():
    hdfs_repository.delete(TEST_FILE_HDFS)
    # Verify the file was deleted by listing the directory
    contents = hdfs_repository.list_directory(TEST_DIR)
    assert 'sample.txt' not in contents
    print(f"File {TEST_FILE_HDFS} deleted from HDFS.")


def test_read_path():
    # Re-upload the file for reading
    hdfs_repository.upload_file(TEST_FILE_HDFS, TEST_FILE_LOCAL)
    lines = hdfs_repository.read_path(TEST_FILE_HDFS)
    assert lines == [TEST_CONTENT]
    print(f"Content of {TEST_FILE_HDFS} read successfully: {lines}")


def test_write_path():
    # Write content to a new file in HDFS
    new_hdfs_file = '/data/new_test_file.txt'
    hdfs_repository.write_path(new_hdfs_file, TEST_CONTENT)
    
    # Read back the content to verify
    lines = hdfs_repository.read_path(new_hdfs_file)
    assert lines == [TEST_CONTENT]
    print(f"Content written to {new_hdfs_file} and verified successfully.")

    # Clean up
    hdfs_repository.delete(new_hdfs_file)
    print(f"File {new_hdfs_file} deleted successfully.")


# Add a main block to execute test functions
if __name__ == "__main__":
    # Run each test manually
    test_create_directory()
    test_upload_file()
    test_download_file()
    test_delete()
    test_read_path()
    test_write_path()
