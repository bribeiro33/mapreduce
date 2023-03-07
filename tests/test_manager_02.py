"""See unit test function docstring."""

import json
import tempfile
import threading
import mapreduce
import utils
from utils import TESTDATA_DIR


def worker_message_generator(mock_sendall, tmp_path):
    """Fake Worker messages."""
    # Worker register
    yield json.dumps({
        "message_type": "register",
        "worker_host": "localhost",
        "worker_port": 3001,
    }).encode("utf-8")
    yield None

    # User submits new job
    yield json.dumps({
        "message_type": "new_manager_job",
        "input_directory": TESTDATA_DIR/"input",
        "output_directory": tmp_path,
        "mapper_executable": TESTDATA_DIR/"exec/wc_map.sh",
        "reducer_executable": TESTDATA_DIR/"exec/wc_reduce.sh",
        "num_mappers": 2,
        "num_reducers": 1
    }, cls=utils.PathJSONEncoder).encode("utf-8")
    yield None

    # Wait for Manager to create temporary directory
    #
    # Transfer control back to solution under test in between each check for
    # tmpdir to simulate the Manager calling recv() when there's nothing
    # to receive.
    tmpdir_job0 = None
    for tmpdir_job0 in (
        utils.wait_for_exists_glob(f"{tmp_path}/mapreduce-shared-job00000-*")
    ):
        yield None

    # Simulate files created by Worker.  The files are empty because the
    # Manager does not read the contents, just the filenames.
    (tmpdir_job0/"maptask00000-part00000").touch()
    (tmpdir_job0/"maptask00001-part00000").touch()

    # Wait for Manager to send first map message
    #
    # Transfer control back to solution under test in between each check for
    # map messages to simulate the Manager calling recv() when there's nothing
    # to receive.
    for _ in utils.wait_for_map_messages(mock_sendall, num=1):
        yield None

    # Status finished message from first map task
    yield json.dumps({
        "message_type": "finished",
        "task_id": 0,
        "worker_host": "localhost",
        "worker_port": 3001,
    }).encode("utf-8")
    yield None

    # Wait for Manager to send second map message
    #
    # Transfer control back to solution under test in between each check for
    # map messages to simulate the Manager calling recv() when there's nothing
    # to receive.
    for _ in utils.wait_for_map_messages(mock_sendall, num=2):
        yield None

    # Status finished message from second map task
    yield json.dumps({
        "message_type": "finished",
        "task_id": 1,
        "worker_host": "localhost",
        "worker_port": 3001,
    }).encode("utf-8")
    yield None

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode("utf-8")
    yield None


def test_map(mocker, tmp_path):
    """Verify content of map messages sent by the Manager.

    Note: 'mocker' is a fixture function provided by the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.

    See https://github.com/pytest-dev/pytest-mock/ for more info.

    Note: 'tmp_path' is a fixture provided by the pytest-mock package.
    This fixture creates a temporary directory for use within this test.

    See https://docs.pytest.org/en/6.2.x/tmpdir.html for more info.
    """
    # Mock the socket library socket class
    mock_socket = mocker.patch("socket.socket")

    # sendall() records messages
    mock_sendall = mock_socket.return_value.__enter__.return_value.sendall

    # accept() returns a mock client socket
    mock_clientsocket = mocker.MagicMock()
    mock_accept = mock_socket.return_value.__enter__.return_value.accept
    mock_accept.return_value = (mock_clientsocket, ("127.0.0.1", 10000))

    # TCP recv() returns values generated by worker_message_generator()
    mock_recv = mock_clientsocket.recv
    mock_recv.side_effect = worker_message_generator(mock_sendall, tmp_path)

    # UDP recv() returns heartbeat messages
    mock_udp_recv = mock_socket.return_value.__enter__.return_value.recv
    mock_udp_recv.side_effect = utils.worker_heartbeat_generator(3001)

    # Set the location where the Manager's temporary directory
    # will be created.
    tempfile.tempdir = tmp_path

    # Spy on tempfile.TemporaryDirectory so that we can determine the name
    # of the directory that was created.
    mock_tmpdir = mocker.spy(tempfile.TemporaryDirectory, "__init__")

    # Run student Manager code.  When student Manager calls recv(), it will
    # return the faked responses configured above.
    try:
        mapreduce.manager.Manager("localhost", 6000)
        assert threading.active_count() == 1, "Failed to shutdown threads"
    except SystemExit as error:
        assert error.code == 0

    # Verify that the correct number of TemporaryDirectories was used.
    assert mock_tmpdir.call_count == 1, \
        "Expected to see call to `tempfile.TemporaryDirectory(...)`"

    # Find the name of the temporary directory.
    tmpdir_job0 = utils.get_tmpdir_name(mock_tmpdir)

    # Verify first 3 messages sent by the Manager
    #
    # Pro-tip: show log messages and detailed diffs with
    #   $ pytest -vvs --log-cli-level=info tests/test_manager_X.py
    messages = utils.get_messages(mock_sendall)
    assert messages[:3] == [
        {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": 3001,
        },
        {
            "message_type": "new_map_task",
            "task_id": 0,
            "executable": str(TESTDATA_DIR/"exec/wc_map.sh"),
            "input_paths": [
                str(TESTDATA_DIR/"input/file01"),
                str(TESTDATA_DIR/"input/file03"),
                str(TESTDATA_DIR/"input/file05"),
                str(TESTDATA_DIR/"input/file07"),
            ],
            "output_directory": tmpdir_job0,
            "num_partitions": 1,
            "worker_host": "localhost",
            "worker_port": 3001,
        },
        {
            "message_type": "new_map_task",
            "task_id": 1,
            "executable": str(TESTDATA_DIR/"exec/wc_map.sh"),
            "input_paths": [
                str(TESTDATA_DIR/"input/file02"),
                str(TESTDATA_DIR/"input/file04"),
                str(TESTDATA_DIR/"input/file06"),
                str(TESTDATA_DIR/"input/file08"),
            ],
            "output_directory": tmpdir_job0,
            "num_partitions": 1,
            "worker_host": "localhost",
            "worker_port": 3001,
        }
    ]
