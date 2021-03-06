import logging
import struct
import json
import subprocess
import uuid
from threading import Thread

import bluetooth


class EndpointExistsError(Exception):
    pass


def _send_client_response(client_socket: bluetooth.BluetoothSocket,
                          return_data: bytes):
    if return_data is None:
        return_data = b''
    parsed_return_data = struct.pack(f'I{len(return_data)}s', len(return_data),
                                     return_data)
    client_socket.send(parsed_return_data)


def _recv_client_data(client_socket: bluetooth.BluetoothSocket):
    data = b''
    tmp_char = client_socket.recv(1)
    while tmp_char != b'\x00':
        data += tmp_char
        tmp_char = client_socket.recv(1)
    return data


def _recv_client_endpoint(client_socket: bluetooth.BluetoothSocket):
    end_point_raw_length = client_socket.recv(4)
    end_point_length = struct.unpack('I', end_point_raw_length)[0]
    end_point = client_socket.recv(end_point_length)
    return end_point.decode()


def _get_current_bluetooth_address():
    return subprocess.check_output(
        """hciconfig | grep "BD Address" | awk '{ print $3}'""",
        shell=True).decode().strip()


class BluetoothApp(Thread):
    _SERVICE_UUID = str(uuid.uuid1())

    def __init__(self,
                 service_name,
                 service_uuid=None,
                 port=bluetooth.PORT_ANY,
                 backlog=1,
                 logger=None):
        Thread.__init__(self)
        self._name = service_name
        self._service_uuid = service_uuid or self._SERVICE_UUID
        self._port = port
        self._backlog = backlog

        self._logger = logger or logging.getLogger(__name__)

        self._mac_address = _get_current_bluetooth_address()
        self._end_points = dict()

        self._server_socket = None
        self._running = True
        self.daemon = True

    @property
    def server_socket(self):
        if self._server_socket:
            return self._server_socket
        server_socket = bluetooth.BluetoothSocket(bluetooth.RFCOMM)
        server_socket.bind(("", self._port))
        self._server_socket = server_socket
        return self._server_socket

    def register(self, end_point):
        def _register_wrapper(func):
            def _inner_wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            self.register_endpoint(end_point, func)
            return _inner_wrapper

        return _register_wrapper

    def register_endpoint(self, end_point, callback):
        if end_point in self._end_points:
            raise EndpointExistsError("The given endpoint already exists")

        self._end_points[end_point] = callback

    def run(self):
        self._start_listening()
        self._advertise_service()
        while self._running:
            try:
                self._handle_clients()
            except Exception:
                self._logger.exception("Unhandled exception")

    def stop(self):
        self._running = False

    def _handle_clients(self):
        client_socket = self._wait_for_client()
        self._logger.info("Handling client")
        while client_socket and self._running:
            try:
                self._handle_client(client_socket)
            except bluetooth.btcommon.BluetoothError as e:
                if e.errno == 104:
                    self._logger.info("Client disconnected")
                else:
                    self._logger.exception("Unexpected client error")
                client_socket = None

    def _start_listening(self):
        self.server_socket.listen(self._backlog)
        self._logger.info(
            f"Listening on ({self._mac_address}, {self.server_socket.getsockname()[1]})"
            f" with [{self._service_uuid}]")

    def _advertise_service(self):
        bluetooth.advertise_service(
            self.server_socket,
            self._name,
            service_id=self._service_uuid,
            service_classes=[self._service_uuid, bluetooth.SERIAL_PORT_CLASS],
            profiles=[bluetooth.SERIAL_PORT_PROFILE])

    def _wait_for_client(self):
        self._logger.debug("Waiting for client")
        client_socket, client_info = self.server_socket.accept()
        self._logger.info(f"Received new client on [{client_info}]")
        return client_socket

    def _handle_client(self, client_socket):
        end_point = _recv_client_endpoint(client_socket)
        self._logger.debug(f"Running endpoint {end_point}")
        data = _recv_client_data(client_socket)
        if end_point in self._end_points:
            self._run_endpoint(client_socket, end_point, data)

    def _run_endpoint(self, client_socket, end_point, data):
        try:
            return_data = self._end_points[end_point](data=data)
        except Exception as e:
            self._logger.exception("Unhandled end-point exception")
            return_data = {"error": str(e)}
        if return_data is None:
            return_data = b''
        if type(return_data) is str:
            return_data = return_data.encode()
        if type(return_data) is dict:
            return_data = json.dumps(return_data).encode()
        _send_client_response(client_socket, return_data)
