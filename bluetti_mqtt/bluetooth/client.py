import asyncio
from enum import Enum, auto, unique
import logging
from typing import Union
from bleak import BleakClient, BleakScanner, BleakError
from bluetti_mqtt.core import DeviceCommand
from .exc import BadConnectionError, ModbusError, ParseError


@unique
class ClientState(Enum):
    NOT_CONNECTED = auto()
    CONNECTED = auto()
    READY = auto()
    PERFORMING_COMMAND = auto()
    COMMAND_ERROR_WAIT = auto()
    DISCONNECTING = auto()


class BluetoothClient:
    RESPONSE_TIMEOUT = 5
    WRITE_UUID = '0000ff02-0000-1000-8000-00805f9b34fb'
    NOTIFY_UUID = '0000ff01-0000-1000-8000-00805f9b34fb'
    DEVICE_NAME_UUID = '00002a00-0000-1000-8000-00805f9b34fb'

    name: Union[str, None]
    current_command: DeviceCommand
    notify_future: asyncio.Future
    notify_response: bytearray

    def __init__(self, address: str):
        self.address = address
        self.state = ClientState.NOT_CONNECTED
        self.name = None
        self.client = None
        self.command_queue = asyncio.Queue()
        self.notify_future = None
        self.loop = asyncio.get_running_loop()
        self._notify_started = False

    @property
    def is_ready(self):
        return self.state == ClientState.READY or self.state == ClientState.PERFORMING_COMMAND

    async def perform(self, cmd: DeviceCommand):
        future = self.loop.create_future()
        await self.command_queue.put((cmd, future))
        return future

    async def perform_nowait(self, cmd: DeviceCommand):
        await self.command_queue.put((cmd, None))

    async def run(self):
        try:
            while True:
                if self.state == ClientState.NOT_CONNECTED:
                    await self._connect()
                elif self.state == ClientState.CONNECTED:
                    if not self.name:
                        await self._get_name()
                    else:
                        await self._start_listening()
                elif self.state == ClientState.READY:
                    await self._perform_command()
                elif self.state == ClientState.DISCONNECTING:
                    await self._disconnect()
                else:
                    logging.warning(f'Unexpected current state {self.state}')
                    self.state = ClientState.NOT_CONNECTED
        finally:
            # Ensure that we disconnect
            if self.client:
                try:
                    if self._notify_started:
                        await self.client.stop_notify(self.NOTIFY_UUID)
                except Exception:
                    pass
                try:
                    await self.client.disconnect()
                except Exception:
                    pass

    async def _connect(self):
        """Establish connection to the bluetooth device"""
        while True:
            try:
                if self.client and self.client.is_connected:
                    self.state = ClientState.CONNECTED
                    return
                # cleanup old connection
                if self.client:
                    try:
                        if self._notify_started:
                            await self.client.stop_notify(self.NOTIFY_UUID)
                            self._notify_started = False
                    except Exception:
                        pass

                    try:
                        await self.client.disconnect()
                    except Exception:
                        pass
                # scan for device (forces BlueZ to recreate device object)
                device = await BleakScanner.find_device_by_address(self.address, timeout=20)

                if device is None:
                    logging.warning(f"Device {self.address} not found during scan")
                    await asyncio.sleep(5)
                    continue

                self.client = BleakClient(device, timeout=20)

                await self.client.connect()

                self._notify_started = False
                self.state = ClientState.CONNECTED

                logging.info(f"Connected to device: {self.address}")
                return

            except Exception as e:
                logging.warning(f"BLE connect failed ({self.address}): {type(e).__name__} {e}")
                await asyncio.sleep(3)

    async def _get_name(self):
        """Get device name, which can be parsed for type"""
        try:
            name = await self.client.read_gatt_char(self.DEVICE_NAME_UUID)
            self.name = name.decode('ascii')
            logging.info(f'Device {self.address} has name: {self.name}')
        except BleakError:
            logging.exception(f'Error retrieving device name {self.address}:')
            self.state = ClientState.DISCONNECTING

    async def _start_listening(self):
        """Register for command response notifications"""
        if self._notify_started:
            self.state = ClientState.READY
            return
        try:
            await self.client.start_notify(
                self.NOTIFY_UUID,
                self._notification_handler)
            self._notify_started = True
            self.state = ClientState.READY
        except BleakError:
            self.state = ClientState.DISCONNECTING

    async def _perform_command(self):
        cmd, cmd_future = await self.command_queue.get()
        retries = 0
        while retries < 5:
            try:
                # Prepare to make request
                self.state = ClientState.PERFORMING_COMMAND
                self.current_command = cmd
                self.notify_future = self.loop.create_future()
                self.notify_response = bytearray()

                # Make request
                await self.client.write_gatt_char(
                    self.WRITE_UUID,
                    bytes(self.current_command))

                # Wait for response
                res = await asyncio.wait_for(
                    self.notify_future,
                    timeout=self.RESPONSE_TIMEOUT)
                if cmd_future:
                    cmd_future.set_result(res)

                # Success!
                self.state = ClientState.READY
                break
            except ParseError:
                # For safety, wait the full timeout before retrying again
                self.state = ClientState.COMMAND_ERROR_WAIT
                retries += 1
                await asyncio.sleep(self.RESPONSE_TIMEOUT)
            except asyncio.TimeoutError:
                self.state = ClientState.COMMAND_ERROR_WAIT
                retries += 1
            except ModbusError as err:
                if cmd_future:
                    cmd_future.set_exception(err)

                # Don't retry
                self.state = ClientState.READY
                break
            except (BleakError, EOFError, BadConnectionError) as err:
                if cmd_future:
                    cmd_future.set_exception(err)

                self.state = ClientState.DISCONNECTING
                break

        if retries == 5:
            err = BadConnectionError('too many retries')
            if cmd_future:
                cmd_future.set_exception(err)
            self.state = ClientState.DISCONNECTING

        self.command_queue.task_done()

    async def _disconnect(self):
        try:
            if self._notify_started:
                await self.client.stop_notify(self.NOTIFY_UUID)
                self._notify_started = False
        except Exception:
            pass

        try:
            await self.client.disconnect()
        except Exception:
            pass

        logging.warning(f'Delayed reconnect to {self.address} after error')
        await asyncio.sleep(5)
        self.state = ClientState.NOT_CONNECTED

    def _notification_handler(self, _sender: int, data: bytearray):
        # Ignore notifications we don't expect
        if not self.notify_future or self.notify_future.done():
            return

        # If something went wrong, we might get weird data.
        if data == b'AT+NAME?\r' or data == b'AT+ADV?\r':
            err = BadConnectionError('Got AT+ notification')
            self.notify_future.set_exception(err)
            return

        # Save data
        self.notify_response.extend(data)

        if len(self.notify_response) == self.current_command.response_size():
            if self.current_command.is_valid_response(self.notify_response):
                self.notify_future.set_result(self.notify_response)
            else:
                self.notify_future.set_exception(ParseError('Failed checksum'))
        elif self.current_command.is_exception_response(self.notify_response):
            # We got a MODBUS command exception
            msg = f'MODBUS Exception {self.current_command}: {self.notify_response[2]}'
            self.notify_future.set_exception(ModbusError(msg))
