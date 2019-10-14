# To run this program, the file ``ssh_host_key`` must exist with an SSH
# private key in it to use as a server host key. An SSH host certificate
# can optionally be provided in the file ``ssh_host_key-cert.pub``.

import asyncio
import crypt
import csv
import logging
import sys
import os
import io
import asyncssh
from datetime import datetime as dt


LOG_FORMAT = '%(name)s::%(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

# defaults
MAX_ACTIVE_CONN = 20
TARPIT_DELAY_SEC = 10

RAISE_WITH = asyncssh.DISC_TOO_MANY_CONNECTIONS
RAISE_MSG = ''

OUTPUT_CSV_FILE = "ssh_credentials.csv"
OUTPUT_WRITE_AFTER_LINES = 5
CUSTOM_FILE_BUFF_SIZE = 512

# globals
ACTIVE_CONS = 0
OUTPUT_CSV_LINE_BUFF = []


def bail(count=True, custom_raise_f=None):
    if count:
        global ACTIVE_CONS
        ACTIVE_CONS -= 1
        logging.info(f'CONN CLOSED, TOTAL {ACTIVE_CONS}')

    if not custom_raise_f:
        raise asyncssh.DisconnectError(RAISE_WITH, RAISE_MSG)

    custom_raise_f()


class MySSHServer(asyncssh.SSHServer):
    peer_ip = None
    conn_on = False

    def connection_made(self, conn):
        conn.set_keepalive(TARPIT_DELAY_SEC+5)
        global ACTIVE_CONS
        self.peer_ip = conn.get_extra_info('peername')[0]

        if ACTIVE_CONS >= MAX_ACTIVE_CONN:
            logging.info(
                f"MAX CONN REACHED {ACTIVE_CONS}/{MAX_ACTIVE_CONN} "
                f"DROP: {self.peer_ip}")

            bail(False, conn.close)
            return False

        self.conn_on = True
        ACTIVE_CONS += 1
        logging.info(f'NEW CONN {self.peer_ip} TOTAL {ACTIVE_CONS}')

    def connection_lost(self, exc):
        '''Remove connections that were accepted'''
        if self.conn_on:
            global ACTIVE_CONS
            ACTIVE_CONS -= 1

    def password_auth_supported(self):
        return True

    def public_key_auth_supported(self):
        return False

    def kbdint_auth_supported(self):
        return False

    async def change_password(self, username, old_password, new_password):
        self.append_output_buffer(username, new_password)
        await asyncio.sleep(TARPIT_DELAY_SEC)

    async def validate_password(self, username, password):
        self.append_output_buffer(username, password)
        await asyncio.sleep(TARPIT_DELAY_SEC)

    def append_output_buffer(self, user, passw):
        if user and passw:
            OUTPUT_CSV_LINE_BUFF.append(
                [int(dt.now().timestamp()), self.peer_ip, user, passw]
            )


class OutputBuffer(object):
    def __init__(self, fname, *args, **kwargs):


def setup_csv_file():
    """Setup headers if the file is new, returns (CSV Writer, file object)"""
    existed = os.path.exists(OUTPUT_CSV_FILE)

    BUFF_S = CUSTOM_FILE_BUFF_SIZE if CUSTOM_FILE_BUFF_SIZE else io.DEFAULT_BUFFER_SIZE

    logging.debug("IO buffer set {}.".format(BUFF_S))

    fd = open(OUTPUT_CSV_FILE, 'a', buffering=BUFF_S)
    writer = csv.writer(fd, delimiter=',', quotechar='"', escapechar='\\')

    if not existed:
        logging.info("Output CSV doesn't exist, writing header.")
        writer.writerow(['timestamp', 'ip', 'user', 'password'])

    return writer, fd


async def writer_task(writer, write_buff):
    writer.writerows(write_buff)
    logging.info("CSWriter: written {}".format(len(write_buff)))


async def writer_loop(writer):
    global OUTPUT_CSV_LINE_BUFF
    global OUTPUT_WRITE_AFTER_LINES

    loop = asyncio.get_event_loop()
    while loop.is_running():
        # if len(OUTPUT_CSV_LINE_BUFF) > OUTPUT_WRITE_AFTER_LINES:
        # logging.debug("CSVWriter buff above limit, scheduling write")
        if OUTPUT_CSV_LINE_BUFF:
            buff_copy = OUTPUT_CSV_LINE_BUFF.copy()
            OUTPUT_CSV_LINE_BUFF = []
            asyncio.ensure_future(writer_task(writer, buff_copy))
            # loop.create_task(writer_task(writer, buff_copy))
        await asyncio.sleep(1)

    logging.debug("CSVWriter stopped.")


async def start_server():
    await asyncssh.create_server(MySSHServer, '', 8022,
                                 server_host_keys=['ssh_host_key'])


if __name__ == "__main__":

    loop = asyncio.get_event_loop()

    csv_writer, fd = setup_csv_file()

    try:
        try:
            loop.run_until_complete(start_server())
        except (OSError, asyncssh.Error) as exc:
            sys.exit('Error starting server: ' + str(exc))

        try:
            logging.info("Running loop.")
            asyncio.ensure_future(writer_loop(csv_writer))
            loop.run_forever()
        except KeyboardInterrupt:
            loop.stop()

    finally:
        fd.close()
