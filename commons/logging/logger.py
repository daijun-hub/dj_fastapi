# !/usr/bin/python

import os
import random
import re
import sys
import time
import traceback
from logging import Handler, LogRecord
from logging.handlers import BaseRotatingHandler
from stat import ST_MTIME

try:
    import codecs
except ImportError:
    codecs = None

try:
    import secrets
except ImportError:
    secrets = None

try:
    import gzip
except ImportError:
    gzip = None

__all__ = ["lock", "unlock", "LOCK_EX", "LOCK_SH", "LOCK_NB", "LockException", "ConcurrentTimedRotatingFileHandler"]


class LockException(RuntimeError):
    LOCK_FAILED = 1


class LockTimeoutException(RuntimeError):
    """
    readLockedFile will raise this when a lock acquisition attempt times out.
    """
    pass


if os.name == 'nt':
    import win32con
    import win32file
    import pywintypes
    import struct

    LOCK_EX = win32con.LOCKFILE_EXCLUSIVE_LOCK
    LOCK_SH = 0
    LOCK_NB = win32con.LOCKFILE_FAIL_IMMEDIATELY
    __overlapped = pywintypes.OVERLAPPED()


    def UnpackSigned32bitInt(hexnum):
        """
        Given a bytestring such as b'\xff\xff\x00\x00', interpret it as a SIGNED 32
        bit integer and return the result, -65536 in this case.

        This function was needed because somewhere along the line Python
        started interpreting a literal string like this (in source code)::

         >>> print(0xFFFF0000)
         -65536

        But then in Python 2.6 (or somewhere between 2.1 and 2.6) it started
        interpreting that as an unsigned int, which converted into a much
        larger longint.::

         >>> print(0xFFF0000)
         4294901760

        This allows the original behavior.  Currently needed to specify
        flags for Win32API locking calls.

        @TODO: candidate for docstring test cases!!
        """
        return struct.unpack('!i', hexnum)[0]


    nNumberOfBytesToLockHigh = UnpackSigned32bitInt(b'\xff\xff\x00\x00')
elif os.name == 'posix':
    import fcntl

    LOCK_EX = fcntl.LOCK_EX
    LOCK_SH = fcntl.LOCK_SH
    LOCK_NB = fcntl.LOCK_NB
else:
    raise RuntimeError("portalocker only defined for nt and posix platforms")

if os.name == 'nt':
    def lock(file, flags):
        hfile = win32file._get_osfhandle(file.fileno())
        try:
            win32file.LockFileEx(hfile, flags, 0, nNumberOfBytesToLockHigh, __overlapped)
        except pywintypes.error as exc_value:
            # error: (33, 'LockFileEx', 'The process cannot access the file because another process has locked a
            # portion of the file.')
            if exc_value.winerror == 33:
                raise LockException(str(exc_value))
            else:
                raise  # Q:  Are there exceptions/codes we should be dealing with here?


    def unlock(file):
        hfile = win32file._get_osfhandle(file.fileno())
        try:
            win32file.UnlockFileEx(hfile, 0, nNumberOfBytesToLockHigh, __overlapped)
        except pywintypes.error as exc_value:
            if exc_value.winerror == 158:
                # error: (158, 'UnlockFileEx', 'The segment is already unlocked.') To match the 'posix' implementation,
                # silently ignore this error
                pass
            else:
                raise  # Q:  Are there exceptions/codes we should be dealing with here?

elif os.name == 'posix':
    def lock(file, flags):
        try:
            fcntl.flock(file, flags)
        except IOError as exc_value:
            # The exception code varies on different systems so we'll catch every IO error
            raise LockException(str(exc_value))


    def unlock(file):
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)


def get_random_bits(num_bits=64):
    """Get a random number, using secrets module if available (Python 3.6), otherwise use random.SystemRandom()."""
    if secrets is not None and hasattr(secrets, "randbits"):
        rand_bits = secrets.randbits(num_bits)
    else:
        rand_bits = random.SystemRandom().getrandbits(num_bits)
    return rand_bits


class NullLogRecord(LogRecord):
    def __init__(self, *args, **kw):
        super(NullLogRecord, self).__init__(*args, **kw)

    def __getattr__(self, attr):
        return None


class ConcurrentRotatingFileHandler(BaseRotatingHandler):
    """
    Handler for logging to a set of files, which switches from one file to the
    next when the current file reaches a certain size. Multiple processes can
    write to the log file concurrently, but this may mean that the file will
    exceed the given size.
    """

    def __init__(
            self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, debug=False, delay=0, use_gzip=False):
        """
        Open the specified file and use it as the stream for logging.

        :param filename: name of the log file to output to.
        :param mode: write mode: defaults to 'a' for text append
        :param maxBytes: rotate the file at this size in bytes
        :param backupCount: number of rotated files to keep before deleting.
        :param encoding: text encoding for logfile
        :param debug: add extra debug statements to this class (for development)
        :param delay: see note below
        :param use_gzip: automatically gzip rotated logs if available.

        By default, the file grows indefinitely. You can specify particular
        values of maxBytes and backupCount to allow the file to rollover at
        a predetermined size.

        Rollover occurs whenever the current log file is nearly maxBytes in
        length. If backupCount is >= 1, the system will successively create
        new files with the same pathname as the base file, but with extensions
        ".1", ".2" etc. appended to it. For example, with a backupCount of 5
        and a base file name of "app.log", you would get "app.log",
        "app.log.1", "app.log.2", ... through to "app.log.5". The file being
        written to is always "app.log" - when it gets filled up, it is closed
        and renamed to "app.log.1", and if files "app.log.1", "app.log.2" etc.
        exist, then they are renamed to "app.log.2", "app.log.3" etc.
        respectively.

        If maxBytes is zero, rollover never occurs.

        On Windows, it is not possible to rename a file that is currently opened
        by another process.  This means that it is not possible to rotate the
        log files if multiple processes is using the same log file.  In this
        case, the current log file will continue to grow until the rotation can
        be completed successfully.  In order for rotation to be possible, all of
        the other processes need to close the file first.  A mechanism, called
        "degraded" mode, has been created for this scenario.  In degraded mode,
        the log file is closed after each log message is written.  So once all
        processes have entered degraded mode, the next rotation attempt should
        be successful and then normal logging can be resumed.  Using the 'delay'
        parameter may help reduce contention in some usage patterns.

        This log handler assumes that all concurrent processes logging to a
        single file will are using only this class, and that the exact same
        parameters are provided to each instance of this class.  If, for
        example, two different processes are using this class, but with
        different values for 'maxBytes' or 'backupCount', then odd behavior is
        expected. The same is true if this class is used by one application, but
        the RotatingFileHandler is used by another.
        """
        self.stream = None
        # Absolute file name handling done by FileHandler since Python 2.5
        BaseRotatingHandler.__init__(self, filename, mode, encoding, delay)
        self.delay = delay
        self._rotateFailed = False
        self.maxBytes = maxBytes
        self.backupCount = backupCount

        self.stream_lock = None
        self._debug = debug
        self.use_gzip = True if gzip and use_gzip else False

        self._stream_lock_count = 0

        if debug:
            self._degrade = self._degrade_debug

    def _open_lockfile(self):
        if self.stream_lock and not self.stream_lock.closed:
            self._console_log("Lockfile already open in this process")
            return
        # Use 'file.lock' and not 'file.log.lock' (Only handles the normal "*.log" case.)
        if self.baseFilename.endswith(".log"):
            lock_file = self.baseFilename[:-4]
        else:
            lock_file = self.baseFilename
        lock_file += ".lock"
        lock_path, lock_name = os.path.split(lock_file)
        # hide the file on Unix and generally from file completion
        lock_name = "." + lock_name
        lock_file = os.path.join(lock_path, lock_name)
        self._console_log(
            "concurrent-log-handler %s opening %s" % (hash(self), lock_file), stack=False)
        self.stream_lock = open(lock_file, "wb", buffering=0)

    def _do_file_unlock(self):
        self._console_log("in _do_file_unlock for %s" % (self.stream_lock,), stack=False)
        self._stream_lock_count = 0
        try:
            if self.stream_lock:
                unlock(self.stream_lock)
                self.stream_lock.close()
                self.stream_lock = None
            self._close()
            self._console_log(
                ">release complete lock for %s" % (self.stream_lock,), stack=False)
        except (OSError, ValueError, LockException) as e:
            # May be closed already but that's ok
            self._console_log(e, stack=True)
            # pass

    def _open(self, mode=None):
        """
        Open the current base file with the (original) mode and encoding. Return the resulting stream.

        Note:  Copied from stdlib.  Added option to override 'mode'
        """
        if mode is None:
            mode = self.mode
        if self.encoding is None:
            stream = open(self.baseFilename, mode)
        else:
            stream = codecs.open(self.baseFilename, mode, self.encoding)
        return stream

    def _close(self):
        """
        Close file stream.  Unlike close(), we don't tear anything down, we expect the log to be re-opened after
        rotation.
        """
        if self.stream:
            try:
                if not self.stream.closed:
                    self.stream.flush()
                    self.stream.close()
            finally:
                self.stream = None

    def _console_log(self, msg, stack=False):
        if not self._debug:
            return
        import threading
        tid = threading.current_thread().name
        pid = os.getpid()
        stack_str = ''
        if stack:
            stack_str = ":\n" + "".join(traceback.format_stack())
        print("[%s %s %s] %s%s" % (tid, pid, hash(self), msg, stack_str,))

    def acquire(self):
        """
        Acquire thread and file locks.  Re-opening log for 'degraded' mode.
        """
        self._console_log("In acquire", stack=True)

        # Handle thread lock
        Handler.acquire(self)

        # Noinspection PyBroadException
        try:
            self._open_lockfile()
        except Exception:
            self.handleError(NullLogRecord())

        self._stream_lock_count += 1
        self._console_log(">> stream_lock_count = %s" % (self._stream_lock_count,))
        if self._stream_lock_count == 1:
            self._console_log(">Getting lock for %s" % (self.stream_lock,), stack=True)
            lock(self.stream_lock, LOCK_EX)
            self.stream = self._open()

    def release(self):
        """
        Release file and thread locks. If in 'degraded' mode, close the stream to reduce contention until the log files
        can be rotated.
        """
        self._console_log("In release", stack=True)
        try:
            if self._rotateFailed:
                self._close()
        except Exception:
            self.handleError(NullLogRecord())
        finally:
            try:
                self._stream_lock_count -= 1
                if self._stream_lock_count < 0:
                    self._stream_lock_count = 0
                if self._stream_lock_count == 0:
                    self._do_file_unlock()
                    self._console_log("#completed release", stack=False)
                elif self._stream_lock_count:
                    self._console_log("#inner release (%s)" % (self._stream_lock_count,), stack=True)
            except Exception:
                self.handleError(NullLogRecord())
            finally:
                Handler.release(self)

    def close(self):
        """
        Close log stream and stream_lock. """
        self._console_log("In close()", stack=True)
        try:
            self._close()
        finally:
            self._do_file_unlock()
            self.stream_lock = None
            Handler.close(self)

    def _degrade(self, degrade, msg, *args):
        """ Set degrade mode or not.  Ignore msg. """
        self._rotateFailed = degrade
        del msg, args  # avoid pychecker warnings

    def _degrade_debug(self, degrade, msg, *args):
        """ A more colorful version of _degade(). (This is enabled by passing
        "debug=True" at initialization).
        """
        if degrade:
            if not self._rotateFailed:
                sys.stderr.write("Degrade mode - ENTERING - (pid=%d)  %s\n" %
                                 (os.getpid(), msg % args))
                self._rotateFailed = True
        else:
            if self._rotateFailed:
                # self._console_log("Exiting degrade")
                sys.stderr.write("Degrade mode - EXITING  - (pid=%d)   %s\n" %
                                 (os.getpid(), msg % args))
                self._rotateFailed = False

    def doRollover(self):
        """
        Do a rollover, as described in __init__().
        """
        self._close()
        if self.backupCount <= 0:
            self.stream = self._open("w")
            return
        try:
            tmpname = None
            while not tmpname or os.path.exists(tmpname):
                tmpname = "%s.rotate.%08d" % (self.baseFilename, get_random_bits(64))
            try:
                # Do a rename test to determine if we can successfully rename the log file
                os.rename(self.baseFilename, tmpname)
                if self.use_gzip:
                    self.do_gzip(tmpname)
            except (IOError, OSError):
                exc_value = sys.exc_info()[1]
                self._console_log("rename failed.  File in use? exception=%s" % (exc_value,))
                self._degrade(True, "rename failed.  File in use? exception=%s", exc_value)
                return

            gzip_ext = ''
            if self.use_gzip:
                gzip_ext = '.gz'

            def do_rename(source_fn, dest_fn):
                self._console_log("Rename %s -> %s" % (source_fn, dest_fn + gzip_ext))
                if os.path.exists(dest_fn):
                    os.remove(dest_fn)
                if os.path.exists(dest_fn + gzip_ext):
                    os.remove(dest_fn + gzip_ext)
                source_gzip = source_fn + gzip_ext
                if os.path.exists(source_gzip):
                    os.rename(source_gzip, dest_fn + gzip_ext)
                elif os.path.exists(source_fn):
                    os.rename(source_fn, dest_fn)

            for i in range(self.backupCount - 1, 0, -1):
                sfn = "%s.%d" % (self.baseFilename, i)
                dfn = "%s.%d" % (self.baseFilename, i + 1)
                if os.path.exists(sfn + gzip_ext):
                    do_rename(sfn, dfn)
            dfn = self.baseFilename + ".1"
            do_rename(tmpname, dfn)
            self._console_log("Rotation completed")
            self._degrade(False, "Rotation completed")
        finally:
            if not self.delay:
                self.stream = self._open()

    def shouldRollover(self, record):
        """
        Determine if rollover should occur.

        For those that are keeping track. This differs from the standard library's RotatingLogHandler class.
        Because there is no promise to keep the file size under maxBytes we ignore the length of the current record.
        """
        del record  # avoid pychecker warnings
        if self.stream is None:
            return False
        if self._shouldRollover():
            self._close()
            self.stream = self._open()
            return self._shouldRollover()
        return False

    def _shouldRollover(self):
        if self.maxBytes > 0:  # are we rolling over?
            self.stream.seek(0, 2)  # due to non-posix-compliant Windows feature
            if self.stream.tell() >= self.maxBytes:
                return True
            else:
                self._degrade(False, "Rotation done or not needed at this time")
        return False

    def do_gzip(self, input_filename):
        if not gzip:
            self._console_log("#no gzip available", stack=False)
            return
        out_filename = input_filename + ".gz"
        with open(input_filename, "rb") as input_fh:
            with gzip.open(out_filename, "wb") as gzip_fh:
                gzip_fh.write(input_fh.read())
        os.remove(input_filename)
        self._console_log("#gzipped: %s" % (out_filename,), stack=False)
        return


_MIDNIGHT = 24 * 60 * 60  # Number of seconds in a day


class ConcurrentTimedRotatingFileHandler(ConcurrentRotatingFileHandler):
    """
    Handler for logging to a file, rotating the log file at certain timed
    intervals.

    If backupCount is > 0, when rollover is done, no more than backupCount
    files are kept - the oldest ones are deleted.
    """

    def __init__(
            self, filename, when='h', interval=1, backup_count=0, encoding=None, delay=False, utc=False, atTime=None):
        """
        Calculate the real rollover interval, which is just the number of seconds between rollovers.  Also set the
        filename suffix used when a rollover occurs.  Current 'when' events supported:

        S - Seconds
        M - Minutes
        H - Hours
        D - Days
        midnight - roll over at midnight
        W{0-6} - roll over on a certain day; 0 - Monday

        Case of the 'when' specifier is not important; lower or upper case will work.

        :param filename:
        :param when:
        :param interval:
        :param backup_count:
        :param encoding:
        :param delay:
        :param utc:
        :param atTime:
        """
        ConcurrentRotatingFileHandler.__init__(self, filename=filename, mode='a', encoding=encoding, delay=delay)
        self.when = when.upper()
        self.backup_count = backup_count
        self.utc = utc
        self.atTime = atTime
        if self.when == 'S':
            self.interval = 1  # one second
            self.suffix = "%Y-%m-%d_%H-%M-%S"
            self.extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}(\.\w+)?$"
        elif self.when == 'M':
            self.interval = 60  # one minute
            self.suffix = "%Y-%m-%d_%H-%M"
            self.extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}(\.\w+)?$"
        elif self.when == 'H':
            self.interval = 60 * 60  # one hour
            self.suffix = "%Y-%m-%d_%H"
            self.extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}(\.\w+)?$"
        elif self.when == 'D' or self.when == 'MIDNIGHT':
            self.interval = 60 * 60 * 24  # one day
            self.suffix = "%Y-%m-%d"
            self.extMatch = r"^\d{4}-\d{2}-\d{2}(\.\w+)?$"
        elif self.when.startswith('W'):
            self.interval = 60 * 60 * 24 * 7  # one week
            if len(self.when) != 2:
                raise ValueError("You must specify a day for weekly rollover from 0 to 6 (0 is Monday): %s" % self.when)
            if self.when[1] < '0' or self.when[1] > '6':
                raise ValueError("Invalid day specified for weekly rollover: %s" % self.when)
            self.dayOfWeek = int(self.when[1])
            self.suffix = "%Y-%m-%d"
            self.extMatch = r"^\d{4}-\d{2}-\d{2}(\.\w+)?$"
        else:
            raise ValueError("Invalid rollover interval specified: %s" % self.when)

        self.extMatch = re.compile(self.extMatch, re.ASCII)
        self.interval = self.interval * interval  # multiply by units requested
        # The following line added because the filename passed in could be a
        # path object (see Issue #27493), but self.baseFilename will be a string
        filename = self.baseFilename
        if os.path.exists(filename):
            t = os.stat(filename)[ST_MTIME]
        else:
            t = int(time.time())
        self.rolloverAt = self.computeRollover(t)

    def computeRollover(self, currentTime):
        """
        Work out the rollover time based on the specified time.
        """
        result = currentTime + self.interval
        if self.when == 'MIDNIGHT' or self.when.startswith('W'):
            # This could be done with less code, but I wanted it to be clear
            if self.utc:
                t = time.gmtime(currentTime)
            else:
                t = time.localtime(currentTime)
            currentHour, currentMinute, currentSecond, currentDay = t[3], t[4], t[5], t[6]
            if self.atTime is None:
                rotate_ts = _MIDNIGHT
            else:
                rotate_ts = ((self.atTime.hour * 60 + self.atTime.minute) * 60 + self.atTime.second)
            r = rotate_ts - ((currentHour * 60 + currentMinute) * 60 + currentSecond)
            if r < 0:
                r += _MIDNIGHT
                currentDay = (currentDay + 1) % 7
            result = currentTime + r
            if self.when.startswith('W'):
                day = currentDay  # 0 is Monday
                if day != self.dayOfWeek:
                    if day < self.dayOfWeek:
                        daysToWait = self.dayOfWeek - day
                    else:
                        daysToWait = 6 - day + self.dayOfWeek + 1
                    newRolloverAt = result + (daysToWait * (60 * 60 * 24))
                    if not self.utc:
                        dstNow = t[-1]
                        dstAtRollover = time.localtime(newRolloverAt)[-1]
                        if dstNow != dstAtRollover:
                            if not dstNow:
                                addend = -3600
                            else:
                                addend = 3600
                            newRolloverAt += addend
                    result = newRolloverAt
        return result

    def shouldRollover(self, record):
        """
        Determine if rollover should occur.

        record is not used, as we are just comparing times, but it is needed so
        the method signatures are the same
        """
        t = int(time.time())
        if t >= self.rolloverAt:
            return 1
        return 0

    def getFilesToDelete(self):
        """
        Determine the files to delete when rolling over.

        More specific than the earlier method, which just used glob.glob().
        """
        dirName, baseName = os.path.split(self.baseFilename)
        fileNames = os.listdir(dirName)
        result = []
        prefix = baseName + "."
        plen = len(prefix)
        for fileName in fileNames:
            if fileName[:plen] == prefix:
                suffix = fileName[plen:]
                if self.extMatch.match(suffix):
                    result.append(os.path.join(dirName, fileName))
        if len(result) < self.backup_count:
            result = []
        else:
            result.sort()
            result = result[:len(result) - self.backup_count]
        return result

    def doRollover(self):
        """
        do a rollover; in this case, a date/time stamp is appended to the filename
        when the rollover happens.  However, you want the file to be named for the
        start of the interval, not the current time.  If there is a backup count,
        then we have to get a list of matching filenames, sort them and remove
        the one with the oldest suffix.
        """
        if self.stream:
            self.stream.close()
            self.stream = None
        currentTime = int(time.time())
        dstNow = time.localtime(currentTime)[-1]
        t = self.rolloverAt - self.interval
        if self.utc:
            timeTuple = time.gmtime(t)
        else:
            timeTuple = time.localtime(t)
            dstThen = timeTuple[-1]
            if dstNow != dstThen:
                if dstNow:
                    addend = 3600
                else:
                    addend = -3600
                timeTuple = time.localtime(t + addend)
        dfn = self.rotation_filename(self.baseFilename + "." + time.strftime(self.suffix, timeTuple))
        if os.path.exists(dfn):
            os.remove(dfn)
        self.rotate(self.baseFilename, dfn)
        if self.backup_count > 0:
            for s in self.getFilesToDelete():
                os.remove(s)
        if not self.delay:
            self.stream = self._open()
        newRolloverAt = self.computeRollover(currentTime)
        while newRolloverAt <= currentTime:
            newRolloverAt = newRolloverAt + self.interval
        if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
            dstAtRollover = time.localtime(newRolloverAt)[-1]
            if dstNow != dstAtRollover:
                if not dstNow:
                    addend = -3600
                else:
                    addend = 3600
                newRolloverAt += addend
        self.rolloverAt = newRolloverAt
