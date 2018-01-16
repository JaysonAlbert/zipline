# coding=utf-8
# Author: Hongzhong
# 2017-12-28 11:43$id

from __future__ import print_function
from lru import LRU
from intervaltree import IntervalTree
from .minute_bars import BcolzMinuteBarWriter, BcolzMinuteBarMetadata, BcolzMinuteWriterColumnMismatch, BcolzMinuteBarReader
from .minute_bars import OHLC_RATIO, DEFAULT_EXPECTEDLEN
from .minute_bars import convert_cols
import os
import pandas as pd
from toolz import keymap, valmap
import datetime
import numpy as np
import rocksdb
import logbook
import glob
import struct
import time
import pytz
from zipline.utils.memoize import lazyval


def _sid_subdir_path(sid):
    """
    Format subdir path to limit the number directories in any given
    subdirectory to 100.

    The number in each directory is designed to support at least 100000
    equities.

    Parameters:
    -----------
    sid : int
        Asset identifier.

    Returns:
    --------
    out : string
        A path for the bcolz rootdir, including subdirectory prefixes based on
        the padded string representation of the given sid.

        e.g. 1 is formatted as 00/00/000001.bcolz
    """
    padded_sid = format(sid, '06')
    return os.path.join(
        # subdir 1 00/XX
        padded_sid[0:2],
        # subdir 2 XX/00
        padded_sid[2:4],
        "{0}.rocksdb".format(str(padded_sid))
    )


class RocksdbMinuteBarWriter(BcolzMinuteBarWriter):
    """
    Class capable of writing minute OHLCV data to disk into bcolz format.

    Parameters
    ----------
    rootdir : string
        Path to the root directory into which to write the metadata and
        bcolz subdirectories.
    calendar : zipline.utils.calendars.trading_calendar.TradingCalendar
        The trading calendar on which to base the minute bars. Used to
        get the market opens used as a starting point for each periodic
        span of minutes in the index, and the market closes that
        correspond with the market opens.
    minutes_per_day : int
        The number of minutes per each period. Defaults to 390, the mode
        of minutes in NYSE trading days.
    start_session : datetime
        The first trading session in the data set.
    end_session : datetime
        The last trading session in the data set.
    default_ohlc_ratio : int, optional
        The default ratio by which to multiply the pricing data to
        convert from floats to integers that fit within np.uint32. If
        ohlc_ratios_per_sid is None or does not contain a mapping for a
        given sid, this ratio is used. Default is OHLC_RATIO (1000).
    ohlc_ratios_per_sid : dict, optional
        A dict mapping each sid in the output to the ratio by which to
        multiply the pricing data to convert the floats from floats to
        an integer to fit within the np.uint32.
    expectedlen : int, optional
        The expected length of the dataset, used when creating the initial
        bcolz ctable.

        If the expectedlen is not used, the chunksize and corresponding
        compression ratios are not ideal.

        Defaults to supporting 15 years of NYSE equity market data.
        see: http://bcolz.blosc.org/opt-tips.html#informing-about-the-length-of-your-carrays # noqa
    write_metadata : bool, optional
        If True, writes the minute bar metadata (on init of the writer).
        If False, no metadata is written (existing metadata is
        retained). Default is True.

    Notes
    -----
    Writes a bcolz directory for each individual sid, all contained within
    a root directory which also contains metadata about the entire dataset.

    Each individual asset's data is stored as a bcolz table with a column for
    each pricing field: (open, high, low, close, volume)

    The open, high, low, and close columns are integers which are 1000 times
    the quoted price, so that the data can represented and stored as an
    np.uint32, supporting market prices quoted up to the thousands place.

    volume is a np.uint32 with no mutation of the tens place.

    The 'index' for each individual asset are a repeating period of minutes of
    length `minutes_per_day` starting from each market open.
    The file format does not account for half-days.
    e.g.:
    2016-01-19 14:31
    2016-01-19 14:32
    ...
    2016-01-19 20:59
    2016-01-19 21:00
    2016-01-20 14:31
    2016-01-20 14:32
    ...
    2016-01-20 20:59
    2016-01-20 21:00

    All assets are written with a common 'index', sharing a common first
    trading day. Assets that do not begin trading until after the first trading
    day will have zeros for all pricing data up and until data is traded.

    'index' is in quotations, because bcolz does not provide an index. The
    format allows index-like behavior by writing each minute's data into the
    corresponding position of the enumeration of the aforementioned datetime
    index.

    The datetimes which correspond to each position are written in the metadata
    as integer nanoseconds since the epoch into the `minute_index` key.

    See Also
    --------
    zipline.data.minute_bars.BcolzMinuteBarReader
    """

    logger = logbook.Logger('RocksdbMinuteBarWriter')
    COL_NAMES_BYTE = (b'open', b'high', b'low', b'close', b'volume')
    COL_NAMES_BYTE_ALL = (b'default', b'open', b'high', b'low', b'close', b'volume')

    def __init__(self,
                 rootdir,
                 calendar,
                 start_session,
                 end_session,
                 minutes_per_day,
                 default_ohlc_ratio=OHLC_RATIO,
                 ohlc_ratios_per_sid=None,
                 expectedlen=DEFAULT_EXPECTEDLEN,
                 write_metadata=True):

        # self._rootdir = self.TIME_PATH_REGEX.sub("/", rootdir)
        self._rootdir = rootdir
        self._start_session = start_session
        self._end_session = end_session
        self._calendar = calendar
        slicer = (
            calendar.schedule.index.slice_indexer(start_session, end_session))
        self._schedule = calendar.schedule[slicer]
        self._session_labels = self._schedule.index
        self._minutes_per_day = minutes_per_day
        self._expectedlen = expectedlen
        self._default_ohlc_ratio = default_ohlc_ratio
        self._ohlc_ratios_per_sid = ohlc_ratios_per_sid

        self._minute_index = calendar.minutes_in_range(self._schedule.market_open[0],self._schedule.market_close[-1])

        if write_metadata:
            metadata = BcolzMinuteBarMetadata(
                self._default_ohlc_ratio,
                self._ohlc_ratios_per_sid,
                self._calendar,
                self._start_session,
                self._end_session,
                self._minutes_per_day,
            )
            metadata.write(self._rootdir)

    def __del__(self):
        if self.db:
            self.db.close()
            del self.db

    @classmethod
    def open(cls, rootdir, end_session=None):
        """
        Open an existing ``rootdir`` for writing.

        Parameters
        ----------
        end_session : Timestamp (optional)
            When appending, the intended new ``end_session``.
        """
        metadata = BcolzMinuteBarMetadata.read(rootdir)
        return BcolzMinuteBarWriter(
            rootdir,
            metadata.calendar,
            metadata.start_session,
            end_session if end_session is not None else metadata.end_session,
            metadata.minutes_per_day,
            metadata.default_ohlc_ratio,
            metadata.ohlc_ratios_per_sid,
            write_metadata=end_session is not None
        )

    def last_date_in_output_for_sid(self, sid):
        """
        Parameters:
        -----------
        sid : int
            Asset identifier.

        Returns:
        --------
        out : pd.Timestamp
            The midnight of the last date written in to the output for the
            given sid.
        """
        return None

    def _init_ctable(self, path):
        """
        Create empty ctable for given path.

        Parameters:
        -----------
        path : string
            The path to rootdir of the new ctable.
        """
        # Only create the containing subdir on creation.
        # This is not to be confused with the `.bcolz` directory, but is the
        # directory up one level from the `.bcolz` directories.
        sid_containing_dirname = os.path.dirname(path)
        if not os.path.exists(sid_containing_dirname):
            # Other sids may have already created the containing directory.
            os.makedirs(sid_containing_dirname)
        opt = rocksdb.Options(create_if_missing=True, write_buffer_size=512 * 1024 * 1024, max_write_buffer_number=5,
                              min_write_buffer_number_to_merge=2, compression=rocksdb.CompressionType.lz4_compression)
        db = rocksdb.DB(path, opt, [b"default"])
        db.create_column_family(self.COL_NAMES_BYTE)
        db.close()
        del db

    def _open_ctable(self, path):
        cols = self.COL_NAMES_BYTE_ALL
        opt = rocksdb.Options(create_if_missing=True, write_buffer_size=512 * 1024 * 1024, max_write_buffer_number=5,
                              min_write_buffer_number_to_merge=2, compression=rocksdb.CompressionType.lz4_compression)
        db = rocksdb.DB(path, opt, cols)
        return db

    db = None

    def sidpath(self, sid):
        """
        Parameters:
        -----------
        sid : int
            Asset identifier.

        Returns:
        --------
        out : string
            Full path to the bcolz rootdir for the given sid.
        """
        sid_subdir = _sid_subdir_path(sid)
        return os.path.join(self._rootdir, sid_subdir)

    def _ensure_ctable(self, sid):
        """Ensure that a ctable exists for ``sid``, then return it."""
        sidpath = self.sidpath(sid)
        if not os.path.exists(sidpath):
            self._init_ctable(sidpath)
        if self.db is None:
            self.db = self._open_ctable(sidpath)
        return self.db

    def _zerofill(self, table, numdays):
        pass

    def pad(self, sid, date):
        """
        Fill sid container with empty data through the specified date.

        If the last recorded trade is not at the close, then that day will be
        padded with zeros until its close. Any day after that (up to and
        including the specified date) will be padded with `minute_per_day`
        worth of zeros

        Parameters:
        -----------
        sid : int
            The asset identifier for the data being written.
        date : datetime-like
            The date used to calculate how many slots to be pad.
            The padding is done through the date, i.e. after the padding is
            done the `last_date_in_output_for_sid` will be equal to `date`
        """
        table = self._ensure_ctable(sid)

        last_date = self.last_date_in_output_for_sid(sid)
        if last_date is None:
            # No need to p
            return
        tds = self._session_labels



        if last_date == pd.NaT:
            # If there is no data, determine how many days to add so that
            # desired days are written to the correct slots.
            days_to_zerofill = tds[tds.slice_indexer(end=date)]
        else:
            days_to_zerofill = tds[tds.slice_indexer(
                start=last_date + tds.freq,
                end=date)]

        self._zerofill(table, len(days_to_zerofill))

        # new_last_date = self.last_date_in_output_for_sid(sid)
        # assert new_last_date == date, "new_last_date={0} != date={1}".format(
        #     new_last_date, date)

    def set_sid_attrs(self, sid, **kwargs):
        """Write all the supplied kwargs as attributes of the sid's file.
        """
        table = self._ensure_ctable(sid)
        for k, v in kwargs.items():
            table.attrs[k] = v

    def write_cols(self, sid, dts, cols, invalid_data_behavior='warn'):
        """
        Write the OHLCV data for the given sid.
        If there is no bcolz ctable yet created for the sid, create it.
        If the length of the bcolz ctable is not exactly to the date before
        the first day provided, fill the ctable with 0s up to that date.

        Parameters:
        -----------
        sid : int
            The asset identifier for the data being written.
        dts : datetime64 array
            The dts corresponding to values in cols.
        cols : dict of str -> np.array
            dict of market data with the following characteristics.
            keys are ('open', 'high', 'low', 'close', 'volume')
            open : float64
            high : float64
            low  : float64
            close : float64
            volume : float64|int64
        """
        if not all(len(dts) == len(cols[name]) for name in self.COL_NAMES):
            raise BcolzMinuteWriterColumnMismatch(
                "Length of dts={0} should match cols: {1}".format(
                    len(dts),
                    " ".join("{0}={1}".format(name, len(cols[name]))
                             for name in self.COL_NAMES)))
        self._write_cols(sid, dts, cols, invalid_data_behavior)

    def _write_cols(self, sid, dts, cols, invalid_data_behavior):
        """
        Internal method for `write_cols` and `write`.

        Parameters:
        -----------
        sid : int
            The asset identifier for the data being written.
        dts : datetime64 array
            The dts corresponding to values in cols.
        cols : dict of str -> np.array
            dict of market data with the following characteristics.
            keys are ('open', 'high', 'low', 'close', 'volume')
            open : float64
            high : float64
            low  : float64
            close : float64
            volume : float64|int64
        """
        tds = self._session_labels
        input_first_day = self._calendar.minute_to_session_label(
            pd.Timestamp(dts[0]), direction='previous')

        last_date = self.last_date_in_output_for_sid(sid)

        day_before_input = input_first_day - tds.freq

        self.pad(sid, day_before_input)
        table = self._ensure_ctable(sid)

        # Get the number of minutes already recorded in this sid's ctable
        num_rec_mins = 0

        all_minutes = self._minute_index
        # Get the latest minute we wish to write to the ctable
        last_minute_to_write = pd.Timestamp(dts[-1], tz='UTC')

        latest_min_count = all_minutes.get_loc(last_minute_to_write)

        # Get all the minutes we wish to write (all market minutes after the
        # latest currently written, up to and including last_minute_to_write)
        all_minutes_in_window = all_minutes[num_rec_mins:latest_min_count + 1]

        minutes_count = all_minutes_in_window.size

        open_col = np.zeros(minutes_count, dtype=np.uint32)
        high_col = np.zeros(minutes_count, dtype=np.uint32)
        low_col = np.zeros(minutes_count, dtype=np.uint32)
        close_col = np.zeros(minutes_count, dtype=np.uint32)
        vol_col = np.zeros(minutes_count, dtype=np.uint32)

        dt_ixs = np.searchsorted(all_minutes_in_window.values,
                                 dts.astype('datetime64[ns]'))

        ohlc_ratio = self.ohlc_ratio_for_sid(sid)

        (
            open_col[dt_ixs],
            high_col[dt_ixs],
            low_col[dt_ixs],
            close_col[dt_ixs],
            vol_col[dt_ixs],
        ) = convert_cols(cols, ohlc_ratio, sid, invalid_data_behavior)
        single_size = len(struct.pack('@i', open_col[0]))
        for i in range(0, minutes_count):
            bts = bytearray(single_size*5)
            index = 0
            bts[index: index+single_size] = struct.pack('@i',open_col[i])
            index += single_size
            bts[index: index + single_size] = struct.pack('@i', close_col[i])
            index += single_size
            bts[index: index + single_size] = struct.pack('@i', high_col[i])
            index += single_size
            bts[index: index + single_size] = struct.pack('@i', low_col[i])
            index += single_size
            bts[index: index + single_size] = struct.pack('@i', vol_col[i])
            table.put(b'default', struct.pack('>i',int(time.mktime(all_minutes_in_window[i].timetuple()))), bytes(bts))

        self.db.close()
        del self.db
        self.db = None

    def data_len_for_day(self, day):
        """
        Return the number of data points up to and including the
        provided day.
        """
        day_ix = self._session_labels.get_loc(day)
        # Add one to the 0-indexed day_ix to get the number of days.
        num_days = day_ix + 1
        return num_days * self._minutes_per_day

    def truncate(self, date):
        """Truncate data beyond this date in all ctables."""
        truncate_slice_end = self.data_len_for_day(date)

        glob_path = os.path.join(self._rootdir, "*.db")
        sid_paths = sorted(glob(glob_path))

        for sid_path in sid_paths:
            file_name = os.path.basename(sid_path)


class RocksdbMinuteBarReader(BcolzMinuteBarReader):
    """
    Reader for data written by BcolzMinuteBarWriter

    Parameters:
    -----------
    rootdir : string
        The root directory containing the metadata and asset bcolz
        directories.

    See Also
    --------
    zipline.data.minute_bars.BcolzMinuteBarWriter
    """
    FIELDS = ('open', 'close', 'high', 'low', 'volume')
    COL_NAMES_BYTE_ALL = (b'default', b'open', b'high', b'low', b'close', b'volume')
    dbs = None
    FIELD_VAL_SIZE = 4   #int struct pack value size

    def __init__(self, rootdir, sid_cache_size=1000, realtime = False):
        self._rootdir = rootdir
        self._realtime = realtime
        self.dbs = dict()
        metadata = self._get_metadata()
        size = len(struct.pack("@i", 1))
        if size != self.FIELD_VAL_SIZE:
            self.FIELD_VAL_SIZE = size
        self.FIELD_MAP = dict()
        for i, val in enumerate(self.FIELDS):
            self.FIELD_MAP[val] = i
        self._start_session = metadata.start_session
        self._end_session = metadata.end_session

        self.calendar = metadata.calendar
        tz_offset = self.calendar.tz._utcoffset.seconds
        self.tz_utcoffset_seconds = tz_offset - (tz_offset % 3600)  # hours
        slicer = self.calendar.schedule.index.slice_indexer(
            self._start_session,
            self._end_session,
        )
        self._schedule = self.calendar.schedule[slicer]
        self._market_opens = self._schedule.market_open
        self._market_open_values = self._market_opens.values. \
            astype('datetime64[m]').astype(np.int64)
        self._market_closes = self._schedule.market_close
        self._market_close_values = self._market_closes.values. \
            astype('datetime64[m]').astype(np.int64)

        self._default_ohlc_inverse = 1.0 / metadata.default_ohlc_ratio
        ohlc_ratios = metadata.ohlc_ratios_per_sid
        if ohlc_ratios:
            self._ohlc_inverses_per_sid = (
                valmap(lambda x: 1.0 / x, ohlc_ratios))
        else:
            self._ohlc_inverses_per_sid = None

        self._minutes_per_day = metadata.minutes_per_day

        self._carrays = {
            field: LRU(sid_cache_size)
            for field in self.FIELDS
        }

        self._last_get_value_dt_position = None
        self._last_get_value_dt_value = None

        # This is to avoid any bad data or other performance-killing situation
        # where there a consecutive streak of 0 (no volume) starting at an
        # asset's start date.
        # if asset 1 started on 2015-01-03 but its first trade is 2015-01-06
        # 10:31 AM US/Eastern, this dict would store {1: 23675971},
        # which is the minute epoch of that date.
        self._known_zero_volume_dict = {}

    def __del__(self):
        if self.dbs is not None and len(self.dbs) > 0:
            for db in self.dbs.values():
                db.close()
                del db
            self.dbs.clear()


    def _ohlc_ratio_inverse_for_sid(self, sid):
        if self._ohlc_inverses_per_sid is not None:
            try:
                return self._ohlc_inverses_per_sid[sid]
            except KeyError:
                pass

        # If we can not get a sid-specific OHLC inverse for this sid,
        # fallback to the default.
        return self._default_ohlc_inverse

    def _minutes_to_exclude(self):
        """
        Calculate the minutes which should be excluded when a window
        occurs on days which had an early close, i.e. days where the close
        based on the regular period of minutes per day and the market close
        do not match.

        Returns:
        --------
        List of DatetimeIndex representing the minutes to exclude because
        of early closes.
        """

        slicer = self.calendar.schedule.index.slice_indexer(
            self._start_session,
            self._end_session,
        )
        minutes_per_day = self.calendar._minutes_per_session[slicer]
        early_indices = np.where(
            minutes_per_day != self._minutes_per_day )[0]
        early_opens = self._market_opens[early_indices]
        early_closes = self._market_closes[early_indices]
        minutes = [(market_open, early_close)
                   for market_open, early_close
                   in zip(early_opens, early_closes)]
        return minutes

    @lazyval
    def _minute_exclusion_tree(self):
        """
        Build an interval tree keyed by the start and end of each range
        of positions should be dropped from windows. (These are the minutes
        between an early close and the minute which would be the close based
        on the regular period if there were no early close.)
        The value of each node is the same start and end position stored as
        a tuple.

        The data is stored as such in support of a fast answer to the question,
        does a given start and end position overlap any of the exclusion spans?

        Returns
        -------
        IntervalTree containing nodes which represent the minutes to exclude
        because of early closes.
        """
        itree = IntervalTree()
        for market_open, early_close in self._minutes_to_exclude():
            start_pos = self._find_position_of_minute(early_close) + 1
            end_pos = (
                self._find_position_of_minute(market_open)
                +
                self._minutes_per_day
                -
                1
            )
            data = (start_pos, end_pos)
            itree[start_pos:end_pos + 1] = data
        return itree

    def _exclusion_indices_for_range(self, start_idx, end_idx):
        """
        Returns
        -------
        List of tuples of (start, stop) which represent the ranges of minutes
        which should be excluded when a market minute window is requested.
        """
        itree = self._minute_exclusion_tree
        if itree.overlaps(start_idx, end_idx):
            ranges = []
            intervals = itree[start_idx:end_idx]
            for interval in intervals:
                ranges.append(interval.data)
            return sorted(ranges)
        else:
            return None

    def _get_field_value(self, sid, field):
        sidpath = self.sidpath(sid)
        db = self._open_db(sid, sidpath)
        if field not in self.FIELD_MAP:
            return None
        it = db.iteritems(b'default')
        it.seek_to_first()

        dts = []
        vals = []
        local_tz = self.calendar.tz
        for k, v in it:
            #dts.append(datetime.datetime.fromtimestamp(struct.unpack(">i", k)[0]).replace(tzinfo=pytz.UTC).astimezone(local_tz))
            dts.append(datetime.datetime.fromtimestamp(struct.unpack(">i", k)[0]))
            vals.append(struct.unpack("@i", bytearray(v)[self.FIELD_MAP[field] * self.FIELD_VAL_SIZE:(self.FIELD_MAP[field]+1) * self.FIELD_VAL_SIZE]))
        del it
        self._close_db(sid, db)
        items = {"dt": dts}
        items[field] = vals
        df = pd.DataFrame.from_dict(items)
        df["dt"] = pd.to_datetime(df['dt'])
        df.set_index(["dt"])
        return df

    def sidpath(self, sid):
        """
        Parameters:
        -----------
        sid : int
            Asset identifier.

        Returns:
        --------
        out : string
            Full path to the bcolz rootdir for the given sid.
        """
        sid_subdir = _sid_subdir_path(sid)
        return os.path.join(self._rootdir, sid_subdir)

    def _open_db(self, sid, path):
        if not self._realtime:
            if sid in self.dbs:
                return self.dbs[sid]

        cols = self.COL_NAMES_BYTE_ALL
        opt = rocksdb.Options(create_if_missing=True, write_buffer_size=512 * 1024 * 1024, max_write_buffer_number=5,
                              min_write_buffer_number_to_merge=2, compression=rocksdb.CompressionType.lz4_compression)
        db = rocksdb.DB(path, opt, cols, read_only=True)
        if not self._realtime:
            self.dbs[sid] = db
        return db

    def _close_db(self, sid, db):
        if self._realtime:
            db.close()

    def _open_minute_file(self, field, sid):
        sid = int(sid)

        try:
            carray = self._carrays[field][sid]
        except KeyError:
            carray = self._carrays[field][sid] = self._get_field_value(sid, field)

        return carray

    def table_len(self, sid):
        """Returns the length of the underlying table for this sid."""
        return len(self._open_minute_file('close', sid))

    def get_value(self, sid, dt, field):
        """
        Retrieve the pricing info for the given sid, dt, and field.

        Parameters:
        -----------
        sid : int
            Asset identifier.
        dt : datetime-like
            The datetime at which the trade occurred.
        field : string
            The type of pricing data to retrieve.
            ('open', 'high', 'low', 'close', 'volume')

        Returns:
        --------
        out : float|int

        The market data for the given sid, dt, and field coordinates.

        For OHLC:
            Returns a float if a trade occurred at the given dt.
            If no trade occurred, a np.nan is returned.

        For volume:
            Returns the integer value of the volume.
            (A volume of 0 signifies no trades for the given dt.)
        """
        dt_value = int(dt.value / 10 ** 9)
        if int(dt.tz._utcoffset.seconds / 3600) != int(self.tz_utcoffset_seconds / 3600):
            dt_value -= self.tz_utcoffset_seconds
        key = struct.pack(">i", dt_value)
        path = self.sidpath(sid.sid)
        db = self._open_db(sid.sid, path)
        value = db.get(b'default', key)
        self._close_db(sid.sid, db)
        if value is None:
            value = 0
        else:
            value = struct.unpack("@i", bytearray(value)[self.FIELD_MAP[field] * self.FIELD_VAL_SIZE:(self.FIELD_MAP[field]+1) * self.FIELD_VAL_SIZE])[0]
        if value == 0:
            if field == 'volume':
                return 0
            else:
                return np.nan

        if field != 'volume':
            value *= self._ohlc_ratio_inverse_for_sid(sid)
        return value

    def get_last_traded_dt(self, asset, dt):
        sid = asset.sid
        sidpath = self.sidpath(sid)
        key = int(dt.value // 10 ** 9)
        key = struct.pack(">i", key)
        db = self._open_db(sid, sidpath)
        it = db.iterkeys(b'default')
        it.seek_for_prev(key)
        ret_key = next(it)
        del it
        self._close_db(sid, db)
        ret_key = struct.unpack(">i", ret_key)[0]
        ret_dt = datetime.datetime.fromtimestamp(ret_key)

        return pd.Timestamp(ret_dt, tz=self.calendar.tz)

    def _find_last_traded_position(self, asset, dt):
        return None

    def _pos_to_minute(self, pos):
        return None

    def _find_position_of_minute(self, minute_dt):
        return None

    def _get_single_sid_value(self, sid, fields, start, end=None):
        sidpath = self.sidpath(sid)
        db = self._open_db(sid, sidpath)

        it = db.iteritems(b'default')
        it.seek(start)

        dts = []
        vals = dict()
        local_tz = self.calendar.tz
        for k, v in it:
            if end is not None:
                if k > end:
                    break
            dts.append(datetime.datetime.fromtimestamp(struct.unpack(">i", k)[0]))
            barr = struct.unpack("@iiiii", v)
            for field in fields:
                if field in vals:
                    val = vals[field]
                else:
                    val = []
                    vals[field] = val
                val.append(barr[self.FIELD_MAP[field]])
        del it
        self._close_db(sid, db)
        # vals["dt"] = dts
        df = pd.DataFrame.from_dict(vals)
        # df["dt"] = pd.to_datetime(df['dt'])
        ohlc_ratio = self._ohlc_ratio_inverse_for_sid(sid)
        for field in fields:
            if field == "volume":
                continue
            df[field] = df[field] * ohlc_ratio
        # df.set_index(["dt"])
        return df

    def load_raw_arrays(self, fields, start_dt, end_dt, sids):
        """
        Parameters
        ----------
        fields : list of str
           'open', 'high', 'low', 'close', or 'volume'
        start_dt: Timestamp
           Beginning of the window range.
        end_dt: Timestamp
           End of the window range.
        sids : list of int
           The asset identifiers in the window.

        Returns
        -------
        list of np.ndarray
            A list with an entry per field of ndarrays with shape
            (minutes in range, sids) with a dtype of float64, containing the
            values for the respective field over start and end dt range.
        """
        data_tz = start_dt.tz
        offset = 0
        if int(data_tz._utcoffset.seconds / 3600) != int(self.tz_utcoffset_seconds / 3600):
            offset -= self.tz_utcoffset_seconds
        start_idx = struct.pack(">i", int(start_dt.value // 10 ** 9) + offset)
        end_idx = struct.pack(">i", int(end_dt.value // 10 ** 9) + offset)

        # num_minutes = (end_idx - start_idx + 1)

        results = []

        # indices_to_exclude = self._exclusion_indices_for_range(
        #     start_idx, end_idx)
        # if indices_to_exclude is not None:
        #     for excl_start, excl_stop in indices_to_exclude:
        #         length = excl_stop - excl_start + 1
        #         num_minutes -= length
        #
        # shape = num_minutes, len(sids)

        for sid in sids:
            results.append(self._get_single_sid_value(sid.sid, fields, start_idx, end_idx))
        return results