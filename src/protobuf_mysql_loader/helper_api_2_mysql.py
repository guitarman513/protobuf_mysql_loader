import struct
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4
from typing import List, Optional
from enum import Enum

UTC_STRFTIME_STRING_SAFE_FOR_MYSQL = "%Y-%m-%d %H:%M:%S.%f"
# __UTC_STRFTIME_STRING = "%Y-%m-%dT%H:%M:%S.%f%z" # Mysql can't handle T or Z

def datetime_to_us(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000)

def us_to_zulu(micro_time:int) -> str:
    dt = datetime.fromtimestamp(micro_time / 1_000_000, tz=timezone.utc)
    return dt.strftime(UTC_STRFTIME_STRING_SAFE_FOR_MYSQL)

def zulu_to_us(zulutime:str) -> int:
    return datetime_to_us(datetime.strptime(zulutime, UTC_STRFTIME_STRING_SAFE_FOR_MYSQL))

class ObDataType(Enum):
    U64 = 'Q'
    F32 = 'f'
    F64 = 'd'
    
def pack_list_of_values_as_little_endian_bytes(values:List, dtype:ObDataType) -> bytes:
    # to pack '1' do struct.pack("<Q", 1). For '1,2,3' struct.pack("<3Q", 1, 2, 3)
    return struct.pack(f"<{len(values)}{dtype.value}", *values)

def unpack_little_endian_bytes_to_values(blob:bytes, dtype:ObDataType) -> List:
    item_size = struct.calcsize(dtype.value)
    count = len(blob) // item_size
    return list(struct.unpack(f"<{count}{dtype.value}", blob))


@dataclass
class Observation:
    # ob-specific stuff
    obtime: datetime
    pitrf: List[float]
    vitrf: List[float]
    ra: float
    dec: float
    ra_unc: float
    dec_unc: float
    mag: float
    
    # track-specific stuff that doesn't change per-ob but is still reported in each ob
    id_on_orbit: str
    id_sensor: str
    sat_no: str
    orig_object_id: str
    orig_sensor_id: str
    uct: bool

    @classmethod
    def from_proto(cls, ob):
        return cls(
            obtime=datetime.strptime(ob.ob_time.value, "%Y-%m-%dT%H:%M:%S.%fZ"),
            pitrf=[float(ob.senx.value), float(ob.seny.value), float(ob.senz.value)],
            vitrf=[float(ob.senvelx.value), float(ob.senvely.value), float(ob.senvelz.value)],
            ra=float(ob.ra.value),
            dec=float(ob.declination.value),
            ra_unc=float(ob.ra_unc.value),
            dec_unc=float(ob.declination_unc.value),
            mag=float(ob.mag.value),
            
            id_on_orbit=str(ob.id_on_orbit.value),
            id_sensor=str(ob.id_sensor.value),
            sat_no=str(ob.sat_no.value),
            orig_object_id=str(ob.orig_object_id.value),
            orig_sensor_id=str(ob.orig_sensor_id.value),
            uct=bool(ob.uct.value),
        )


@dataclass
class MySQLRecord:
    # Categorical data about the track. Ref Frame is given to us in ECEF.
    track_id:Optional[int]  # auto-incrementing integer added by the mysql db. Keep this exposed here so we can load from tables into this data structure.
    id_on_orbit:str         # Usually blank for . Unique ID of the satellite being observed. orig_sensor_id contains this info. If UCT, this could be an internal identifier rather than an official one.
    id_sensor:str           # Usually blank for . Unique ID of the sensor. orig_sensor_id contains this info.
    sat_no: str             # Satellite/Catalog number of the target on-orbit object if correlated.
    orig_object_id:str      # Detected object. Sometimes NORAD sometimes internal (if starlink) If they give something like satellite34366 it means starlink-34366 (We shouldn't rename it!)
    orig_sensor_id: str     # Something like sdfsdf-11269-4 
    uct: bool               # if True then either didn't try to correlate the track or couldn't.
    
    # Timestamps. These come to us as ISO 8601 UTC with microsecond precision. #NOTE: right of this comment is old: we convert them to Unix Timestamps (microseconds since 1970) e.g. 1751915752503105
    trackstart_utc: int          # The earliest timestamp in the track
    trackend_utc: int            # The latest timestamp in the track
    median_timestamp_utc: int    # The middle timestamp in the track
    rx_time_utc: int             # The time MITLL uploaded the record to the mysql database
    
    # Summary Stats. Really what we're calling itrf are ECEF values given to us by SpaceX. RA/Dec are J2000.
    median_ra_deg: float    # [0 and 360]
    median_dec_deg: float   # [-90,90]
    median_senx_itrf_km: float # e.g. 4040300.0
    median_seny_itrf_km: float # e.g. -3428865.8
    median_senz_itrf_km: float # e.g. -4479361.5
    median_mag: float       # Usually between -0.5 and 9 e.g. 3.34
    
    # BLOBS (Binary Large OBjectS). These are encoded and decoded (little-endian) without delimiters so knowing the datatype of the underlying data is crucial!
    timestamp_us_blob: bytes         # u64 ints representing the unix timestamps in microseconds
    ra_and_dec_deg_blob: bytes       # f64
    ra_and_dec_unc_deg_blob: bytes   # f32, allegedly 1 sigma uncertainty
    sen_pos_xyz_itrf_km_blob: bytes  # f32, but three-times as many values as some other blobs since we have x,y,z 
    sen_vel_xyz_itrf_kms_blob: bytes # f32, but three-times as many values as some other blobs since we have x,y,z 
    mag_blob: bytes                  # f32
    

def mysqlify_track(sx_api_track):
    # Cast raw data into usable structures
    distilled_observations:List[Observation] = [Observation.from_proto(ob) for ob in sx_api_track.udl_observation_data]
    n_obs = len(distilled_observations)
    assert n_obs > 0 # each track should have obs...
    
    # get track-level data from one of the obs
    id_on_orbit = distilled_observations[0].id_on_orbit # DON'T RENAME "satellite" to "Starlink". Keep it as is!
    id_sensor = distilled_observations[0].id_sensor
    sat_no = distilled_observations[0].sat_no
    orig_object_id = distilled_observations[0].orig_object_id
    orig_sensor_id = distilled_observations[0].orig_sensor_id
    uct = distilled_observations[0].uct
    
    trackstart_utc = distilled_observations[0].obtime
    trackend_utc = distilled_observations[-1].obtime
    median_timestamp_utc = distilled_observations[n_obs//2].obtime
    rx_time_utc = datetime.now().strftime(UTC_STRFTIME_STRING_SAFE_FOR_MYSQL) # supposed to be the time the database received the data... so this is close enough for jazz
    
    median_ra_deg = distilled_observations[n_obs//2].ra
    median_dec_deg = distilled_observations[n_obs//2].dec
    median_senx_itrf_km = distilled_observations[n_obs//2].pitrf[0] # 0 is x coord
    median_seny_itrf_km = distilled_observations[n_obs//2].pitrf[1] # 1 is y coord
    median_senz_itrf_km = distilled_observations[n_obs//2].pitrf[2] # 2 is z coord
    median_mag = distilled_observations[n_obs//2].mag
    
    timestamp_us_blob         = pack_list_of_values_as_little_endian_bytes([datetime_to_us(o.obtime) for o in distilled_observations], ObDataType.U64)
    ra_and_dec_deg_blob       = pack_list_of_values_as_little_endian_bytes([ra_or_dec for ra_dec_pair in zip([o.ra for o in distilled_observations], [o.dec for o in distilled_observations]) for ra_or_dec in ra_dec_pair], ObDataType.F64) # sexy one liner sorry for anyone else who has to read this (or my future self) 
    ra_and_dec_unc_deg_blob   = pack_list_of_values_as_little_endian_bytes([ra_or_dec_unc for ra_dec_unc_pair in zip([o.ra_unc for o in distilled_observations], [o.dec_unc for o in distilled_observations]) for ra_or_dec_unc in ra_dec_unc_pair], ObDataType.F32) # same apologies as above
    sen_pos_xyz_itrf_km_blob  = pack_list_of_values_as_little_endian_bytes([coord for o in distilled_observations for coord in [o.pitrf[0],o.pitrf[1],o.pitrf[2]]], ObDataType.F32)
    sen_vel_xyz_itrf_kms_blob = pack_list_of_values_as_little_endian_bytes([coord for o in distilled_observations for coord in [o.vitrf[0],o.vitrf[1],o.vitrf[2]]], ObDataType.F32)
    mag_blob                  = pack_list_of_values_as_little_endian_bytes([o.mag for o in distilled_observations], ObDataType.F32)
    
    return MySQLRecord(
        track_id = None, # make sure to not push this when adding to the table!!
        id_on_orbit = id_on_orbit,
        sat_no = sat_no,
        id_sensor = id_sensor,
        orig_object_id = orig_object_id,
        orig_sensor_id = orig_sensor_id,
        uct = uct,

        trackstart_utc = trackstart_utc,
        trackend_utc = trackend_utc,
        median_timestamp_utc = median_timestamp_utc,
        rx_time_utc = rx_time_utc,
        
        median_ra_deg = median_ra_deg,
        median_dec_deg = median_dec_deg,
        median_senx_itrf_km = median_senx_itrf_km,
        median_seny_itrf_km = median_seny_itrf_km,
        median_senz_itrf_km = median_senz_itrf_km,
        median_mag = median_mag,
        
        timestamp_us_blob = timestamp_us_blob,
        ra_and_dec_deg_blob = ra_and_dec_deg_blob,
        ra_and_dec_unc_deg_blob = ra_and_dec_unc_deg_blob,
        sen_pos_xyz_itrf_km_blob = sen_pos_xyz_itrf_km_blob,
        sen_vel_xyz_itrf_kms_blob = sen_vel_xyz_itrf_kms_blob,
        mag_blob = mag_blob,
    )