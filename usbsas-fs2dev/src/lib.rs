//! usbsas process responsible for writing the file system on destination USB
//! device. It can also wipe devices (with 0's).

use bitvec::prelude::*;
use byteorder::{LittleEndian, ReadBytesExt};
use log::{debug, error, trace};
use proto::fs2dev::{self, request::Msg};
use std::{
    fs::File,
    io::{prelude::*, SeekFrom},
};
use thiserror::Error;
use usbsas_comm::{ComRpFs2Dev, ProtoRespCommon, ProtoRespFs2Dev};
use usbsas_proto::{self as proto, common::Status};
use usbsas_utils::SECTOR_SIZE;
#[cfg(not(feature = "mock"))]
use {
    std::os::unix::io::AsRawFd,
    usbsas_comm::ToFd,
    usbsas_mass_storage::{self, MassStorage},
};
#[cfg(feature = "mock")]
use {
    std::{env, fs::OpenOptions},
    usbsas_mock::mass_storage::MockMassStorage as MassStorage,
};

#[derive(Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    IO(#[from] std::io::Error),
    #[error("{0}")]
    Error(String),
    #[error("mass_storage error: {0}")]
    MassStorage(#[from] usbsas_mass_storage::Error),
    #[error("sandbox: {0}")]
    Sandbox(#[from] usbsas_sandbox::Error),
    #[error("var error: {0}")]
    VarError(#[from] std::env::VarError),
    #[error("Bad Request")]
    BadRequest,
    #[error("State error")]
    State,
}
pub type Result<T> = std::result::Result<T, Error>;

// Some usb keys don't support bigger buffers
// (Linux writes 240 sectors per scsi write(10) requests)
const MAX_WRITE_SECTORS: usize = 240;
const BUFFER_MAX_WRITE_SIZE: u64 = MAX_WRITE_SECTORS as u64 * SECTOR_SIZE;

enum State {
    Init(InitState),
    DevOpened(DevOpenedState),
    BitVecLoaded(BitVecLoadedState),
    Copying(CopyingState),
    Reading(ReadingState),
    Wiping(WipingState),
    WaitEnd(WaitEndState),
    End,
}

impl State {
    fn run(self, comm: &mut ComRpFs2Dev) -> Result<Self> {
        match self {
            State::Init(s) => s.run(comm),
            State::DevOpened(s) => s.run(comm),
            State::BitVecLoaded(s) => s.run(comm),
            State::WaitEnd(s) => s.run(comm),
            State::Copying(s) => s.run(comm),
            State::Reading(s) => s.run(comm),
            State::Wiping(s) => s.run(comm),
            State::End => Err(Error::State),
        }
    }
}

struct InitState {
    fs_fname: String,
}

struct DevOpenedState {
    fs: File,
    mass_storage: MassStorage,
}

struct BitVecLoadedState {
    fs: File,
    fs_bv: BitVecIterOnes,
    mass_storage: MassStorage,
}

struct CopyingState {
    fs: File,
    fs_bv: BitVecIterOnes,
    mass_storage: MassStorage,
}

struct WipingState {
    fs: File,
    mass_storage: MassStorage,
}

struct ReadingState {
    fs: File,
    mass_storage: MassStorage,
    dev_size: u64,
}

struct WaitEndState;

// Wrapper around BitVec to iterate over contiguous group of ones
struct BitVecIterOnes {
    pub bv: BitVec<u8, Lsb0>,
    pos: usize,
    next_stop: usize,
}

impl BitVecIterOnes {
    fn new(bv: BitVec<u8, Lsb0>) -> Self {
        BitVecIterOnes {
            bv,
            pos: 0,
            next_stop: 0,
        }
    }
    fn count_ones(&self) -> usize {
        self.bv.count_ones()
    }
}

impl Iterator for BitVecIterOnes {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        let index_start = self.pos + self.bv[self.pos..].iter().position(|bit| *bit)?;
        if self.next_stop <= index_start {
            self.next_stop = index_start
                + self.bv[index_start..]
                    .iter()
                    .position(|bit| !*bit)
                    .unwrap_or_else(|| self.bv[index_start..].len());
        }
        self.pos = if self.next_stop - index_start > MAX_WRITE_SECTORS {
            index_start + MAX_WRITE_SECTORS
        } else {
            self.next_stop
        };
        Some((index_start as u64, self.pos as u64))
    }
}

impl InitState {
    fn run(self, comm: &mut ComRpFs2Dev) -> Result<State> {
        let busnum = comm.read_u32::<LittleEndian>()?;
        let devnum = comm.read_u32::<LittleEndian>()?;

        debug!("unlocked with busnum={busnum} devnum={devnum}");

        if busnum == 0 && devnum == 0 {
            #[cfg(not(feature = "mock"))]
            usbsas_sandbox::fs2dev::seccomp(comm.input_fd(), comm.output_fd(), None, None)?;
            return Ok(State::WaitEnd(WaitEndState));
        }

        let fs = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.fs_fname)?;

        #[cfg(not(feature = "mock"))]
        let (device_file, device_fd) = {
            let device_path = format!("/dev/bus/usb/{busnum:03}/{devnum:03}");
            match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(device_path)
            {
                Ok(file) => {
                    let file_fd = file.as_raw_fd();
                    (file, file_fd)
                }
                Err(err) => {
                    error!("Error opening device file: {err}");
                    comm.error(err)?;
                    return Ok(State::WaitEnd(WaitEndState {}));
                }
            }
        };

        #[cfg(feature = "mock")]
        let device_file = OpenOptions::new()
            .read(false)
            .write(true)
            .open(env::var("USBSAS_MOCK_OUT_DEV")?)?;

        let mass_storage = MassStorage::from_opened_file(device_file)?;

        #[cfg(not(feature = "mock"))]
        usbsas_sandbox::fs2dev::seccomp(
            comm.input_fd(),
            comm.output_fd(),
            Some(fs.as_raw_fd()),
            Some(device_fd),
        )?;

        Ok(State::DevOpened(DevOpenedState { fs, mass_storage }))
    }
}

impl CopyingState {
    fn run(mut self, comm: &mut ComRpFs2Dev) -> Result<State> {
        trace!("copying state");
        comm.writefs(proto::fs2dev::ResponseWriteFs {})?;

        let fs_size = self.fs.seek(SeekFrom::End(0))?;
        self.fs.rewind()?;

        let total_size = self.fs_bv.count_ones() as u64 * SECTOR_SIZE;

        trace!("state=copying: size={total_size} ");

        let mut current_size = 0u64;
        let mut buffer = vec![0; BUFFER_MAX_WRITE_SIZE as usize];
        let mut status_counter: u64 = 0;

        for (sector_start, sector_stop) in self.fs_bv {
            let sector_start_pos = sector_start * SECTOR_SIZE;
            self.fs.seek(SeekFrom::Start(sector_start_pos))?;

            let sector_count = sector_stop - sector_start;
            let sector_write_size = sector_count * SECTOR_SIZE;

            let (size, pad) = if sector_start_pos + sector_write_size > fs_size {
                let size = fs_size - sector_start_pos;
                (size, (sector_write_size - size))
            } else {
                (sector_write_size, 0)
            };

            self.fs.read_exact(&mut buffer[..size as usize])?;
            buffer[size as usize..]
                .iter_mut()
                .take(pad as usize)
                .for_each(|b| *b = 0);

            self.mass_storage.scsi_write_10(
                &mut buffer[..size as usize + pad as usize],
                sector_start,
                sector_count,
            )?;

            current_size += sector_write_size;
            status_counter += 1;
            if status_counter.is_multiple_of(10) {
                comm.status(current_size, total_size, false, Status::WriteDst)?;
            }
        }
        comm.done(Status::WriteDst)?;
        Ok(State::WaitEnd(WaitEndState))
    }
}

impl WipingState {
    fn run(mut self, comm: &mut ComRpFs2Dev) -> Result<State> {
        trace!("wiping state");
        comm.wipe(proto::fs2dev::ResponseWipe {})?;
        let mut buffer = vec![0u8; BUFFER_MAX_WRITE_SIZE as usize];
        let total_size = self.mass_storage.dev_size;
        let mut todo = total_size;
        let mut sector_index = 0;
        let mut sector_count = buffer.len() as u64 / SECTOR_SIZE;
        let mut current_size = 0;
        let mut status_counter: u64 = 0;
        trace!(
            "wipe device; size: {} total sectors: {}",
            total_size,
            total_size / SECTOR_SIZE
        );

        while todo > 0 {
            trace!("wipe cur size: {current_size}, sector index: {sector_index}, todo: {todo}",);
            if todo < buffer.len() as u64 {
                sector_count = todo / SECTOR_SIZE;
                buffer.truncate(todo as usize);
            }
            self.mass_storage
                .scsi_write_10(&mut buffer, sector_index, sector_count)?;
            current_size += buffer.len() as u64;

            todo -= buffer.len() as u64;
            sector_index += sector_count;

            status_counter += 1;
            if status_counter.is_multiple_of(10) {
                comm.status(current_size, total_size, false, Status::Wipe)?;
            }
        }
        comm.done(Status::Wipe)?;
        Ok(State::DevOpened(DevOpenedState {
            fs: self.fs,
            mass_storage: self.mass_storage,
        }))
    }
}

/// FAT32 BPB info needed to determine used clusters
struct Fat32Info {
    /// Sector where the partition (and BPB) starts
    partition_start: u64,
    /// Sectors before the FAT table (relative to partition start)
    reserved_sectors: u64,
    /// Number of FAT copies
    num_fats: u64,
    /// Size of each FAT in sectors
    fat_size: u64,
    /// Sectors per cluster
    sectors_per_cluster: u64,
    /// Total number of data clusters
    total_clusters: u64,
    /// First sector of the data region (absolute)
    data_start_sector: u64,
}

/// NTFS info from boot sector, needed to read the $Bitmap
struct NtfsInfo {
    /// Sector where the partition starts
    partition_start: u64,
    /// Sectors per cluster
    sectors_per_cluster: u64,
    /// Total number of clusters in the volume
    total_clusters: u64,
    /// Absolute sector where the MFT starts
    mft_start_sector: u64,
    /// Size of one MFT record in bytes
    mft_record_size: u64,
}

impl ReadingState {
    fn run(mut self, comm: &mut ComRpFs2Dev) -> Result<State> {
        trace!("reading state - reading device to file");
        comm.readfs(proto::fs2dev::ResponseReadFs {})?;
        self.fs.rewind()?;

        // Try FAT32-aware sparse read, then NTFS, fallback to full read
        let result = match self.parse_fat32_info() {
            Ok(info) => {
                debug!(
                    "FAT32 detected: partition_start={}, data_start={}, clusters={}, spc={}",
                    info.partition_start, info.data_start_sector,
                    info.total_clusters, info.sectors_per_cluster
                );
                self.read_fat32_sparse(comm, &info)
            }
            Err(fat_err) => {
                debug!("FAT32 detection failed ({fat_err}), trying NTFS");
                match self.parse_ntfs_info() {
                    Ok(info) => {
                        debug!(
                            "NTFS detected: partition_start={}, clusters={}, spc={}, mft_sector={}",
                            info.partition_start, info.total_clusters,
                            info.sectors_per_cluster, info.mft_start_sector
                        );
                        self.read_ntfs_sparse(comm, &info)
                    }
                    Err(ntfs_err) => {
                        debug!("NTFS detection failed ({ntfs_err}), falling back to full read");
                        self.read_full(comm)
                    }
                }
            }
        };
        result?;

        self.fs.flush()?;
        comm.done(Status::ReadSrc)?;

        Ok(State::DevOpened(DevOpenedState {
            fs: self.fs,
            mass_storage: self.mass_storage,
        }))
    }

    /// Try to parse FAT32 BPB from the USB device
    fn parse_fat32_info(&mut self) -> Result<Fat32Info> {
        // Read MBR (sector 0) to find partition start
        let mbr = self.mass_storage.read_sectors(0, 1, SECTOR_SIZE as usize)?;

        // Check MBR signature
        if mbr[510] != 0x55 || mbr[511] != 0xAA {
            return Err(Error::Error("No MBR signature".into()));
        }

        // Read first partition entry (offset 446, 16 bytes)
        let partition_start =
            u32::from_le_bytes([mbr[454], mbr[455], mbr[456], mbr[457]]) as u64;
        if partition_start == 0 {
            return Err(Error::Error("No partition found".into()));
        }

        // Read BPB at partition start
        let bpb = self
            .mass_storage
            .read_sectors(partition_start, 1, SECTOR_SIZE as usize)?;

        // Validate FAT32: jump boot code and "FAT32" signature at offset 82
        if bpb[0] != 0xEB && bpb[0] != 0xE9 {
            return Err(Error::Error("No FAT jump boot".into()));
        }
        let fs_type = &bpb[82..87];
        if fs_type != b"FAT32" {
            return Err(Error::Error(format!(
                "Not FAT32: {:?}",
                String::from_utf8_lossy(fs_type)
            )));
        }

        let bytes_per_sector =
            u16::from_le_bytes([bpb[11], bpb[12]]) as u64;
        if bytes_per_sector != SECTOR_SIZE {
            return Err(Error::Error("Unexpected sector size".into()));
        }

        let sectors_per_cluster = bpb[13] as u64;
        let reserved_sectors = u16::from_le_bytes([bpb[14], bpb[15]]) as u64;
        let num_fats = bpb[16] as u64;
        let fat_size =
            u32::from_le_bytes([bpb[36], bpb[37], bpb[38], bpb[39]]) as u64;
        let total_sectors =
            u32::from_le_bytes([bpb[32], bpb[33], bpb[34], bpb[35]]) as u64;

        let data_start_rel = reserved_sectors + num_fats * fat_size;
        let data_sectors = total_sectors.saturating_sub(data_start_rel);
        let total_clusters = data_sectors / sectors_per_cluster;

        if total_clusters == 0 || sectors_per_cluster == 0 {
            return Err(Error::Error("Invalid FAT32 geometry".into()));
        }

        Ok(Fat32Info {
            partition_start,
            reserved_sectors,
            num_fats,
            fat_size,
            sectors_per_cluster,
            total_clusters,
            data_start_sector: partition_start + data_start_rel,
        })
    }

    /// Build a bitmap of used clusters by reading the FAT table
    fn read_fat32_used_clusters(&mut self, info: &Fat32Info) -> Result<Vec<bool>> {
        let fat_start = info.partition_start + info.reserved_sectors;
        let mut used = vec![false; info.total_clusters as usize + 2];

        // Read FAT table in chunks
        let mut fat_offset = 0u64;
        let fat_total = info.fat_size;
        while fat_offset < fat_total {
            let chunk_sectors = (fat_total - fat_offset).min(BUFFER_MAX_WRITE_SIZE / SECTOR_SIZE);
            let data = self.mass_storage.read_sectors(
                fat_start + fat_offset,
                chunk_sectors,
                SECTOR_SIZE as usize,
            )?;

            // Parse 4-byte FAT32 entries
            let base_entry = (fat_offset * SECTOR_SIZE / 4) as usize;
            for i in 0..data.len() / 4 {
                let entry_idx = base_entry + i;
                if entry_idx < 2 || entry_idx >= used.len() {
                    continue;
                }
                let entry = u32::from_le_bytes([
                    data[i * 4],
                    data[i * 4 + 1],
                    data[i * 4 + 2],
                    data[i * 4 + 3],
                ]) & 0x0FFF_FFFF;
                used[entry_idx] = entry != 0;
            }

            fat_offset += chunk_sectors;
        }

        Ok(used)
    }

    /// FAT32-aware read: only read metadata + used clusters
    fn read_fat32_sparse(&mut self, comm: &mut ComRpFs2Dev, info: &Fat32Info) -> Result<()> {
        let used_clusters = self.read_fat32_used_clusters(info)?;
        let used_count: u64 = used_clusters.iter().filter(|&&u| u).count() as u64;
        let metadata_sectors = info.data_start_sector;
        let total_to_read =
            metadata_sectors * SECTOR_SIZE + used_count * info.sectors_per_cluster * SECTOR_SIZE;

        debug!(
            "FAT32 sparse read: {}/{} clusters used, reading ~{}MB instead of ~{}MB",
            used_count,
            info.total_clusters,
            total_to_read / (1024 * 1024),
            self.dev_size / (1024 * 1024)
        );

        let mut current_read = 0u64;
        let mut status_counter = 0u64;

        // 1) Read everything before the data region (MBR + reserved + FAT tables)
        let mut sector = 0u64;
        while sector < info.data_start_sector {
            let count = (info.data_start_sector - sector).min(BUFFER_MAX_WRITE_SIZE / SECTOR_SIZE);
            let data = self
                .mass_storage
                .read_sectors(sector, count, SECTOR_SIZE as usize)?;
            self.fs.write_all(&data)?;
            sector += count;
            current_read += count * SECTOR_SIZE;
            status_counter += 1;
            if status_counter.is_multiple_of(10) {
                comm.status(current_read, total_to_read, false, Status::ReadSrc)?;
            }
        }

        // 2) Read only used clusters in the data region, seek past free ones
        for cluster_idx in 2..(info.total_clusters as usize + 2) {
            let cluster_sector =
                info.data_start_sector + (cluster_idx as u64 - 2) * info.sectors_per_cluster;
            let cluster_bytes = info.sectors_per_cluster * SECTOR_SIZE;

            if cluster_idx < used_clusters.len() && used_clusters[cluster_idx] {
                // Used cluster: read from USB and write to .img
                let data = self.mass_storage.read_sectors(
                    cluster_sector,
                    info.sectors_per_cluster,
                    SECTOR_SIZE as usize,
                )?;
                self.fs.write_all(&data)?;
                current_read += cluster_bytes;
            } else {
                // Free cluster: seek past it (sparse)
                self.fs.seek(SeekFrom::Current(cluster_bytes as i64))?;
            }

            status_counter += 1;
            if status_counter.is_multiple_of(100) {
                comm.status(current_read, total_to_read, false, Status::ReadSrc)?;
            }
        }

        Ok(())
    }

    /// Try to parse NTFS boot sector from the USB device
    fn parse_ntfs_info(&mut self) -> Result<NtfsInfo> {
        // Read MBR (sector 0) to find partition start
        let mbr = self.mass_storage.read_sectors(0, 1, SECTOR_SIZE as usize)?;
        if mbr[510] != 0x55 || mbr[511] != 0xAA {
            return Err(Error::Error("No MBR signature".into()));
        }

        let partition_start =
            u32::from_le_bytes([mbr[454], mbr[455], mbr[456], mbr[457]]) as u64;
        if partition_start == 0 {
            return Err(Error::Error("No partition found".into()));
        }

        // Read NTFS boot sector at partition start
        let boot = self
            .mass_storage
            .read_sectors(partition_start, 1, SECTOR_SIZE as usize)?;

        // Check NTFS OEM ID at offset 3
        if &boot[3..7] != b"NTFS" {
            return Err(Error::Error(format!(
                "Not NTFS: {:?}",
                String::from_utf8_lossy(&boot[3..11])
            )));
        }

        let bytes_per_sector = u16::from_le_bytes([boot[11], boot[12]]) as u64;
        if bytes_per_sector != SECTOR_SIZE {
            return Err(Error::Error("Unexpected sector size".into()));
        }

        let sectors_per_cluster = boot[13] as u64;
        let total_sectors = u64::from_le_bytes([
            boot[40], boot[41], boot[42], boot[43],
            boot[44], boot[45], boot[46], boot[47],
        ]);
        let mft_cluster = u64::from_le_bytes([
            boot[48], boot[49], boot[50], boot[51],
            boot[52], boot[53], boot[54], boot[55],
        ]);

        // MFT record size: boot[64] is signed
        // If positive: clusters per MFT record
        // If negative: 2^|value| bytes per record
        let mft_record_size = {
            let val = boot[64] as i8;
            if val > 0 {
                val as u64 * sectors_per_cluster * SECTOR_SIZE
            } else {
                1u64 << (-val as u64)
            }
        };

        let total_clusters = total_sectors / sectors_per_cluster;
        let mft_start_sector = partition_start + mft_cluster * sectors_per_cluster;

        if total_clusters == 0 || sectors_per_cluster == 0 {
            return Err(Error::Error("Invalid NTFS geometry".into()));
        }

        Ok(NtfsInfo {
            partition_start,
            sectors_per_cluster,
            total_clusters,
            mft_start_sector,
            mft_record_size,
        })
    }

    /// Apply NTFS fixup array to an MFT record (restores sector end bytes)
    fn apply_ntfs_fixup(record: &mut [u8]) -> Result<()> {
        let fixup_offset = u16::from_le_bytes([record[4], record[5]]) as usize;
        let fixup_count = u16::from_le_bytes([record[6], record[7]]) as usize;

        if fixup_count < 2 || fixup_offset + fixup_count * 2 > record.len() {
            return Err(Error::Error("Invalid fixup array".into()));
        }

        let signature = u16::from_le_bytes([
            record[fixup_offset],
            record[fixup_offset + 1],
        ]);

        for i in 1..fixup_count {
            let sector_end = i * SECTOR_SIZE as usize;
            if sector_end > record.len() {
                break;
            }
            let stored = u16::from_le_bytes([
                record[sector_end - 2],
                record[sector_end - 1],
            ]);
            if stored != signature {
                return Err(Error::Error("Fixup signature mismatch".into()));
            }
            record[sector_end - 2] = record[fixup_offset + i * 2];
            record[sector_end - 1] = record[fixup_offset + i * 2 + 1];
        }

        Ok(())
    }

    /// Parse NTFS data runs encoding into (absolute_cluster, cluster_count) pairs
    fn parse_ntfs_data_runs(data: &[u8]) -> Result<Vec<(u64, u64)>> {
        let mut runs = Vec::new();
        let mut pos = 0;
        let mut prev_offset: i64 = 0;

        loop {
            if pos >= data.len() || data[pos] == 0 {
                break;
            }

            let header = data[pos];
            let len_size = (header & 0x0F) as usize;
            let off_size = ((header >> 4) & 0x0F) as usize;
            pos += 1;

            if len_size == 0 || pos + len_size + off_size > data.len() {
                break;
            }

            // Read run length (unsigned)
            let mut length: u64 = 0;
            for i in 0..len_size {
                length |= (data[pos + i] as u64) << (i * 8);
            }
            pos += len_size;

            // Read run offset (signed, relative to previous run)
            if off_size == 0 {
                // Sparse run: skip (unallocated)
                continue;
            }
            let mut offset: i64 = 0;
            for i in 0..off_size {
                offset |= (data[pos + i] as i64) << (i * 8);
            }
            // Sign extend
            if (data[pos + off_size - 1] & 0x80) != 0 {
                for i in off_size..8 {
                    offset |= 0xFFi64 << (i * 8);
                }
            }
            pos += off_size;

            prev_offset += offset;
            runs.push((prev_offset as u64, length));
        }

        if runs.is_empty() {
            return Err(Error::Error("No data runs found".into()));
        }
        Ok(runs)
    }

    /// Read the NTFS $Bitmap file to determine which clusters are used
    fn read_ntfs_bitmap(&mut self, info: &NtfsInfo) -> Result<Vec<bool>> {
        // $Bitmap is MFT entry #6
        let bitmap_byte_offset = 6 * info.mft_record_size;
        let bitmap_sector = info.mft_start_sector + bitmap_byte_offset / SECTOR_SIZE;
        let sectors_per_record = info.mft_record_size.div_ceil(SECTOR_SIZE);

        let entry_data = self.mass_storage.read_sectors(
            bitmap_sector,
            sectors_per_record,
            SECTOR_SIZE as usize,
        )?;

        let entry_within = (bitmap_byte_offset % SECTOR_SIZE) as usize;
        let mut entry = entry_data[entry_within..entry_within + info.mft_record_size as usize]
            .to_vec();

        // Verify FILE signature
        if &entry[0..4] != b"FILE" {
            return Err(Error::Error("$Bitmap: no FILE signature".into()));
        }

        // Apply fixup array to restore sector-end bytes
        Self::apply_ntfs_fixup(&mut entry)?;

        // Walk attributes to find $DATA (type 0x80)
        let first_attr = u16::from_le_bytes([entry[20], entry[21]]) as usize;
        let mut attr_off = first_attr;

        loop {
            if attr_off + 8 > entry.len() {
                return Err(Error::Error("$Bitmap: $DATA not found (overflow)".into()));
            }

            let attr_type = u32::from_le_bytes([
                entry[attr_off],
                entry[attr_off + 1],
                entry[attr_off + 2],
                entry[attr_off + 3],
            ]);

            if attr_type == 0xFFFF_FFFF {
                return Err(Error::Error("$Bitmap: end of attributes, no $DATA".into()));
            }

            let attr_size = u32::from_le_bytes([
                entry[attr_off + 4],
                entry[attr_off + 5],
                entry[attr_off + 6],
                entry[attr_off + 7],
            ]) as usize;

            if attr_type == 0x80 {
                // Found $DATA attribute
                let non_resident = entry[attr_off + 8];
                if non_resident == 0 {
                    return Err(Error::Error("$Bitmap $DATA is resident (unexpected)".into()));
                }

                // Non-resident: data runs offset at +32, real size at +48
                let runs_offset = u16::from_le_bytes([
                    entry[attr_off + 32],
                    entry[attr_off + 33],
                ]) as usize;

                let real_size = u64::from_le_bytes([
                    entry[attr_off + 48],
                    entry[attr_off + 49],
                    entry[attr_off + 50],
                    entry[attr_off + 51],
                    entry[attr_off + 52],
                    entry[attr_off + 53],
                    entry[attr_off + 54],
                    entry[attr_off + 55],
                ]);

                let data_runs =
                    Self::parse_ntfs_data_runs(&entry[attr_off + runs_offset..])?;

                // Read bitmap data from disk following the data runs
                let mut bitmap_bytes = Vec::with_capacity(real_size as usize);
                for &(cluster_offset, cluster_count) in &data_runs {
                    let sector = info.partition_start
                        + cluster_offset * info.sectors_per_cluster;
                    let total_secs = cluster_count * info.sectors_per_cluster;
                    let mut read_secs = 0;
                    while read_secs < total_secs {
                        let chunk =
                            (total_secs - read_secs).min(BUFFER_MAX_WRITE_SIZE / SECTOR_SIZE);
                        let data = self.mass_storage.read_sectors(
                            sector + read_secs,
                            chunk,
                            SECTOR_SIZE as usize,
                        )?;
                        bitmap_bytes.extend_from_slice(&data);
                        read_secs += chunk;
                    }
                }
                bitmap_bytes.truncate(real_size as usize);

                // Convert bitmap bytes to bool vec (1 bit per cluster)
                let mut used = vec![false; info.total_clusters as usize];
                for (i, cluster_used) in used.iter_mut().enumerate() {
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    if byte_idx < bitmap_bytes.len() {
                        *cluster_used = (bitmap_bytes[byte_idx] >> bit_idx) & 1 == 1;
                    }
                }

                return Ok(used);
            }

            if attr_size == 0 {
                return Err(Error::Error("$Bitmap: zero-size attribute".into()));
            }
            attr_off += attr_size;
        }
    }

    /// NTFS-aware read: only read pre-partition area + used clusters
    fn read_ntfs_sparse(&mut self, comm: &mut ComRpFs2Dev, info: &NtfsInfo) -> Result<()> {
        let used_clusters = self.read_ntfs_bitmap(info)?;
        let used_count: u64 = used_clusters.iter().filter(|&&u| u).count() as u64;
        let pre_partition_bytes = info.partition_start * SECTOR_SIZE;
        let total_to_read =
            pre_partition_bytes + used_count * info.sectors_per_cluster * SECTOR_SIZE;

        debug!(
            "NTFS sparse read: {}/{} clusters used, reading ~{}MB instead of ~{}MB",
            used_count,
            info.total_clusters,
            total_to_read / (1024 * 1024),
            self.dev_size / (1024 * 1024)
        );

        let mut current_read = 0u64;
        let mut status_counter = 0u64;

        // 1) Read everything before the partition (MBR + gap)
        let mut sector = 0u64;
        while sector < info.partition_start {
            let count =
                (info.partition_start - sector).min(BUFFER_MAX_WRITE_SIZE / SECTOR_SIZE);
            let data = self
                .mass_storage
                .read_sectors(sector, count, SECTOR_SIZE as usize)?;
            self.fs.write_all(&data)?;
            sector += count;
            current_read += count * SECTOR_SIZE;
            status_counter += 1;
            if status_counter.is_multiple_of(10) {
                comm.status(current_read, total_to_read, false, Status::ReadSrc)?;
            }
        }

        // 2) Read only used clusters in the NTFS partition
        for cluster_idx in 0..info.total_clusters as usize {
            let cluster_bytes = info.sectors_per_cluster * SECTOR_SIZE;

            if cluster_idx < used_clusters.len() && used_clusters[cluster_idx] {
                let cluster_sector =
                    info.partition_start + cluster_idx as u64 * info.sectors_per_cluster;
                let data = self.mass_storage.read_sectors(
                    cluster_sector,
                    info.sectors_per_cluster,
                    SECTOR_SIZE as usize,
                )?;
                self.fs.write_all(&data)?;
                current_read += cluster_bytes;
            } else {
                self.fs.seek(SeekFrom::Current(cluster_bytes as i64))?;
            }

            status_counter += 1;
            if status_counter.is_multiple_of(100) {
                comm.status(current_read, total_to_read, false, Status::ReadSrc)?;
            }
        }

        Ok(())
    }

    /// Fallback: read entire device (for unrecognized filesystems)
    fn read_full(&mut self, comm: &mut ComRpFs2Dev) -> Result<()> {
        let total_size = self.dev_size;
        let mut current_size = 0u64;
        let mut sector_index = 0u64;
        let mut status_counter = 0u64;

        while current_size < total_size {
            let remaining = total_size - current_size;
            let read_size = if remaining > BUFFER_MAX_WRITE_SIZE {
                BUFFER_MAX_WRITE_SIZE
            } else {
                remaining
            };
            let sector_count = read_size / SECTOR_SIZE;

            let data = self.mass_storage.read_sectors(
                sector_index,
                sector_count,
                SECTOR_SIZE as usize,
            )?;

            if data.iter().all(|&b| b == 0) {
                self.fs.seek(SeekFrom::Current(read_size as i64))?;
            } else {
                self.fs.write_all(&data)?;
            }

            current_size += read_size;
            sector_index += sector_count;

            status_counter += 1;
            if status_counter.is_multiple_of(10) {
                comm.status(current_size, total_size, false, Status::ReadSrc)?;
            }
        }

        Ok(())
    }
}

impl DevOpenedState {
    fn run(self, comm: &mut ComRpFs2Dev) -> Result<State> {
        trace!("dev opened state");
        Ok(match comm.recv_req()? {
            Msg::DevSize(_) => {
                comm.devsize(fs2dev::ResponseDevSize {
                    size: self.mass_storage.dev_size,
                })?;
                State::DevOpened(self)
            }
            Msg::LoadBitVec(ref mut msg) => self.load_bitvec(comm, &mut msg.chunk, msg.last)?,
            Msg::Wipe(_) => State::Wiping(WipingState {
                fs: self.fs,
                mass_storage: self.mass_storage,
            }),
            Msg::ReadFs(msg) => State::Reading(ReadingState {
                fs: self.fs,
                mass_storage: self.mass_storage,
                dev_size: msg.dev_size,
            }),
            Msg::End(_) => {
                comm.end()?;
                State::End
            }
            _ => {
                error!("bad request");
                comm.error("bad request")?;
                return Err(Error::State);
            }
        })
    }

    fn load_bitvec(self, comm: &mut ComRpFs2Dev, chunk: &mut Vec<u8>, last: bool) -> Result<State> {
        let mut fs_bv_buf = Vec::new();
        fs_bv_buf.append(chunk);
        comm.loadbitvec(fs2dev::ResponseLoadBitVec {})?;
        if !last {
            loop {
                match comm.recv_req()? {
                    Msg::LoadBitVec(ref mut msg) => {
                        fs_bv_buf.append(&mut msg.chunk);
                        comm.loadbitvec(fs2dev::ResponseLoadBitVec {})?;
                        if msg.last {
                            break;
                        }
                    }
                    _ => {
                        error!("bad request");
                        comm.error("bad request")?;
                        return Err(Error::State);
                    }
                }
            }
        }
        let fs_bv = BitVecIterOnes::new(BitVec::from_vec(fs_bv_buf));
        Ok(State::BitVecLoaded(BitVecLoadedState {
            fs: self.fs,
            fs_bv,
            mass_storage: self.mass_storage,
        }))
    }
}

impl BitVecLoadedState {
    fn run(self, comm: &mut ComRpFs2Dev) -> Result<State> {
        trace!("bitvec loaded state");
        Ok(match comm.recv_req()? {
            Msg::WriteFs(_) => State::Copying(CopyingState {
                fs: self.fs,
                fs_bv: self.fs_bv,
                mass_storage: self.mass_storage,
            }),
            Msg::End(_) => {
                comm.end()?;
                State::End
            }
            _ => {
                error!("bad request");
                comm.error("bad request")?;
                return Err(Error::State);
            }
        })
    }
}

impl WaitEndState {
    fn run(self, comm: &mut ComRpFs2Dev) -> Result<State> {
        match comm.recv_req()? {
            Msg::End(_) => {
                comm.end()?;
            }
            _ => {
                error!("bad request");
                comm.error("bad request")?;
            }
        }
        Ok(State::End)
    }
}

pub struct Fs2Dev {
    comm: ComRpFs2Dev,
    state: State,
}

impl Fs2Dev {
    pub fn new(comm: ComRpFs2Dev, fs_fname: String) -> Result<Self> {
        let state = State::Init(InitState { fs_fname });
        Ok(Fs2Dev { comm, state })
    }

    pub fn main_loop(self) -> Result<()> {
        let (mut comm, mut state) = (self.comm, self.state);
        loop {
            state = match state.run(&mut comm) {
                Ok(State::End) => break,
                Ok(state) => state,
                Err(err) => {
                    error!("state run error: {err}, waiting end");
                    comm.error(format!("{err}"))?;
                    State::WaitEnd(WaitEndState {})
                }
            };
        }
        Ok(())
    }
}
