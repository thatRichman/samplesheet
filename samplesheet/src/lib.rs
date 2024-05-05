#![allow(dead_code)]

use fxhash::FxHashMap;
pub use parser::transpose_kv;
use serde::{de::DeserializeOwned, Deserialize};
use std::{convert::Infallible, fmt::Display, num::ParseIntError, str::FromStr};
use thiserror::Error;

pub(crate) mod deserializers;
pub(crate) mod parser;
pub mod reader;

use self::deserializers::*;

pub const DEFAULT_ADAPTER_STRINGENCY: f32 = 0.9;
pub const DEFAULT_MASK_SHORT_READS: u16 = 22;

const fn default_adapter_stringency() -> f32 {
    DEFAULT_ADAPTER_STRINGENCY
}

const fn default_mask_short_reads() -> u16 {
    DEFAULT_MASK_SHORT_READS
}

const fn default_version() -> SampleSheetVersion {
    SampleSheetVersion::V1
}

#[derive(Error, Debug)]
pub enum SampleSheetError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    DeserializeError(#[from] csv::Error),
    #[error("Unexpected end of file")]
    EofError,
    #[error("Error reading SampleSheet: {0}")]
    ParseError(String),
    #[error("Error reading OverrideCycles: unknown cycle type {0}")]
    UnknownCycleKind(char),
    #[error("Error reading OverrideCycles: could not parse cycle count as u8")]
    ParseIntError(#[from] ParseIntError),
    #[error("Samplesheet section format incorrect: exepected {0} got {1}")]
    BadSectionFormat(&'static str, &'static str),
    #[error("Samplesheet missing required section: {0}")]
    MissingSection(&'static str),
    #[error("attempt to insert duplicate section data for key: {0}")]
    DuplicateKeyError(String),
}

/// fastq compression format
#[derive(Debug, Deserialize, Default)]
pub enum CompressionFormat {
    #[default]
    #[serde(rename = "gzip")]
    Gzip,
    #[serde(rename = "dragen")]
    Dragen,
    #[serde(rename = "dragen-interleaved")]
    DragenInterleaved,
}

/// How adapaters are treated
///
/// Mask => masks adapters with Ns
/// Trim => removes adapters
///
/// The default of Trim matches the Illumina default.
#[derive(Deserialize, Default, Debug, PartialEq)]
pub enum AdapterBehavior {
    #[serde(rename = "mask")]
    Mask,
    #[default]
    #[serde(rename = "trim")]
    Trim,
}

/// A single component of the OverrideCycles field
///
/// Each variant encodes a type and its length.
///
/// I => Index reads
///
/// Y => Sequencing reads
///
/// U => UMI reads,
///
/// N => trimmed reads
///
/// Only one Y or I sequence is allowed per read
#[derive(Debug, PartialEq, Eq)]
pub enum OverrideCycle {
    I(u8),
    Y(u8),
    U(u8),
    N(u8),
}

impl FromStr for OverrideCycle {
    type Err = SampleSheetError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match parser::override_cycle(s) {
            Ok((_, cyc)) => Ok(cyc),
            Err(e) => Err(SampleSheetError::ParseError(format!(
                "failed to parse {s} as OverrideCycle: {e}"
            ))),
        }
    }
}

impl TryFrom<(char, u8)> for OverrideCycle {
    type Error = SampleSheetError;
    fn try_from(value: (char, u8)) -> Result<Self, Self::Error> {
        match value.0 {
            'I' => Ok(OverrideCycle::I(value.1)),
            'Y' => Ok(OverrideCycle::Y(value.1)),
            'U' => Ok(OverrideCycle::U(value.1)),
            'N' => Ok(OverrideCycle::N(value.1)),
            otherwise => Err(SampleSheetError::UnknownCycleKind(otherwise)),
        }
    }
}

impl Display for OverrideCycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OverrideCycle::I(val) => write!(f, "I{val}"),
            OverrideCycle::Y(val) => write!(f, "Y{val}"),
            OverrideCycle::U(val) => write!(f, "U{val}"),
            OverrideCycle::N(val) => write!(f, "N{val}"),
        }
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct OverrideCycles(Vec<Vec<OverrideCycle>>);

impl FromStr for OverrideCycles {
    type Err = SampleSheetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match parser::override_cycles(s) {
            Ok((_, cycles)) => {
                validate_cycles(&cycles)?;
                Ok(OverrideCycles(cycles))
            }
            Err(e) => Err(SampleSheetError::ParseError(format!(
                "unable to parse {s}: {e}"
            ))),
        }
    }
}

impl Display for OverrideCycles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.0
                .iter()
                .map(|m| m.iter().map(|c| c.to_string()).collect::<String>())
                .collect::<Vec<String>>()
                .join(";")
        )
    }
}

// TODO find an elegant way to "parse not validate" this
/// each read can contain only one Y or I sequence
fn validate_cycles(cycles: &[Vec<OverrideCycle>]) -> Result<(), SampleSheetError> {
    match cycles
        .iter()
        .map(|s| {
            s.iter()
                .filter(|c| matches!(**c, OverrideCycle::I(..) | OverrideCycle::Y(..)))
                .count()
                .eq(&1)
        })
        .all(|r| r)
    {
        true => Ok(()),
        false => Err(SampleSheetError::ParseError(String::from(
            "each read can contain only one Y or I sequence",
        ))),
    }
}

/// The format of a samplesheet section
///
/// A KV section is comprised of comma-separated key-value pairs with no header.
///
/// A CSV section is comma-separated values with a header.
///
/// Unknown represents a section that cannot be parsed.
#[derive(Debug, PartialEq, Eq)]
pub enum SectionType {
    KV,
    CSV,
    Unknown(String),
}

impl Display for SectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KV => write!(f, "Standalone"),
            Self::CSV => write!(f, "CSV"),
            Self::Unknown(s) => write!(f, "{}", s),
        }
    }
}

/// The Version field of the samplesheet
///
/// Note: when deserializing a samplesheet, V1 is used as a default, under the assumption that if
/// the FileFormatVersion field is not present, it is probably an older samplesheet.
/// Counterintuitively, we also don't currently support V1 samplesheets.
#[repr(u8)]
#[derive(Deserialize, Default, Debug, PartialEq)]
pub enum SampleSheetVersion {
    V1 = 1,
    #[default]
    V2 = 2,
}

/// A single section of the samplesheet
///
/// Common sections have pre-defined variants. Other is used as a catch-all.
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq)]
pub enum SampleSheetSection {
    Header(SectionType),
    Reads(SectionType),
    Settings(SectionType),
    Data(SectionType),
    BCLConvertSettings(SectionType),
    BCLConvertData(SectionType),
    Other(SectionType),
}

impl SampleSheetSection {
    fn get_kind(&self) -> &SectionType {
        match self {
            Self::Header(kind) => kind,
            Self::Reads(kind) => kind,
            Self::Settings(kind) => kind,
            Self::Data(kind) => kind,
            Self::BCLConvertSettings(kind) => kind,
            Self::BCLConvertData(kind) => kind,
            Self::Other(kind) => kind,
        }
    }
}

impl FromStr for SampleSheetSection {
    type Err = Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Header" => Ok(Self::Header(SectionType::KV)),
            "Settings" => Ok(Self::Settings(SectionType::KV)),
            "Data" => Ok(Self::Data(SectionType::CSV)),
            "Reads" => Ok(Self::Reads(SectionType::KV)),
            "BCLConvert_Settings" => Ok(Self::BCLConvertSettings(SectionType::KV)),
            "BCLConvert_Data" => Ok(Self::BCLConvertData(SectionType::CSV)),
            s => Ok(Self::Other(SectionType::Unknown(s.to_string()))),
        }
    }
}

impl Display for SampleSheetSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Header(..) => write!(f, "[Header]"),
            Self::Settings(..) => write!(f, "[Settings]"),
            Self::Data(..) => write!(f, "[Data]"),
            Self::Reads(..) => write!(f, "[Reads]"),
            Self::BCLConvertData(..) => write!(f, "[BCLConvert_Data]"),
            Self::BCLConvertSettings(..) => write!(f, "[BCLConvert_Settings]"),
            Self::Other(name) => write!(f, "[{name}]"),
        }
    }
}

/// The samplesheet header section
#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "PascalCase")]
pub struct SampleSheetHeader {
    #[serde(
        deserialize_with = "samplesheet_version_from_int",
        default = "default_version"
    )]
    pub file_format_version: SampleSheetVersion,
    pub run_name: Option<String>,
    pub instrument_platform: Option<String>,
    pub instrument_type: Option<String>,
}

/// The samplesheet reads section
#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "PascalCase")]
pub struct SampleSheetReads {
    pub read_1_cycles: u16,
    pub read_2_cycles: u16,
    pub index_1_cycles: Option<u16>,
    pub index_2_cycles: Option<u16>,
}

/// Valid MinAdapterOlap values
///
/// The default of 1 matches the Illumina default.
#[repr(u8)]
#[derive(Debug, Deserialize, Default)]
pub enum MinAdapterOlap {
    #[default]
    One = 1,
    Two = 2,
    Three = 3,
}

/// The samplesheet Settings section
///
/// Generally, defaults mirror Illumina defaults, but this should not be blindly trusted as fact.
#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "PascalCase")]
pub struct SampleSheetSettings {
    pub software_version: Option<String>,
    #[serde(deserialize_with = "callback_opt")]
    pub override_cycles: Option<OverrideCycles>,
    #[serde(deserialize_with = "vec_plus_sign_sep", default = "Vec::new")]
    pub adapter_read_1: Vec<String>,
    #[serde(deserialize_with = "vec_plus_sign_sep", default = "Vec::new")]
    pub adapter_read_2: Vec<String>,
    #[serde(default = "AdapterBehavior::default")]
    pub adapter_behavior: AdapterBehavior,
    #[serde(default = "default_adapter_stringency")]
    pub adapter_stringency: f32,
    #[serde(default = "MinAdapterOlap::default")]
    pub minimum_adapter_overlap: MinAdapterOlap,
    #[serde(default = "default_bool::<false>", deserialize_with = "bool_from_int")]
    pub create_fastq_for_index_reads: bool,
    #[serde(default = "default_bool::<true>", deserialize_with = "bool_from_int")]
    pub trim_umi: bool,
    #[serde(default = "default_mask_short_reads")]
    pub mask_short_reads: u16,
    #[serde(default = "default_bool::<false>")]
    pub no_lane_splitting: bool,
    #[serde(default = "CompressionFormat::default")]
    pub fastq_compression_format: CompressionFormat,
}

/// The Data section of a samplesheet
#[derive(Deserialize, Debug, Default)]
pub struct SampleSheetData {
    #[serde(rename = "Lane")]
    pub lane: u8,
    #[serde(rename = "Sample_ID")]
    pub sample_id: String,
    #[serde(rename = "index")]
    pub index: String,
    #[serde(rename = "index2")]
    pub index_2: Option<String>,
}

/// A complete samplesheet
#[derive(Deserialize, Debug, Default)]
pub struct SampleSheet {
    pub header: SampleSheetHeader,
    pub reads: SampleSheetReads,
    pub settings: SampleSheetSettings,
    pub data: Vec<SampleSheetData>,
    pub other: Option<OtherSections>,
}

impl SampleSheet {
    pub fn builder() -> SampleSheetBuilder {
        SampleSheetBuilder::default()
    }
}

#[derive(Default)]
/// Build a [SampleSheet] from its constituent sections
pub struct SampleSheetBuilder {
    header: Option<SampleSheetHeader>,
    reads: Option<SampleSheetReads>,
    settings: Option<SampleSheetSettings>,
    data: Option<Vec<SampleSheetData>>,
    other: Option<OtherSections>,
}

impl SampleSheetBuilder {
    pub fn new() -> SampleSheetBuilder {
        Self::default()
    }

    /// Set header section
    pub fn set_header(&mut self, h: SampleSheetHeader) {
        self.header = Some(h);
    }

    /// Set reads section
    pub fn set_reads(&mut self, r: SampleSheetReads) {
        self.reads = Some(r);
    }

    /// Set setting section
    pub fn set_settings(&mut self, s: SampleSheetSettings) {
        self.settings = Some(s);
    }

    /// Set data section
    pub fn set_data(&mut self, d: Vec<SampleSheetData>) {
        self.data = Some(d)
    }

    /// Append a single piece of data
    pub fn append_data(&mut self, d: SampleSheetData) {
        match &mut self.data {
            None => {
                let mut data = Vec::new();
                data.push(d);
                self.data = Some(data);
            }
            Some(data) => {
                data.push(d);
            }
        }
    }

    /// Set other sections
    pub fn set_other(&mut self, other: OtherSections) {
        self.other = Some(other);
    }

    /// Insert an entry into OtherSections
    pub fn append_other(&mut self, k: String, o: OtherData) -> Result<(), SampleSheetError> {
        match self.other.as_mut() {
            None => {
                let mut other = OtherSections::new();
                other.0.insert(k, o);
                self.other = Some(other);
            }
            Some(sections) => {
                if sections.0.contains_key(&k) {
                    return Err(SampleSheetError::DuplicateKeyError(k.to_owned()));
                }
                sections.0.insert(k.to_owned(), o);
            }
        }
        Ok(())
    }

    /// Build the complete samplesheet
    pub fn build(self) -> Result<SampleSheet, SampleSheetError> {
        let header = self
            .header
            .ok_or_else(|| SampleSheetError::MissingSection("Header"))?;
        let reads = self
            .reads
            .ok_or_else(|| SampleSheetError::MissingSection("Reads"))?;
        let settings = self
            .settings
            .ok_or_else(|| SampleSheetError::MissingSection("Settings"))?;
        let data = self
            .data
            .ok_or_else(|| SampleSheetError::MissingSection("Data"))?;
        Ok(SampleSheet {
            header,
            reads,
            settings,
            data,
            other: self.other,
        })
    }
}

#[derive(Deserialize, Debug, Default)]
/// Sections that cannot be automatically deserialized
///
/// Newtype over [FxHashMap] with section name as the key and [OtherData] as the value.
pub struct OtherSections(pub FxHashMap<String, OtherData>);

impl OtherSections {
    fn new() -> Self {
        OtherSections(FxHashMap::default())
    }
}

/// Data that cannot be automatically deserialized
///
/// Newtype over [String]
#[derive(Deserialize, Debug, Default)]
pub struct OtherData(String);

impl OtherData {
    /// Create OtherData from any type that implements [`Into<String>`]
    pub fn from<T: Into<String>>(value: T) -> Self {
        OtherData(value.into())
    }
    /// Transposes self via [transpose](parser::transpose_kv()) and returns a [Reader](csv::Reader)
    /// over the tranposed data (as bytes)
    pub fn as_kv(&mut self) -> csv::Reader<&[u8]> {
        transpose_kv(&mut self.0, ',');
        into_reader(&self.0)
    }

    /// Returns a [Reader](csv::Reader) over self (as bytes)
    pub fn as_csv(&self) -> csv::Reader<&[u8]> {
        into_reader(&self.0)
    }

    /// Try to deserialize into T assuming data is key-value pairs
    pub fn de_as_kv<T: DeserializeOwned>(&mut self) -> Result<T, SampleSheetError> {
        try_deserialize::<T>(&mut self.as_kv())
    }

    /// Try to deserialize into T assuming data is CSV format
    pub fn de_as_csv<T: DeserializeOwned>(&mut self) -> Result<T, SampleSheetError> {
        try_deserialize::<T>(&mut self.as_csv())
    }
}

pub(crate) fn into_reader(input: &str) -> csv::Reader<&[u8]> {
    csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(input.as_bytes())
}

pub(crate) fn try_deserialize<T: DeserializeOwned>(
    reader: &mut csv::Reader<&[u8]>,
) -> Result<T, SampleSheetError> {
    match reader.deserialize::<T>().next() {
        Some(v) => match v {
            Ok(h) => Ok(h),
            Err(e) => Err(SampleSheetError::from(e)),
        },
        None => {
            dbg!("try_deserialize next call produced None");
            Err(SampleSheetError::EofError)
        }
    }
}

#[cfg(test)]
mod tests {}
