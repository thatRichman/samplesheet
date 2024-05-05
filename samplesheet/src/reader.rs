use crate::SampleSheetVersion;

use super::{
    into_reader, parser, try_deserialize, OtherData, SampleSheet, SampleSheetData,
    SampleSheetError, SampleSheetHeader, SampleSheetReads, SampleSheetSection, SampleSheetSettings,
    SectionType,
};
use std::{fs::File, io::Read, path::Path};

/// Read an Illumina samplesheet
///
/// Uses [SampleSheetBuilder] under the hood. [Header](super::SampleSheetHeader), [Reads](super::SampleSheetReads), [Settings](super::SampleSheetSettings), and
/// [Data](super::SampleSheetData) are required sections. They may appear in any order.
///
/// Other sections are stored in the `other` field, as a map ([OtherSections](super::OtherSections)). The key is the section name, and the
/// value is of type [OtherData], which provides convenience methods for deserializing into the
/// appropraite format.
pub fn read_samplesheet<P: AsRef<Path>>(path: P) -> Result<SampleSheet, SampleSheetError> {
    let mut handle = File::open(path)?;
    let mut buf = String::new();
    handle.read_to_string(&mut buf)?;
    let mut slice = &buf[..];

    let mut builder = super::SampleSheetBuilder::new();

    while !slice.is_empty() {
        match parser::parse_section(slice) {
            Ok((i, (section, mut raw_contents))) => {
                // Standalone sections cannot be natively parsed by csv::Reader
                // To parse them, we first transmute them from long to wide
                // such that the first column becomes the header and the second a row of values
                match section.get_kind() {
                    SectionType::KV => {
                        parser::transpose_kv(&mut raw_contents, ',');
                    }
                    _ => {}
                }
                // Now we can read all of the data as CSV
                let mut csv_reader = into_reader(&raw_contents);
                match section {
                    SampleSheetSection::Header(..) => {
                        builder.set_header(try_deserialize::<SampleSheetHeader>(&mut csv_reader)?);
                        if builder.header.as_ref().unwrap().file_format_version
                            == SampleSheetVersion::V1
                        {
                            return Err(SampleSheetError::ParseError(
                                "V1 samplesheets are not currently supported".to_owned(),
                            ));
                        }
                    }
                    SampleSheetSection::Reads(..) => {
                        builder.set_reads(try_deserialize::<SampleSheetReads>(&mut csv_reader)?);
                    }
                    SampleSheetSection::Settings(..)
                    | SampleSheetSection::BCLConvertSettings(..) => {
                        builder
                            .set_settings(try_deserialize::<SampleSheetSettings>(&mut csv_reader)?);
                    }
                    SampleSheetSection::Data(..) | SampleSheetSection::BCLConvertData(..) => {
                        for row in csv_reader.deserialize::<SampleSheetData>() {
                            match row {
                                Ok(d) => builder.append_data(d),
                                Err(e) => return Err(SampleSheetError::from(e)),
                            }
                        }
                    }
                    SampleSheetSection::Other(section) => {
                        if let SectionType::Unknown(name) = section {
                            builder.append_other(name, OtherData::from(raw_contents))?
                        }
                    }
                }
                slice = i;
            }
            Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => return Err(e),
            Err(nom::Err::Incomplete(..)) => return Err(SampleSheetError::EofError),
        }
    }
    builder.build()
}
