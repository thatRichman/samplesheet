use super::{OverrideCycles, SampleSheetVersion};
use core::str::FromStr;
use serde::{Deserialize, Deserializer};

pub(crate) const fn default_bool<const V: bool>() -> bool {
    V
}

pub(crate) fn bool_from_int<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(deserializer)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(serde::de::Error::invalid_value(
            serde::de::Unexpected::Unsigned(other as u64),
            &"zero or one",
        )),
    }
}

pub(crate) fn samplesheet_version_from_int<'de, D>(
    deserializer: D,
) -> Result<SampleSheetVersion, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(deserializer)? {
        1 => Ok(SampleSheetVersion::V1),
        2 => Ok(SampleSheetVersion::V2),
        other => Err(serde::de::Error::invalid_value(
            serde::de::Unexpected::Unsigned(other as u64),
            &"unknown samplesheet version",
        )),
    }
}

pub(crate) fn vec_plus_sign_sep<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    match String::deserialize(deserializer)? {
        s if s.contains('+') => Ok(s.split('+').map(|s| s.to_string()).collect::<Vec<String>>()),
        _ => Err(serde::de::Error::custom(
            "expected plus-sign(+) delimited list",
        )),
    }
}

impl<'de> Deserialize<'de> for OverrideCycles {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        FromStr::from_str(&s).map_err(serde::de::Error::custom)
    }
}

// stupid clunky workaround to circumvent the fact that
// custom deserializers with Options aren't natively supported
// by serde
// https://users.rust-lang.org/t/solved-serde-deserialize-with-for-option-s/12749
#[derive(Debug, Deserialize)]
pub(crate) struct OptionalOverrideCycles(
    #[serde[deserialize_with = "OverrideCycles::deserialize"]] OverrideCycles,
);

pub(crate) fn callback_opt<'de, D>(deserializer: D) -> Result<Option<OverrideCycles>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<OptionalOverrideCycles>::deserialize(deserializer).map(
        |opt_wrapped: Option<OptionalOverrideCycles>| {
            opt_wrapped.map(|wrapped: OptionalOverrideCycles| wrapped.0)
        },
    )
}
