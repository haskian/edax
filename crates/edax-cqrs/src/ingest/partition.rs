//! Opt-in UUIDv7 placement and bounded replay admission.

use super::{Contract, Input, Plan, Rejection, Stored};
use edax_core::Error;
use std::borrow::Cow;
use std::collections::BTreeMap;

/// Largest admission floor: one microsecond beyond the last UUIDv7 millisecond.
pub const MAX_HORIZON: i64 = 281_474_976_710_655_001;

/// Validate the host's explicit retention boundary. Never derive it from a clock.
pub fn horizon(value: i64) -> Result<i64, Error> {
    if !(0..=MAX_HORIZON).contains(&value) {
        return Err(Error::bad_handle("ingestion horizon is outside the UUIDv7 time range"));
    }
    Ok(value)
}

/// Refusals to attach before acquiring providers. Existing preparation failures win.
pub fn failures(contract: &Contract, inputs: &[Input]) -> Result<Vec<Option<String>>, Error> {
    super::validate_inputs(contract, inputs)?;
    Ok(inputs
        .iter()
        .map(|input| {
            input.failure.clone().or_else(|| {
                contract
                    .partition
                    .as_ref()
                    .and_then(|_| edax_core::uuid7_micros(&input.event_id).err().map(|e| e.to_string()))
            })
        })
        .collect())
}

/// Exact coordinates for valid append keys. Invalid event IDs have no physical row.
pub fn coordinates(contract: &Contract, keys: &[String]) -> Result<BTreeMap<String, String>, Error> {
    super::bind_contract(None, contract)?;
    if contract.partition.is_none() {
        return Err(Error::bad_handle("ingestion target is not partitioned"));
    }
    let mut result = BTreeMap::new();
    for key in keys {
        let parts = super::identity(contract, key)?;
        if parts.len() != 4 || parts.first().and_then(serde_json::Value::as_str) != Some("append") {
            return Err(Error::bad_handle("invalid partitioned ingestion row key"));
        }
        if let Some(at) = parts
            .get(3)
            .and_then(serde_json::Value::as_str)
            .and_then(|id| edax_core::uuid7_micros(id).ok())
        {
            result.insert(key.clone(), at.to_string());
        }
    }
    Ok(result)
}

/// Plan using a floor read under the physical table's transaction gate.
pub fn plan(
    contract: &Contract,
    inputs: &[Input],
    stored: &BTreeMap<String, Stored>,
    rejected: &BTreeMap<String, Rejection>,
    floor: i64,
) -> Result<Plan, Error> {
    let prepared = prepare(contract, inputs)?;
    super::plan_at(
        &contract.mode,
        &prepared,
        stored,
        rejected,
        contract.partition.as_ref().map(|_| horizon(floor)).transpose()?,
    )
}

/// Validate originals and attach partition failures without copying already-prepared payloads.
pub fn prepare<'a>(contract: &Contract, inputs: &'a [Input]) -> Result<Cow<'a, [Input]>, Error> {
    let failures = failures(contract, inputs)?;
    if inputs
        .iter()
        .zip(&failures)
        .all(|(input, failure)| &input.failure == failure)
    {
        return Ok(Cow::Borrowed(inputs));
    }
    Ok(Cow::Owned(
        inputs
            .iter()
            .zip(failures)
            .map(|(input, failure)| Input {
                failure,
                ..input.clone()
            })
            .collect(),
    ))
}
