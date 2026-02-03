use autark_tensor::Tensor;

use crate::{Result, dataadapter::DataFramePayload};

#[derive(Debug, Clone)]
pub struct ProgramPayload {
    pub(crate) dataframes: Vec<DataFramePayload>,
}

impl ProgramPayload {
    pub fn new(dataframes: Vec<DataFramePayload>) -> ProgramPayload {
        ProgramPayload { dataframes }
    }

    pub(crate) fn variant_fuse(&mut self) -> Result<()> {
        let max_cols = self
            .dataframes
            .iter()
            .map(|payload| payload.variant_map.len())
            .max()
            .unwrap_or(0);

        let mut running_offsets = vec![0i64; max_cols];
        let mut global_map: Vec<Vec<String>> = vec![Vec::new(); max_cols];

        for payload in self.dataframes.iter_mut() {
            let mut offsets = vec![0i64; payload.variant_map.len()];
            for (ix, map) in payload.variant_map.iter().enumerate() {
                if map.is_empty() {
                    continue;
                }
                offsets[ix] = running_offsets[ix];
                running_offsets[ix] += map.len() as i64;
                global_map[ix].extend(map.iter().cloned());
            }

            if offsets.iter().any(|&v| v != 0) {
                let offset_tensor = Tensor::from_slice(&offsets)?;
                payload.data = &payload.data + &offset_tensor;
            }
        }

        for payload in self.dataframes.iter_mut() {
            payload.variant_map = global_map.clone();
        }

        Ok(())
    }
}
