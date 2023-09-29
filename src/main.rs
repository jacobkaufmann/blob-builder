use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    str::FromStr,
    sync::{Arc, RwLock},
};

use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    routing::get,
    Router,
};
use beacon_api_client::{mainnet::Client as BeaconApiClient, BlockId};
use c_kzg::{
    two_dim_samples, Blob, Error as KzgError, KzgSettings, Sample, SampleMatrix,
    BYTES_PER_FIELD_ELEMENT,
};
use clap::Parser;
use ethereum_consensus::{
    clock::{self, Clock, SystemTimeProvider},
    configs::mainnet::SECONDS_PER_SLOT,
    deneb::presets::mainnet::BYTES_PER_BLOB,
    phase0::mainnet::SLOTS_PER_EPOCH,
};
use ethereum_consensus::{
    deneb::{mainnet::FIELD_ELEMENTS_PER_BLOB, BlobSidecar},
    primitives::Slot,
    ssz::prelude::*,
};
use tokio_stream::StreamExt;
use tracing_subscriber::FmtSubscriber;
use url::Url;

// TODO: refactor c-kzg-4844 to expose these values as constants
const DANKSHARDING_MAX_BLOBS_PER_BLOCK: usize = 256;
const FIELD_ELEMENTS_PER_SAMPLE: usize = 16;

/// the number of epochs for which blob info is kept available
const BLOB_HISTORY_EPOCHS: u64 = 1;

#[derive(Clone, Debug, Default, serde::Serialize, SimpleSerialize)]
struct BlobSample(Vector<ByteVector<BYTES_PER_FIELD_ELEMENT>, FIELD_ELEMENTS_PER_SAMPLE>);

impl BlobSample {
    pub fn new(sample: Sample) -> Self {
        let elements: Vec<_> = sample
            .as_slice()
            .into_iter()
            .map(|point| ByteVector::try_from(point.into_inner().as_slice()).unwrap())
            .collect();
        let elements = Vector::try_from(elements).unwrap();
        Self(elements)
    }
}

#[derive(Debug)]
struct BlobContainer {
    kzg: Arc<KzgSettings>,
    blobs: HashMap<Slot, Vec<BlobSidecar<BYTES_PER_BLOB>>>,
    samples: HashMap<Slot, HashMap<(u64, u64), BlobSample>>,
}

impl BlobContainer {
    pub fn new(kzg: Arc<KzgSettings>) -> Self {
        Self {
            kzg,
            blobs: Default::default(),
            samples: Default::default(),
        }
    }

    pub fn ingest(
        &mut self,
        slot: Slot,
        blobs: Vec<BlobSidecar<BYTES_PER_BLOB>>,
    ) -> Result<(), KzgError> {
        self.blobs.insert(slot, blobs.clone());

        let mut blobs = blobs
            .into_iter()
            .map(|sidecar| compat::blob_from_sidecar(sidecar))
            .collect::<Result<Vec<Blob>, KzgError>>()?;

        // TODO: construct empty blobs more efficiently OR move this logic to c-kzg-4844
        while blobs.len() < DANKSHARDING_MAX_BLOBS_PER_BLOCK {
            let mut blob = Vec::with_capacity(BYTES_PER_BLOB);
            for _ in 0..BYTES_PER_BLOB {
                blob.push(0);
            }
            blobs.push(Blob::from_bytes(blob.as_slice()).expect("can construct empty blob"));
        }

        // TODO: verify blob commitments

        // decompose blobs into samples
        //
        // TODO: add note about matrix dimensions
        const ROWS: usize = DANKSHARDING_MAX_BLOBS_PER_BLOCK * 2;
        const COLS: usize = FIELD_ELEMENTS_PER_BLOB / FIELD_ELEMENTS_PER_SAMPLE * 2;
        let samples: SampleMatrix<ROWS, COLS> = match two_dim_samples(blobs, &self.kzg) {
            Ok(samples) => samples,
            Err(err) => return Err(err),
        };

        let mut entries = HashMap::new();
        for i in 0..ROWS {
            for j in 0..COLS {
                entries.insert(
                    (i as u64, j as u64),
                    BlobSample::new(samples.entry(i, j).unwrap().clone()),
                );
            }
        }
        self.samples.insert(slot, entries);

        Ok(())
    }

    pub fn remove(&mut self, slot: Slot) {
        self.blobs.remove(&slot);
        self.samples.remove(&slot);
    }

    pub fn sample(&self, slot: Slot, row: u64, col: u64) -> Option<&BlobSample> {
        self.samples
            .get(&slot)
            .and_then(|samples| samples.get(&(row, col)))
    }
}

struct AppState {
    #[allow(dead_code)]
    kzg: Arc<KzgSettings>,
    clock: Arc<Clock<SystemTimeProvider>>,
    blobs: Arc<RwLock<BlobContainer>>,
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    beacon_endpoint: Url,
    #[arg(short, long)]
    port: u16,
    #[arg(short, long)]
    trusted_setup_file: PathBuf,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let subscriber = FmtSubscriber::default();
    tracing::subscriber::set_global_default(subscriber)
        .expect("can set default tracing subscriber");

    // load KZG settings from trusted setup file
    tracing::info!(
        "loading KZG settings from {}...",
        args.trusted_setup_file.display()
    );
    let kzg = KzgSettings::load_trusted_setup_file(args.trusted_setup_file)
        .expect("can load KZG settings from file");
    let kzg = Arc::new(kzg);
    tracing::info!("successfully loaded KZG settings {:?}", *kzg);

    // initialize consensus layer objects
    tracing::info!("retrieving genesis details from beacon node ({})...", args.beacon_endpoint);
    let beacon = Arc::new(BeaconApiClient::new(args.beacon_endpoint));
    let genesis = beacon
        .get_genesis_details()
        .await
        .expect("can retrieve genesis details");
    let clock = clock::from_system_time(genesis.genesis_time, SECONDS_PER_SLOT, SLOTS_PER_EPOCH);
    let clock = Arc::new(clock);
    tracing::info!("successfully retrieved genesis details from beacon node {genesis:?}");

    // initialize container for blobs
    let blobs = BlobContainer::new(Arc::clone(&kzg));
    let blobs = Arc::new(RwLock::new(blobs));

    // initialize state
    let state = AppState {
        kzg,
        clock: Arc::clone(&clock),
        blobs: Arc::clone(&blobs),
    };
    let state = Arc::new(state);

    let inner_state = Arc::clone(&state);
    tokio::spawn(async move {
        // on each slot, retrieve blobs and ingest into state and also remove any "old" blobs
        let slots = clock.stream_slots();
        tokio::pin!(slots);
        while let Some(slot) = slots.next().await {
            // retrieve blobs for previous slot to give beacon node time to receive the blobs
            let slot = slot.saturating_sub(1);

            tracing::info!(%slot, "retrieving blobs from beacon node...");
            let blobs = match beacon.get_blob_sidecars(BlockId::Slot(slot), &[]).await {
                Ok(blobs) => blobs,
                Err(err) => {
                    tracing::warn!(%slot,
                        "failed to retreive blobs from beacon node w/ err {err}"
                    );
                    continue;
                }
            };
            tracing::info!(%slot,
                "successfully retrieved {} blobs from beacon node",
                blobs.len()
            );

            // if there are no blobs for the slot, then move on
            if blobs.is_empty() {
                continue;
            }

            let mut blob_state = inner_state.blobs.write().unwrap();

            // ingest blobs into blob state
            if let Err(err) = blob_state.ingest(slot, blobs) {
                tracing::error!(%slot, "failed to ingest blobs w/ err {err:?}");
                continue;
            }
            tracing::info!(%slot, "successfully ingested blobs");

            // remove blob info for the slot at the end of our availability window
            blob_state.remove(slot.saturating_sub(SLOTS_PER_EPOCH * BLOB_HISTORY_EPOCHS));
        }
    });

    let app = Router::new()
        .route("/eth/v1/blobs/:slot/samples/:row/:col", get(sample))
        .with_state(state);

    let ip = IpAddr::from([0, 0, 0, 0]);
    let addr = SocketAddr::new(ip, args.port);

    tracing::info!("starting blob server at {addr}...");
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .expect("can bind server");
}

async fn sample(
    State(state): State<Arc<AppState>>,
    Path((slot, row, col)): Path<(String, String, String)>,
) -> Result<Json<BlobSample>, StatusCode> {
    let slot = Slot::from_str(&slot).or(Err(StatusCode::BAD_REQUEST))?;
    let row = u64::from_str(&row).or(Err(StatusCode::BAD_REQUEST))?;
    let col = u64::from_str(&col).or(Err(StatusCode::BAD_REQUEST))?;

    // if the request is for a future slot, then error
    let current_slot = state.clock.current_slot().expect("beyond genesis");
    if slot > current_slot {
        return Err(StatusCode::BAD_REQUEST);
    }

    // if the request is for a slot from an epoch that is too far in the past, then error
    let current_epoch = state.clock.current_epoch().expect("beyond genesis");
    if state.clock.epoch_for(slot) < current_epoch.saturating_sub(BLOB_HISTORY_EPOCHS) {
        return Err(StatusCode::BAD_REQUEST);
    }

    match state
        .blobs
        .read()
        .unwrap()
        .sample(Slot::from(slot), row, col)
        .cloned()
    {
        Some(sample) => Ok(Json(sample)),
        None => Err(StatusCode::NOT_FOUND),
    }
}

mod compat {
    use super::*;

    pub fn blob_from_sidecar(sidecar: BlobSidecar<BYTES_PER_BLOB>) -> Result<Blob, KzgError> {
        Ok(Blob::from_bytes(sidecar.blob.as_slice())?)
    }
}
