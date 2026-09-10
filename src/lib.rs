use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use reqwest::{Client, Method};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tracing::info;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    pub chirpstack: ChirpStackConfig,
    pub plan_path: PathBuf,
    pub state_path: PathBuf,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        read_yaml(path)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { bind: default_bind() }
    }
}

fn default_bind() -> String { "0.0.0.0:8085".into() }

#[derive(Debug, Clone, Deserialize)]
pub struct ChirpStackConfig {
    pub endpoint: String,
    #[serde(default = "default_token_env")]
    pub api_token_env: String,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}

fn default_token_env() -> String { "CHIRPSTACK_API_TOKEN".into() }
fn default_timeout() -> u64 { 15 }

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Plan {
    #[serde(default = "default_plan_version")]
    pub version: String,
    #[serde(default)]
    pub tenant: Option<TenantSpec>,
    #[serde(default)]
    pub device_profiles: Vec<DeviceProfileSpec>,
    #[serde(default)]
    pub applications: Vec<ApplicationSpec>,
    #[serde(default)]
    pub gateways: Vec<GatewaySpec>,
    #[serde(default)]
    pub devices: Vec<DeviceSpec>,
}

fn default_plan_version() -> String { "v1".into() }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantSpec {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_true")]
    pub can_have_gateways: bool,
    #[serde(default)]
    pub max_gateway_count: u32,
    #[serde(default)]
    pub max_device_count: u32,
}

fn default_true() -> bool { true }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceProfileSpec {
    pub key: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_region")]
    pub region_config_id: String,
    #[serde(default = "default_true")]
    pub supports_otaa: bool,
    #[serde(default)]
    pub supports_class_b: bool,
    #[serde(default)]
    pub supports_class_c: bool,
    #[serde(default)]
    pub uplink_interval: u32,
    #[serde(default)]
    pub device_status_req_interval: u32,
}

fn default_region() -> String { "eu868".into() }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplicationSpec {
    pub key: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewaySpec {
    pub gateway_id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub stats_interval: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceSpec {
    pub dev_eui: String,
    pub name: String,
    pub application: String,
    pub device_profile: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub join_eui: Option<String>,
    #[serde(default)]
    pub app_key_env: Option<String>,
    #[serde(default)]
    pub nwk_key_env: Option<String>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProvisionState {
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub device_profiles: BTreeMap<String, String>,
    #[serde(default)]
    pub applications: BTreeMap<String, String>,
    #[serde(default)]
    pub gateways: BTreeMap<String, String>,
    #[serde(default)]
    pub devices: BTreeMap<String, DeviceState>,
    #[serde(default)]
    pub last_reconciled_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceState {
    pub dev_eui: String,
    pub application_id: String,
    pub device_profile_id: String,
    #[serde(default)]
    pub keys_applied: bool,
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub plan: Arc<Mutex<Plan>>,
    pub state: Arc<Mutex<ProvisionState>>,
    pub client: Arc<ChirpStackClient>,
}

impl AppState {
    pub async fn load(config: Config) -> Result<Self> {
        let plan = read_yaml(&config.plan_path)?;
        let state = if config.state_path.exists() {
            read_json(&config.state_path)?
        } else {
            ProvisionState::default()
        };
        let client = ChirpStackClient::new(&config.chirpstack)?;
        Ok(Self {
            config: Arc::new(config),
            plan: Arc::new(Mutex::new(plan)),
            state: Arc::new(Mutex::new(state)),
            client: Arc::new(client),
        })
    }

    async fn persist_state(&self, state: &ProvisionState) -> Result<()> {
        atomic_write_json(&self.config.state_path, state)
    }
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/state", get(get_state))
        .route("/v1/reconcile", post(reconcile))
        .route("/v1/devices", post(register_device))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let reachable = state.client.health().await.is_ok();
    Ok(Json(json!({"status":"ok", "chirpstack_reachable": reachable})))
}

async fn get_state(State(state): State<AppState>) -> Result<Json<ProvisionState>, ApiError> {
    Ok(Json(state.state.lock().await.clone()))
}

async fn reconcile(State(state): State<AppState>) -> Result<Json<ProvisionState>, ApiError> {
    let plan = state.plan.lock().await.clone();
    let mut current = state.state.lock().await.clone();
    reconcile_plan(&state.client, &plan, &mut current).await.map_err(ApiError::from)?;
    state.persist_state(&current).await.map_err(ApiError::from)?;
    *state.state.lock().await = current.clone();
    Ok(Json(current))
}

async fn register_device(
    State(state): State<AppState>,
    Json(device): Json<DeviceSpec>,
) -> Result<(StatusCode, Json<ProvisionState>), ApiError> {
    let mut plan = state.plan.lock().await;
    if let Some(existing) = plan.devices.iter_mut().find(|d| d.dev_eui.eq_ignore_ascii_case(&device.dev_eui)) {
        *existing = device;
    } else {
        plan.devices.push(device);
    }
    atomic_write_yaml(&state.config.plan_path, &*plan).map_err(ApiError::from)?;
    let mut current = state.state.lock().await.clone();
    reconcile_plan(&state.client, &plan, &mut current).await.map_err(ApiError::from)?;
    state.persist_state(&current).await.map_err(ApiError::from)?;
    *state.state.lock().await = current.clone();
    Ok((StatusCode::ACCEPTED, Json(current)))
}

pub async fn reconcile_plan(client: &ChirpStackClient, plan: &Plan, state: &mut ProvisionState) -> Result<()> {
    validate_plan(plan)?;
    if let Some(spec) = &plan.tenant {
        let id = ensure_tenant(client, spec, state.tenant_id.clone()).await?;
        state.tenant_id = Some(id);
    }
    let tenant_id = state.tenant_id.clone().ok_or_else(|| anyhow!("plan requires a tenant"))?;

    for spec in &plan.device_profiles {
        let existing = state.device_profiles.get(&spec.key).cloned();
        let id = ensure_device_profile(client, spec, &tenant_id, existing).await?;
        state.device_profiles.insert(spec.key.clone(), id);
    }
    for spec in &plan.applications {
        let existing = state.applications.get(&spec.key).cloned();
        let id = ensure_application(client, spec, &tenant_id, existing).await?;
        state.applications.insert(spec.key.clone(), id);
    }
    for spec in &plan.gateways {
        let existing = state.gateways.get(&spec.gateway_id).cloned();
        let id = ensure_gateway(client, spec, &tenant_id, existing).await?;
        state.gateways.insert(spec.gateway_id.clone(), id);
    }
    for spec in &plan.devices {
        let app_id = state.applications.get(&spec.application).ok_or_else(|| anyhow!("unknown application reference: {}", spec.application))?.clone();
        let profile_id = state.device_profiles.get(&spec.device_profile).ok_or_else(|| anyhow!("unknown device-profile reference: {}", spec.device_profile))?.clone();
        let applied = ensure_device(client, spec, &app_id, &profile_id, state.devices.get(&spec.dev_eui).cloned()).await?;
        state.devices.insert(spec.dev_eui.clone(), applied);
    }
    state.last_reconciled_at = Some(Utc::now());
    Ok(())
}

fn validate_plan(plan: &Plan) -> Result<()> {
    if plan.tenant.is_none() {
        return Err(anyhow!("plan requires a tenant"));
    }

    let applications: std::collections::BTreeSet<_> =
        plan.applications.iter().map(|item| item.key.as_str()).collect();
    let profiles: std::collections::BTreeSet<_> = plan
        .device_profiles
        .iter()
        .map(|item| item.key.as_str())
        .collect();

    for device in &plan.devices {
        if !applications.contains(device.application.as_str()) {
            return Err(anyhow!(
                "device {} references unknown application {}",
                device.dev_eui,
                device.application
            ));
        }
        if !profiles.contains(device.device_profile.as_str()) {
            return Err(anyhow!(
                "device {} references unknown device profile {}",
                device.dev_eui,
                device.device_profile
            ));
        }
    }
    Ok(())
}

async fn ensure_tenant(client: &ChirpStackClient, spec: &TenantSpec, known: Option<String>) -> Result<String> {
    let body = json!({"tenant": {"name": spec.name, "description": spec.description, "can_have_gateways": spec.can_have_gateways, "max_gateway_count": spec.max_gateway_count, "max_device_count": spec.max_device_count}});
    upsert_named(client, "/api/tenants", &spec.name, known, body, "tenant").await
}

async fn ensure_application(client: &ChirpStackClient, spec: &ApplicationSpec, tenant_id: &str, known: Option<String>) -> Result<String> {
    let body = json!({"application": {"name": spec.name, "description": spec.description, "tenant_id": tenant_id}});
    upsert_named(client, &format!("/api/applications?tenant_id={tenant_id}"), &spec.name, known, body, "application").await
}

async fn ensure_device_profile(client: &ChirpStackClient, spec: &DeviceProfileSpec, tenant_id: &str, known: Option<String>) -> Result<String> {
    let body = json!({"device_profile": {"tenant_id": tenant_id, "name": spec.name, "description": spec.description, "region_config_id": spec.region_config_id, "supports_otaa": spec.supports_otaa, "supports_class_b": spec.supports_class_b, "supports_class_c": spec.supports_class_c, "uplink_interval": spec.uplink_interval, "device_status_req_interval": spec.device_status_req_interval}});
    upsert_named(client, &format!("/api/device-profiles?tenant_id={tenant_id}"), &spec.name, known, body, "device_profile").await
}

async fn ensure_gateway(client: &ChirpStackClient, spec: &GatewaySpec, tenant_id: &str, known: Option<String>) -> Result<String> {
    let body = json!({"gateway": {"gateway_id": spec.gateway_id, "name": spec.name, "description": spec.description, "tenant_id": tenant_id, "stats_interval": spec.stats_interval}});
    let path = format!("/api/gateways/{}", spec.gateway_id);
    if client.get(&path).await.is_ok() {
        client.put(&path, body).await?;
        return Ok(spec.gateway_id.clone());
    }
    let _ = known;
    client.post("/api/gateways", body).await?;
    Ok(spec.gateway_id.clone())
}

async fn ensure_device(client: &ChirpStackClient, spec: &DeviceSpec, application_id: &str, profile_id: &str, known: Option<DeviceState>) -> Result<DeviceState> {
    let mut device = json!({"dev_eui": spec.dev_eui, "name": spec.name, "description": spec.description, "application_id": application_id, "device_profile_id": profile_id, "is_disabled": false, "skip_fcnt_check": false, "tags": spec.tags});
    if let Some(join_eui) = &spec.join_eui { device["join_eui"] = json!(join_eui); }
    let body = json!({"device": device});
    let path = format!("/api/devices/{}", spec.dev_eui);
    if client.get(&path).await.is_ok() {
        client.put(&path, body).await?;
    } else {
        client.post("/api/devices", body).await?;
    }
    let keys_applied = apply_device_keys(client, spec, known.as_ref().map(|s| s.keys_applied)).await?;
    Ok(DeviceState { dev_eui: spec.dev_eui.clone(), application_id: application_id.into(), device_profile_id: profile_id.into(), keys_applied })
}

async fn apply_device_keys(client: &ChirpStackClient, spec: &DeviceSpec, known: Option<bool>) -> Result<bool> {
    let app_key = spec.app_key_env.as_deref().and_then(|name| std::env::var(name).ok());
    let nwk_key = spec.nwk_key_env.as_deref().and_then(|name| std::env::var(name).ok());
    if app_key.is_none() && nwk_key.is_none() { return Ok(known.unwrap_or(false)); }
    let body = json!({"device_keys": {"dev_eui": spec.dev_eui, "nwk_key": nwk_key.unwrap_or_default(), "app_key": app_key.unwrap_or_default()}});
    let path = format!("/api/devices/{}/keys", spec.dev_eui);
    if client.get(&path).await.is_ok() { client.put(&path, body).await?; } else { client.post(&path, body).await?; }
    Ok(true)
}

async fn upsert_named(client: &ChirpStackClient, collection: &str, name: &str, known: Option<String>, body: Value, wrapper: &str) -> Result<String> {
    if let Some(id) = known {
        let path = format!("{}/{}", collection.split('?').next().unwrap_or(collection), id);
        if client.get(&path).await.is_ok() {
            let mut update = body;
            update[wrapper]["id"] = json!(id);
            client.put(&path, update).await?;
            return Ok(id);
        }
    }
    if let Ok(value) = client.get(collection).await {
        if let Some(id) = find_result_id(&value, name) {
            let mut update = body;
            update[wrapper]["id"] = json!(id);
            client.put(&format!("{}/{}", collection.split('?').next().unwrap_or(collection), id), update).await?;
            return Ok(id);
        }
    }
    let response = client.post(collection.split('?').next().unwrap_or(collection), body).await?;
    response.get("id").and_then(Value::as_str).map(str::to_owned).or_else(|| response.get(wrapper).and_then(|v| v.get("id")).and_then(Value::as_str).map(str::to_owned)).ok_or_else(|| anyhow!("ChirpStack create {} response did not contain an id", wrapper))
}

fn find_result_id(value: &Value, name: &str) -> Option<String> {
    value.get("result")?.as_array()?.iter().find(|item| item.get("name").and_then(Value::as_str) == Some(name)).and_then(|item| item.get("id").and_then(Value::as_str)).map(str::to_owned)
}

pub struct ChirpStackClient { http: Client, base: String, token: String }

impl ChirpStackClient {
    fn new(config: &ChirpStackConfig) -> Result<Self> {
        let token = std::env::var(&config.api_token_env).with_context(|| format!("missing {}", config.api_token_env))?;
        let http = Client::builder().timeout(std::time::Duration::from_secs(config.timeout_seconds)).build()?;
        Ok(Self { http, base: config.endpoint.trim_end_matches('/').into(), token })
    }
    async fn health(&self) -> Result<()> { self.get("/").await.map(|_| ()) }
    async fn get(&self, path: &str) -> Result<Value> { self.request(Method::GET, path, None).await }
    async fn post(&self, path: &str, body: Value) -> Result<Value> { self.request(Method::POST, path, Some(body)).await }
    async fn put(&self, path: &str, body: Value) -> Result<Value> { self.request(Method::PUT, path, Some(body)).await }
    async fn request(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let mut request = self
            .http
            .request(method, format!("{}{}", self.base, path))
            .bearer_auth(&self.token);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await?;
        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() { return Err(anyhow!("ChirpStack API {} {}: {}", status, path, text)); }
        if text.trim().is_empty() { return Ok(json!({})); }
        serde_json::from_str(&text).with_context(|| format!("invalid ChirpStack JSON from {path}"))
    }
}

#[derive(Debug)]
pub struct ApiError(anyhow::Error);
impl From<anyhow::Error> for ApiError { fn from(error: anyhow::Error) -> Self { Self(error) } }
impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response { (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": self.0.to_string()}))).into_response() }
}

fn read_yaml<T: DeserializeOwned>(path: &Path) -> Result<T> { Ok(serde_yaml::from_slice(&std::fs::read(path)?)?) }
fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> { Ok(serde_json::from_slice(&std::fs::read(path)?)?) }

fn atomic_write_yaml<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
    let temp = path.with_extension("yaml.tmp");
    std::fs::write(&temp, serde_yaml::to_string(value)?)?;
    std::fs::rename(temp, path)?;
    Ok(())
}

fn atomic_write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
    let temp = path.with_extension("json.tmp");
    std::fs::write(&temp, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(temp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn plan_round_trips_yaml() {
        let plan = Plan { version: "v1".into(), tenant: Some(TenantSpec { name: "DATUM Lab".into(), description: "test".into(), can_have_gateways: true, max_gateway_count: 0, max_device_count: 0 }), ..Plan::default() };
        let encoded = serde_yaml::to_string(&plan).unwrap();
        let decoded: Plan = serde_yaml::from_str(&encoded).unwrap();
        assert_eq!(decoded.tenant.unwrap().name, "DATUM Lab");
    }

    #[test]
    fn state_is_written_atomically() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        let state = ProvisionState { tenant_id: Some("tenant-1".into()), ..Default::default() };
        atomic_write_json(&path, &state).unwrap();
        let loaded: ProvisionState = read_json(&path).unwrap();
        assert_eq!(loaded.tenant_id.as_deref(), Some("tenant-1"));
        assert!(!path.with_extension("json.tmp").exists());
    }

    #[test]
    fn unknown_references_are_rejected_before_device_calls() {
        let plan = Plan { tenant: Some(TenantSpec { name: "tenant".into(), description: String::new(), can_have_gateways: true, max_gateway_count: 0, max_device_count: 0 }), devices: vec![DeviceSpec { dev_eui: "0102030405060708".into(), name: "dev".into(), application: "missing".into(), device_profile: "missing".into(), description: String::new(), join_eui: None, app_key_env: None, nwk_key_env: None, tags: BTreeMap::new() }], ..Default::default() };
        let error = validate_plan(&plan).unwrap_err().to_string();
        assert!(error.contains("unknown application"));
    }
}
