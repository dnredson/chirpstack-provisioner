use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;

pub mod internal_bootstrap {
    tonic::include_proto!("api");
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    pub chirpstack: ChirpStackConfig,
    pub plan_path: PathBuf,
    #[serde(default)]
    pub bootstrap_plan_path: Option<PathBuf>,
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
        Self {
            bind: default_bind(),
        }
    }
}

fn default_bind() -> String {
    "0.0.0.0:8085".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChirpStackConfig {
    pub endpoint: String,
    #[serde(default)]
    pub api_token_env: Option<String>,
    #[serde(default)]
    pub bootstrap: BootstrapAuthConfig,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BootstrapAuthConfig {
    #[serde(default = "default_bootstrap_email_env")]
    pub email_env: String,
    #[serde(default = "default_bootstrap_password_env")]
    pub password_env: String,
    #[serde(default = "default_bootstrap_email")]
    pub default_email: String,
    #[serde(default = "default_bootstrap_password")]
    pub default_password: String,
    #[serde(default = "default_api_key_name")]
    pub api_key_name: String,
    #[serde(default = "default_token_path")]
    pub token_path: PathBuf,
}

impl Default for BootstrapAuthConfig {
    fn default() -> Self {
        Self {
            email_env: default_bootstrap_email_env(),
            password_env: default_bootstrap_password_env(),
            default_email: default_bootstrap_email(),
            default_password: default_bootstrap_password(),
            api_key_name: default_api_key_name(),
            token_path: default_token_path(),
        }
    }
}

fn default_bootstrap_email_env() -> String {
    "CHIRPSTACK_BOOTSTRAP_EMAIL".into()
}
fn default_bootstrap_password_env() -> String {
    "CHIRPSTACK_BOOTSTRAP_PASSWORD".into()
}
fn default_bootstrap_email() -> String {
    "admin".into()
}
fn default_bootstrap_password() -> String {
    "admin".into()
}
fn default_api_key_name() -> String {
    "datum-chirpstack-provisioner".into()
}
fn default_token_path() -> PathBuf {
    "/var/lib/chirpstack-provisioner/api-token".into()
}
fn default_timeout() -> u64 {
    15
}

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

fn default_plan_version() -> String {
    "v1".into()
}

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

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceProfileSpec {
    pub key: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_region")]
    pub region_config_id: String,
    /// Radio region for custom ChirpStack region configuration IDs.
    #[serde(default)]
    pub region: Option<String>,
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

fn default_region() -> String {
    "eu868".into()
}

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
        let plan = if config.plan_path.exists() {
            read_yaml(&config.plan_path)?
        } else if let Some(bootstrap_path) = &config.bootstrap_plan_path {
            let plan = read_yaml(bootstrap_path)?;
            atomic_write_yaml(&config.plan_path, &plan)?;
            plan
        } else {
            return Err(anyhow!(
                "plan file does not exist: {}",
                config.plan_path.display()
            ));
        };
        let state = if config.state_path.exists() {
            read_json(&config.state_path)?
        } else {
            ProvisionState::default()
        };
        let token = resolve_api_token(&config).await?;
        let client = ChirpStackClient::new(&config.chirpstack, token)?;
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
    Ok(Json(
        json!({"status":"ok", "chirpstack_reachable": reachable}),
    ))
}

async fn get_state(State(state): State<AppState>) -> Result<Json<ProvisionState>, ApiError> {
    Ok(Json(state.state.lock().await.clone()))
}

async fn reconcile(State(state): State<AppState>) -> Result<Json<ProvisionState>, ApiError> {
    let plan = state.plan.lock().await.clone();
    let mut current = state.state.lock().await.clone();
    reconcile_plan(&state.client, &plan, &mut current)
        .await
        .map_err(ApiError::from)?;
    state
        .persist_state(&current)
        .await
        .map_err(ApiError::from)?;
    *state.state.lock().await = current.clone();
    Ok(Json(current))
}

async fn register_device(
    State(state): State<AppState>,
    Json(device): Json<DeviceSpec>,
) -> Result<(StatusCode, Json<ProvisionState>), ApiError> {
    let mut plan = state.plan.lock().await;
    if let Some(existing) = plan
        .devices
        .iter_mut()
        .find(|d| d.dev_eui.eq_ignore_ascii_case(&device.dev_eui))
    {
        *existing = device;
    } else {
        plan.devices.push(device);
    }
    atomic_write_yaml(&state.config.plan_path, &*plan).map_err(ApiError::from)?;
    let mut current = state.state.lock().await.clone();
    reconcile_plan(&state.client, &plan, &mut current)
        .await
        .map_err(ApiError::from)?;
    state
        .persist_state(&current)
        .await
        .map_err(ApiError::from)?;
    *state.state.lock().await = current.clone();
    Ok((StatusCode::ACCEPTED, Json(current)))
}

pub async fn reconcile_plan(
    client: &ChirpStackClient,
    plan: &Plan,
    state: &mut ProvisionState,
) -> Result<()> {
    validate_plan(plan)?;
    if let Some(spec) = &plan.tenant {
        let id = ensure_tenant(client, spec, state.tenant_id.clone()).await?;
        state.tenant_id = Some(id);
    }
    let tenant_id = state
        .tenant_id
        .clone()
        .ok_or_else(|| anyhow!("plan requires a tenant"))?;

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
        let app_id = state
            .applications
            .get(&spec.application)
            .ok_or_else(|| anyhow!("unknown application reference: {}", spec.application))?
            .clone();
        let profile_id = state
            .device_profiles
            .get(&spec.device_profile)
            .ok_or_else(|| anyhow!("unknown device-profile reference: {}", spec.device_profile))?
            .clone();
        let applied = ensure_device(
            client,
            spec,
            &app_id,
            &profile_id,
            state.devices.get(&spec.dev_eui).cloned(),
        )
        .await?;
        state.devices.insert(spec.dev_eui.clone(), applied);
    }
    state.last_reconciled_at = Some(Utc::now());
    Ok(())
}

fn validate_plan(plan: &Plan) -> Result<()> {
    if plan.tenant.is_none() {
        return Err(anyhow!("plan requires a tenant"));
    }
    for profile in &plan.device_profiles {
        parse_region(
            profile
                .region
                .as_deref()
                .unwrap_or(&profile.region_config_id),
        )?;
    }

    let applications: std::collections::BTreeSet<_> = plan
        .applications
        .iter()
        .map(|item| item.key.as_str())
        .collect();
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

async fn ensure_tenant(
    client: &ChirpStackClient,
    spec: &TenantSpec,
    known: Option<String>,
) -> Result<String> {
    let body = json!({"tenant": {"name": spec.name, "description": spec.description, "can_have_gateways": spec.can_have_gateways, "max_gateway_count": spec.max_gateway_count, "max_device_count": spec.max_device_count}});
    upsert_named(client, "/api/tenants", &spec.name, known, body, "tenant").await
}

async fn ensure_application(
    client: &ChirpStackClient,
    spec: &ApplicationSpec,
    tenant_id: &str,
    known: Option<String>,
) -> Result<String> {
    let body = json!({"application": {"name": spec.name, "description": spec.description, "tenant_id": tenant_id}});
    upsert_named(
        client,
        &format!("/api/applications?tenant_id={tenant_id}"),
        &spec.name,
        known,
        body,
        "application",
    )
    .await
}

async fn ensure_device_profile(
    client: &ChirpStackClient,
    spec: &DeviceProfileSpec,
    tenant_id: &str,
    known: Option<String>,
) -> Result<String> {
    let body = json!({"device_profile": {"tenant_id": tenant_id, "name": spec.name, "description": spec.description, "region_config_id": spec.region_config_id, "region": spec.region.as_deref().unwrap_or(&spec.region_config_id), "supports_otaa": spec.supports_otaa, "supports_class_b": spec.supports_class_b, "supports_class_c": spec.supports_class_c, "uplink_interval": spec.uplink_interval, "device_status_req_interval": spec.device_status_req_interval}});
    upsert_named(
        client,
        &format!("/api/device-profiles?tenant_id={tenant_id}"),
        &spec.name,
        known,
        body,
        "device_profile",
    )
    .await
}

async fn ensure_gateway(
    client: &ChirpStackClient,
    spec: &GatewaySpec,
    tenant_id: &str,
    known: Option<String>,
) -> Result<String> {
    let body = json!({"gateway": {"gateway_id": spec.gateway_id, "name": spec.name, "description": spec.description, "tenant_id": tenant_id, "stats_interval": spec.stats_interval}});
    let path = format!("/api/gateways/{}", spec.gateway_id);
    if client.exists(&path).await? {
        client.put(&path, body).await?;
        return Ok(spec.gateway_id.clone());
    }
    let _ = known;
    client.post("/api/gateways", body).await?;
    Ok(spec.gateway_id.clone())
}

async fn ensure_device(
    client: &ChirpStackClient,
    spec: &DeviceSpec,
    application_id: &str,
    profile_id: &str,
    known: Option<DeviceState>,
) -> Result<DeviceState> {
    let mut device = json!({"dev_eui": spec.dev_eui, "name": spec.name, "description": spec.description, "application_id": application_id, "device_profile_id": profile_id, "is_disabled": false, "skip_fcnt_check": false, "tags": spec.tags});
    if let Some(join_eui) = &spec.join_eui {
        device["join_eui"] = json!(join_eui);
    }
    let body = json!({"device": device});
    let path = format!("/api/devices/{}", spec.dev_eui);
    if client.exists(&path).await? {
        client.put(&path, body).await?;
    } else {
        client.post("/api/devices", body).await?;
    }
    let keys_applied =
        apply_device_keys(client, spec, known.as_ref().map(|s| s.keys_applied)).await?;
    Ok(DeviceState {
        dev_eui: spec.dev_eui.clone(),
        application_id: application_id.into(),
        device_profile_id: profile_id.into(),
        keys_applied,
    })
}

async fn apply_device_keys(
    client: &ChirpStackClient,
    spec: &DeviceSpec,
    known: Option<bool>,
) -> Result<bool> {
    let app_key = spec
        .app_key_env
        .as_deref()
        .and_then(|name| std::env::var(name).ok());
    let nwk_key = spec
        .nwk_key_env
        .as_deref()
        .and_then(|name| std::env::var(name).ok());
    if app_key.is_none() && nwk_key.is_none() {
        return Ok(known.unwrap_or(false));
    }
    let body = json!({"device_keys": {"dev_eui": spec.dev_eui, "nwk_key": nwk_key.unwrap_or_default(), "app_key": app_key.unwrap_or_default()}});
    let path = format!("/api/devices/{}/keys", spec.dev_eui);
    if client.exists(&path).await? {
        client.put(&path, body).await?;
    } else {
        client.post(&path, body).await?;
    }
    Ok(true)
}

async fn upsert_named(
    client: &ChirpStackClient,
    collection: &str,
    name: &str,
    known: Option<String>,
    body: Value,
    wrapper: &str,
) -> Result<String> {
    if let Some(id) = known {
        let path = format!(
            "{}/{}",
            collection.split('?').next().unwrap_or(collection),
            id
        );
        if client.exists(&path).await? {
            let mut update = body;
            update[wrapper]["id"] = json!(id);
            client.put(&path, update).await?;
            return Ok(id);
        }
    }
    {
        let value = client.get(collection).await?;
        if let Some(id) = find_result_id(&value, name) {
            let mut update = body;
            update[wrapper]["id"] = json!(id);
            client
                .put(
                    &format!(
                        "{}/{}",
                        collection.split('?').next().unwrap_or(collection),
                        id
                    ),
                    update,
                )
                .await?;
            return Ok(id);
        }
    }
    let response = client
        .post(collection.split('?').next().unwrap_or(collection), body)
        .await?;
    response
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            response
                .get(wrapper)
                .and_then(|v| v.get("id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .ok_or_else(|| {
            anyhow!(
                "ChirpStack create {} response did not contain an id",
                wrapper
            )
        })
}

fn find_result_id(value: &Value, name: &str) -> Option<String> {
    value
        .get("result")?
        .as_array()?
        .iter()
        .find(|item| item.get("name").and_then(Value::as_str) == Some(name))
        .and_then(|item| item.get("id").and_then(Value::as_str))
        .map(str::to_owned)
}

async fn resolve_api_token(config: &Config) -> Result<String> {
    if let Some(env_name) = &config.chirpstack.api_token_env {
        if let Ok(token) = std::env::var(env_name) {
            if !token.trim().is_empty() {
                return Ok(token);
            }
        }
    }

    let token_path = &config.chirpstack.bootstrap.token_path;
    if token_path.exists() {
        let token = std::fs::read_to_string(token_path).with_context(|| {
            format!(
                "read persisted ChirpStack API token: {}",
                token_path.display()
            )
        })?;
        if !token.trim().is_empty() {
            return Ok(token.trim().to_string());
        }
    }

    let email = std::env::var(&config.chirpstack.bootstrap.email_env)
        .unwrap_or_else(|_| config.chirpstack.bootstrap.default_email.clone());
    let password = std::env::var(&config.chirpstack.bootstrap.password_env)
        .unwrap_or_else(|_| config.chirpstack.bootstrap.default_password.clone());

    let token = ChirpStackClient::bootstrap_api_token(
        &config.chirpstack.endpoint,
        &email,
        &password,
        &config.chirpstack.bootstrap.api_key_name,
        Duration::from_secs(config.chirpstack.timeout_seconds),
    )
    .await
    .context("automatic ChirpStack bootstrap authentication")?;

    atomic_write_secret(token_path, &token)?;
    Ok(token)
}

fn string_map(value: &Value, field: &str) -> HashMap<String, String> {
    value
        .get(field)
        .and_then(Value::as_object)
        .map(|items| {
            items
                .iter()
                .filter_map(|(key, value)| {
                    value.as_str().map(|value| (key.clone(), value.to_owned()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn append_page(result: &mut Vec<Value>, page: Vec<Value>, total: u32) -> Result<bool> {
    if page.is_empty() && result.len() < total as usize {
        return Err(anyhow!(
            "ChirpStack returned an empty page before total_count"
        ));
    }
    result.extend(page);
    Ok(result.len() >= total as usize)
}

fn parse_region(value: &str) -> Result<i32> {
    let normalized = value.to_ascii_uppercase().replace('-', "_");
    let name = match normalized.rsplit_once('_') {
        Some((band @ ("US915" | "AU915"), subband))
            if subband.len() == 1 && matches!(subband.as_bytes()[0], b'0'..=b'7') =>
        {
            band
        }
        _ => &normalized,
    };
    chirpstack_api::common::Region::from_str_name(name)
        .map(i32::from)
        .ok_or_else(|| anyhow!("unsupported radio region '{value}'; custom region_config_id requires an explicit region"))
}

fn gateway_from_body(body: &Value) -> chirpstack_api::api::Gateway {
    let gateway = &body["gateway"];
    chirpstack_api::api::Gateway {
        gateway_id: gateway["gateway_id"].as_str().unwrap_or_default().into(),
        name: gateway["name"].as_str().unwrap_or_default().into(),
        description: gateway["description"].as_str().unwrap_or_default().into(),
        tenant_id: gateway["tenant_id"].as_str().unwrap_or_default().into(),
        stats_interval: gateway["stats_interval"].as_u64().unwrap_or(0) as u32,
        ..Default::default()
    }
}

pub struct ChirpStackClient {
    endpoint: String,
    token: String,
    timeout: Duration,
}

impl ChirpStackClient {
    fn new(config: &ChirpStackConfig, token: String) -> Result<Self> {
        Ok(Self {
            endpoint: config.endpoint.trim_end_matches('/').into(),
            token,
            timeout: Duration::from_secs(config.timeout_seconds),
        })
    }

    fn auth<T>(&self, value: T) -> Result<tonic::Request<T>> {
        let mut request = tonic::Request::new(value);
        let metadata = tonic::metadata::MetadataValue::try_from(format!("Bearer {}", self.token))
            .context("encode ChirpStack authorization metadata")?;
        request.metadata_mut().insert("authorization", metadata);
        request.set_timeout(self.timeout);
        Ok(request)
    }

    async fn channel(&self) -> Result<tonic::transport::Channel> {
        tonic::transport::Endpoint::from_shared(self.endpoint.clone())?
            .connect_timeout(self.timeout)
            .timeout(self.timeout)
            .connect()
            .await
            .context("connect to ChirpStack gRPC API")
    }

    async fn bootstrap_api_token(
        endpoint: &str,
        email: &str,
        password: &str,
        api_key_name: &str,
        timeout: Duration,
    ) -> Result<String> {
        let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())?
            .connect_timeout(timeout)
            .timeout(timeout)
            .connect()
            .await
            .context("connect to ChirpStack internal gRPC API")?;
        let mut client =
            internal_bootstrap::internal_service_client::InternalServiceClient::new(channel);

        let mut login_request = tonic::Request::new(internal_bootstrap::LoginRequest {
            email: email.to_string(),
            password: password.to_string(),
        });
        login_request.set_timeout(timeout);
        let login = client
            .login(login_request)
            .await
            .context("ChirpStack bootstrap login")?
            .into_inner()
            .jwt;

        let mut request = tonic::Request::new(internal_bootstrap::CreateApiKeyRequest {
            api_key: Some(internal_bootstrap::ApiKey {
                name: api_key_name.to_string(),
                is_admin: true,
                tenant_id: String::new(),
                is_read_only: false,
                id: String::new(),
            }),
        });
        request.set_timeout(timeout);
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {login}")
                .parse()
                .context("encode ChirpStack authorization metadata")?,
        );

        let token = client
            .create_api_key(request)
            .await
            .context("create ChirpStack provisioner API key")?
            .into_inner()
            .token;

        if token.trim().is_empty() {
            return Err(anyhow!("ChirpStack returned an empty API token"));
        }
        Ok(token)
    }

    async fn health(&self) -> Result<()> {
        let mut client = chirpstack_api::api::internal_service_client::InternalServiceClient::new(
            self.channel().await?,
        );
        client
            .get_version(self.auth(())?)
            .await
            .map(|_| ())
            .context("ChirpStack gRPC health check")
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        match self.get(path).await {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .downcast_ref::<tonic::Status>()
                    .is_some_and(|status| status.code() == tonic::Code::NotFound) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    async fn get(&self, path: &str) -> Result<Value> {
        use chirpstack_api::api::{
            application_service_client::ApplicationServiceClient,
            device_profile_service_client::DeviceProfileServiceClient,
            device_service_client::DeviceServiceClient, tenant_service_client::TenantServiceClient,
        };

        if path == "/" {
            self.health().await?;
            return Ok(json!({"status": "ok"}));
        }

        if let Some(gateway_id) = path.strip_prefix("/api/gateways/") {
            let response = chirpstack_api::api::gateway_service_client::GatewayServiceClient::new(
                self.channel().await?,
            )
            .get(self.auth(chirpstack_api::api::GetGatewayRequest {
                gateway_id: gateway_id.into(),
            })?)
            .await?
            .into_inner();
            let gateway = response
                .gateway
                .ok_or_else(|| anyhow!("ChirpStack returned no gateway"))?;
            return Ok(json!({"gateway": {"gateway_id": gateway.gateway_id}}));
        }

        if path.starts_with("/api/tenants") {
            let mut client = TenantServiceClient::new(self.channel().await?);
            let request_path = path.trim_start_matches("/api/tenants");
            if request_path.is_empty() || request_path.starts_with('?') {
                let mut result = Vec::new();
                loop {
                    let offset =
                        u32::try_from(result.len()).context("pagination offset overflow")?;
                    let response = client
                        .list(self.auth(chirpstack_api::api::ListTenantsRequest {
                            limit: 100,
                            offset,
                            search: String::new(),
                            user_id: String::new(),
                        })?)
                        .await?
                        .into_inner();
                    let page = response
                        .result
                        .into_iter()
                        .map(|item| json!({"id": item.id, "name": item.name}))
                        .collect();
                    if append_page(&mut result, page, response.total_count)? {
                        return Ok(json!({"result": result}));
                    }
                }
            }
            let id = request_path.trim_start_matches('/');
            let response = client
                .get(self.auth(chirpstack_api::api::GetTenantRequest { id: id.into() })?)
                .await?
                .into_inner();
            let tenant = response
                .tenant
                .ok_or_else(|| anyhow!("ChirpStack returned no tenant"))?;
            return Ok(json!({"tenant": {"id": tenant.id, "name": tenant.name}}));
        }

        if path.starts_with("/api/applications") {
            let mut client = ApplicationServiceClient::new(self.channel().await?);
            if path.contains('?') {
                let tenant_id = path.split("tenant_id=").nth(1).unwrap_or_default();
                let mut result = Vec::new();
                loop {
                    let offset =
                        u32::try_from(result.len()).context("pagination offset overflow")?;
                    let response = client
                        .list(self.auth(chirpstack_api::api::ListApplicationsRequest {
                            limit: 100,
                            offset,
                            search: String::new(),
                            tenant_id: tenant_id.into(),
                        })?)
                        .await?
                        .into_inner();
                    let page = response
                        .result
                        .into_iter()
                        .map(|item| json!({"id": item.id, "name": item.name}))
                        .collect();
                    if append_page(&mut result, page, response.total_count)? {
                        return Ok(json!({"result": result}));
                    }
                }
            }
            let id = path.rsplit('/').next().unwrap_or_default();
            let response = client
                .get(self.auth(chirpstack_api::api::GetApplicationRequest { id: id.into() })?)
                .await?
                .into_inner();
            let application = response
                .application
                .ok_or_else(|| anyhow!("ChirpStack returned no application"))?;
            return Ok(json!({"application": {"id": application.id, "name": application.name}}));
        }

        if path.starts_with("/api/device-profiles") {
            let mut client = DeviceProfileServiceClient::new(self.channel().await?);
            if path.contains('?') {
                let tenant_id = path.split("tenant_id=").nth(1).unwrap_or_default();
                let mut result = Vec::new();
                loop {
                    let offset =
                        u32::try_from(result.len()).context("pagination offset overflow")?;
                    let response = client
                        .list(self.auth(chirpstack_api::api::ListDeviceProfilesRequest {
                            limit: 100,
                            offset,
                            search: String::new(),
                            tenant_id: tenant_id.into(),
                            device_id: String::new(),
                            global_only: false,
                            tenant_only: true,
                        })?)
                        .await?
                        .into_inner();
                    let page = response
                        .result
                        .into_iter()
                        .map(|item| json!({"id": item.id, "name": item.name}))
                        .collect();
                    if append_page(&mut result, page, response.total_count)? {
                        return Ok(json!({"result": result}));
                    }
                }
            }
            let id = path.rsplit('/').next().unwrap_or_default();
            let response = client
                .get(self.auth(chirpstack_api::api::GetDeviceProfileRequest { id: id.into() })?)
                .await?
                .into_inner();
            let profile = response
                .device_profile
                .ok_or_else(|| anyhow!("ChirpStack returned no device-profile"))?;
            return Ok(json!({"device_profile": {"id": profile.id, "name": profile.name}}));
        }

        if path.starts_with("/api/devices/") {
            let mut client = DeviceServiceClient::new(self.channel().await?);
            let suffix = path.trim_start_matches("/api/devices/");
            if suffix.ends_with("/keys") {
                let dev_eui = suffix.trim_end_matches("/keys");
                client
                    .get_keys(self.auth(chirpstack_api::api::GetDeviceKeysRequest {
                        dev_eui: dev_eui.into(),
                    })?)
                    .await?;
                return Ok(json!({}));
            }
            let response = client
                .get(self.auth(chirpstack_api::api::GetDeviceRequest {
                    dev_eui: suffix.into(),
                })?)
                .await?
                .into_inner();
            let device = response
                .device
                .ok_or_else(|| anyhow!("ChirpStack returned no device"))?;
            return Ok(json!({"device": {"dev_eui": device.dev_eui}}));
        }

        Err(anyhow!("unsupported ChirpStack gRPC GET path: {path}"))
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value> {
        if path == "/api/gateways" {
            chirpstack_api::api::gateway_service_client::GatewayServiceClient::new(
                self.channel().await?,
            )
            .create(self.auth(chirpstack_api::api::CreateGatewayRequest {
                gateway: Some(gateway_from_body(&body)),
            })?)
            .await?;
            return Ok(json!({}));
        }
        use chirpstack_api::api::{
            application_service_client::ApplicationServiceClient,
            device_profile_service_client::DeviceProfileServiceClient,
            device_service_client::DeviceServiceClient, tenant_service_client::TenantServiceClient,
        };

        if path == "/api/tenants" {
            let tenant = &body["tenant"];
            let response = TenantServiceClient::new(self.channel().await?)
                .create(self.auth(chirpstack_api::api::CreateTenantRequest {
                    tenant: Some(chirpstack_api::api::Tenant {
                        id: String::new(),
                        name: tenant["name"].as_str().unwrap_or_default().into(),
                        description: tenant["description"].as_str().unwrap_or_default().into(),
                        can_have_gateways: tenant["can_have_gateways"].as_bool().unwrap_or(true),
                        max_gateway_count: tenant["max_gateway_count"].as_u64().unwrap_or(0) as u32,
                        max_device_count: tenant["max_device_count"].as_u64().unwrap_or(0) as u32,
                        ..Default::default()
                    }),
                })?)
                .await?
                .into_inner();
            return Ok(json!({"id": response.id}));
        }

        if path == "/api/applications" {
            let application = &body["application"];
            let response = ApplicationServiceClient::new(self.channel().await?)
                .create(
                    self.auth(chirpstack_api::api::CreateApplicationRequest {
                        application: Some(chirpstack_api::api::Application {
                            id: String::new(),
                            name: application["name"].as_str().unwrap_or_default().into(),
                            description: application["description"]
                                .as_str()
                                .unwrap_or_default()
                                .into(),
                            tenant_id: application["tenant_id"].as_str().unwrap_or_default().into(),
                            ..Default::default()
                        }),
                    })?,
                )
                .await?
                .into_inner();
            return Ok(json!({"id": response.id}));
        }

        if path == "/api/device-profiles" {
            let profile = &body["device_profile"];
            let response = DeviceProfileServiceClient::new(self.channel().await?)
                .create(
                    self.auth(chirpstack_api::api::CreateDeviceProfileRequest {
                        device_profile: Some(chirpstack_api::api::DeviceProfile {
                            id: String::new(),
                            tenant_id: profile["tenant_id"].as_str().unwrap_or_default().into(),
                            name: profile["name"].as_str().unwrap_or_default().into(),
                            description: profile["description"].as_str().unwrap_or_default().into(),
                            region: parse_region(profile["region"].as_str().unwrap_or_default())?,
                            region_config_id: profile["region_config_id"]
                                .as_str()
                                .unwrap_or_default()
                                .into(),
                            mac_version: chirpstack_api::common::MacVersion::Lorawan103.into(),
                            reg_params_revision: chirpstack_api::common::RegParamsRevision::A
                                .into(),
                            adr_algorithm_id: "default".into(),
                            supports_otaa: profile["supports_otaa"].as_bool().unwrap_or(true),
                            supports_class_b: profile["supports_class_b"]
                                .as_bool()
                                .unwrap_or(false),
                            supports_class_c: profile["supports_class_c"]
                                .as_bool()
                                .unwrap_or(false),
                            uplink_interval: profile["uplink_interval"].as_u64().unwrap_or(60)
                                as u32,
                            device_status_req_interval: profile["device_status_req_interval"]
                                .as_u64()
                                .unwrap_or(86400)
                                as u32,
                            ..Default::default()
                        }),
                    })?,
                )
                .await?
                .into_inner();
            return Ok(json!({"id": response.id}));
        }

        if path == "/api/devices" {
            let device = &body["device"];
            DeviceServiceClient::new(self.channel().await?)
                .create(
                    self.auth(chirpstack_api::api::CreateDeviceRequest {
                        device: Some(chirpstack_api::api::Device {
                            dev_eui: device["dev_eui"].as_str().unwrap_or_default().into(),
                            name: device["name"].as_str().unwrap_or_default().into(),
                            description: device["description"].as_str().unwrap_or_default().into(),
                            application_id: device["application_id"]
                                .as_str()
                                .unwrap_or_default()
                                .into(),
                            device_profile_id: device["device_profile_id"]
                                .as_str()
                                .unwrap_or_default()
                                .into(),
                            join_eui: device["join_eui"].as_str().unwrap_or_default().into(),
                            skip_fcnt_check: false,
                            is_disabled: false,
                            tags: string_map(device, "tags"),
                            variables: HashMap::new(),
                        }),
                    })?,
                )
                .await?;
            return Ok(json!({}));
        }

        if path.ends_with("/keys") {
            let keys = &body["device_keys"];
            DeviceServiceClient::new(self.channel().await?)
                .create_keys(self.auth(chirpstack_api::api::CreateDeviceKeysRequest {
                    device_keys: Some(chirpstack_api::api::DeviceKeys {
                        dev_eui: keys["dev_eui"].as_str().unwrap_or_default().into(),
                        nwk_key: keys["nwk_key"].as_str().unwrap_or_default().into(),
                        app_key: keys["app_key"].as_str().unwrap_or_default().into(),
                        ..Default::default()
                    }),
                })?)
                .await?;
            return Ok(json!({}));
        }

        Err(anyhow!("unsupported ChirpStack gRPC POST path: {path}"))
    }

    async fn put(&self, path: &str, body: Value) -> Result<Value> {
        if let Some(gateway_id) = path.strip_prefix("/api/gateways/") {
            let mut gateway = gateway_from_body(&body);
            gateway.gateway_id = gateway_id.into();
            chirpstack_api::api::gateway_service_client::GatewayServiceClient::new(
                self.channel().await?,
            )
            .update(self.auth(chirpstack_api::api::UpdateGatewayRequest {
                gateway: Some(gateway),
            })?)
            .await?;
            return Ok(json!({}));
        }
        use chirpstack_api::api::{
            application_service_client::ApplicationServiceClient,
            device_profile_service_client::DeviceProfileServiceClient,
            device_service_client::DeviceServiceClient, tenant_service_client::TenantServiceClient,
        };

        if path.starts_with("/api/tenants/") {
            let id = path.trim_start_matches("/api/tenants/");
            let tenant = &body["tenant"];
            TenantServiceClient::new(self.channel().await?)
                .update(self.auth(chirpstack_api::api::UpdateTenantRequest {
                    tenant: Some(chirpstack_api::api::Tenant {
                        id: id.into(),
                        name: tenant["name"].as_str().unwrap_or_default().into(),
                        description: tenant["description"].as_str().unwrap_or_default().into(),
                        can_have_gateways: tenant["can_have_gateways"].as_bool().unwrap_or(true),
                        max_gateway_count: tenant["max_gateway_count"].as_u64().unwrap_or(0) as u32,
                        max_device_count: tenant["max_device_count"].as_u64().unwrap_or(0) as u32,
                        ..Default::default()
                    }),
                })?)
                .await?;
            return Ok(json!({}));
        }

        if path.starts_with("/api/applications/") {
            let id = path.trim_start_matches("/api/applications/");
            let application = &body["application"];
            ApplicationServiceClient::new(self.channel().await?)
                .update(
                    self.auth(chirpstack_api::api::UpdateApplicationRequest {
                        application: Some(chirpstack_api::api::Application {
                            id: id.into(),
                            name: application["name"].as_str().unwrap_or_default().into(),
                            description: application["description"]
                                .as_str()
                                .unwrap_or_default()
                                .into(),
                            tenant_id: application["tenant_id"].as_str().unwrap_or_default().into(),
                            ..Default::default()
                        }),
                    })?,
                )
                .await?;
            return Ok(json!({}));
        }

        if path.starts_with("/api/device-profiles/") {
            let id = path.trim_start_matches("/api/device-profiles/");
            let profile = &body["device_profile"];
            DeviceProfileServiceClient::new(self.channel().await?)
                .update(
                    self.auth(chirpstack_api::api::UpdateDeviceProfileRequest {
                        device_profile: Some(chirpstack_api::api::DeviceProfile {
                            id: id.into(),
                            tenant_id: profile["tenant_id"].as_str().unwrap_or_default().into(),
                            name: profile["name"].as_str().unwrap_or_default().into(),
                            description: profile["description"].as_str().unwrap_or_default().into(),
                            region: parse_region(profile["region"].as_str().unwrap_or_default())?,
                            region_config_id: profile["region_config_id"]
                                .as_str()
                                .unwrap_or_default()
                                .into(),
                            mac_version: chirpstack_api::common::MacVersion::Lorawan103.into(),
                            reg_params_revision: chirpstack_api::common::RegParamsRevision::A
                                .into(),
                            adr_algorithm_id: "default".into(),
                            supports_otaa: profile["supports_otaa"].as_bool().unwrap_or(true),
                            supports_class_b: profile["supports_class_b"]
                                .as_bool()
                                .unwrap_or(false),
                            supports_class_c: profile["supports_class_c"]
                                .as_bool()
                                .unwrap_or(false),
                            uplink_interval: profile["uplink_interval"].as_u64().unwrap_or(60)
                                as u32,
                            device_status_req_interval: profile["device_status_req_interval"]
                                .as_u64()
                                .unwrap_or(86400)
                                as u32,
                            ..Default::default()
                        }),
                    })?,
                )
                .await?;
            return Ok(json!({}));
        }

        if path.starts_with("/api/devices/") {
            let suffix = path.trim_start_matches("/api/devices/");
            let mut client = DeviceServiceClient::new(self.channel().await?);
            if suffix.ends_with("/keys") {
                let keys = &body["device_keys"];
                client
                    .update_keys(self.auth(chirpstack_api::api::UpdateDeviceKeysRequest {
                        device_keys: Some(chirpstack_api::api::DeviceKeys {
                            dev_eui: keys["dev_eui"].as_str().unwrap_or_default().into(),
                            nwk_key: keys["nwk_key"].as_str().unwrap_or_default().into(),
                            app_key: keys["app_key"].as_str().unwrap_or_default().into(),
                            ..Default::default()
                        }),
                    })?)
                    .await?;
            } else {
                let device = &body["device"];
                client
                    .update(
                        self.auth(chirpstack_api::api::UpdateDeviceRequest {
                            device: Some(chirpstack_api::api::Device {
                                dev_eui: suffix.into(),
                                name: device["name"].as_str().unwrap_or_default().into(),
                                description: device["description"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .into(),
                                application_id: device["application_id"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .into(),
                                device_profile_id: device["device_profile_id"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .into(),
                                join_eui: device["join_eui"].as_str().unwrap_or_default().into(),
                                skip_fcnt_check: false,
                                is_disabled: false,
                                tags: string_map(device, "tags"),
                                variables: HashMap::new(),
                            }),
                        })?,
                    )
                    .await?;
            }
            return Ok(json!({}));
        }

        Err(anyhow!("unsupported ChirpStack gRPC PUT path: {path}"))
    }
}

#[derive(Debug)]
pub struct ApiError(anyhow::Error);
impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self(error)
    }
}
impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": self.0.to_string()})),
        )
            .into_response()
    }
}

fn read_yaml<T: DeserializeOwned>(path: &Path) -> Result<T> {
    Ok(serde_yaml::from_slice(&std::fs::read(path)?)?)
}
fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn atomic_write_yaml<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temp = path.with_extension("yaml.tmp");
    std::fs::write(&temp, serde_yaml::to_string(value)?)?;
    std::fs::rename(temp, path)?;
    Ok(())
}

fn atomic_write_secret(path: &Path, value: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temp = path.with_extension("secret.tmp");
    let mut file = fs::File::create(&temp)?;
    file.write_all(value.as_bytes())?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temp, fs::Permissions::from_mode(0o600))?;
    }
    fs::rename(temp, path)?;
    Ok(())
}

fn atomic_write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temp = path.with_extension("json.tmp");
    std::fs::write(&temp, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(temp, path)?;
    Ok(())
}

#[cfg(test)]
mod grpc_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn plan_round_trips_yaml() {
        let plan = Plan {
            version: "v1".into(),
            tenant: Some(TenantSpec {
                name: "DATUM Lab".into(),
                description: "test".into(),
                can_have_gateways: true,
                max_gateway_count: 0,
                max_device_count: 0,
            }),
            ..Plan::default()
        };
        let encoded = serde_yaml::to_string(&plan).unwrap();
        let decoded: Plan = serde_yaml::from_str(&encoded).unwrap();
        assert_eq!(decoded.tenant.unwrap().name, "DATUM Lab");
    }

    #[test]
    fn state_is_written_atomically() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        let state = ProvisionState {
            tenant_id: Some("tenant-1".into()),
            ..Default::default()
        };
        atomic_write_json(&path, &state).unwrap();
        let loaded: ProvisionState = read_json(&path).unwrap();
        assert_eq!(loaded.tenant_id.as_deref(), Some("tenant-1"));
        assert!(!path.with_extension("json.tmp").exists());
    }

    #[test]
    fn unknown_references_are_rejected_before_device_calls() {
        let plan = Plan {
            tenant: Some(TenantSpec {
                name: "tenant".into(),
                description: String::new(),
                can_have_gateways: true,
                max_gateway_count: 0,
                max_device_count: 0,
            }),
            devices: vec![DeviceSpec {
                dev_eui: "0102030405060708".into(),
                name: "dev".into(),
                application: "missing".into(),
                device_profile: "missing".into(),
                description: String::new(),
                join_eui: None,
                app_key_env: None,
                nwk_key_env: None,
                tags: BTreeMap::new(),
            }],
            ..Default::default()
        };
        let error = validate_plan(&plan).unwrap_err().to_string();
        assert!(error.contains("unknown application"));
    }
}
