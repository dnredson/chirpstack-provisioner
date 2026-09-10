//! HTTP/2 test double decoding real ChirpStack protobuf messages.
//! No Docker, LAN endpoint, or production credentials are used.
use super::*;
use axum::{
    body::Bytes,
    http::{HeaderMap, Uri},
    response::{IntoResponse, Response},
};
use chirpstack_api::api::*;
use prost::Message;
use tokio::{net::TcpListener, task::JoinHandle};

#[derive(Default)]
struct Database {
    tenants: BTreeMap<String, Tenant>,
    profiles: BTreeMap<String, DeviceProfile>,
    applications: BTreeMap<String, Application>,
    gateways: BTreeMap<String, Gateway>,
    devices: BTreeMap<String, Device>,
    keys: BTreeMap<String, DeviceKeys>,
    creates: usize,
    updates: usize,
    calls: Vec<String>,
    offsets: Vec<(String, u32)>,
    fail_code: Option<u32>,
    delay: Duration,
}
type SharedDatabase = Arc<Mutex<Database>>;

fn decode<M: Message + Default>(body: &[u8]) -> M {
    assert!(body.len() >= 5);
    assert_eq!(body[0], 0);
    let size = u32::from_be_bytes(body[1..5].try_into().unwrap()) as usize;
    assert_eq!(size, body.len() - 5);
    M::decode(&body[5..]).unwrap()
}

fn grpc<M: Message>(message: M) -> Response {
    let encoded = message.encode_to_vec();
    let mut frame = vec![0];
    frame.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    frame.extend(encoded);
    (
        [("content-type", "application/grpc"), ("grpc-status", "0")],
        frame,
    )
        .into_response()
}

fn grpc_error(code: u32) -> Response {
    (
        [
            ("content-type", "application/grpc".to_owned()),
            ("grpc-status", code.to_string()),
        ],
        Vec::<u8>::new(),
    )
        .into_response()
}

async fn mock_rpc(
    State(db): State<SharedDatabase>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match uri.path() {
        "/api.InternalService/Login" => {}
        "/api.InternalService/CreateApiKey" => {
            assert_eq!(
                headers.get("authorization").unwrap(),
                "Bearer bootstrap-jwt"
            );
        }
        _ => assert_eq!(headers.get("authorization").unwrap(), "Bearer test-token"),
    }
    assert!(headers.contains_key("grpc-timeout"));
    let mut db = db.lock().await;
    db.calls.push(uri.path().into());
    if let Some(code) = db.fail_code {
        return grpc_error(code);
    }
    if !db.delay.is_zero() {
        tokio::time::sleep(db.delay).await;
    }
    match uri.path() {
        "/api.InternalService/Login" => {
            let request = decode::<internal_bootstrap::LoginRequest>(&body);
            assert_eq!(request.email, "test-admin");
            assert_eq!(request.password, "test-password");
            grpc(internal_bootstrap::LoginResponse {
                jwt: "bootstrap-jwt".into(),
            })
        }
        "/api.InternalService/CreateApiKey" => {
            let key = decode::<internal_bootstrap::CreateApiKeyRequest>(&body)
                .api_key
                .unwrap();
            assert_eq!(key.name, "test-provisioner");
            assert!(key.is_admin);
            grpc(internal_bootstrap::CreateApiKeyResponse {
                id: "key-1".into(),
                token: "test-token".into(),
            })
        }

        "/api.TenantService/Create" => {
            let mut entity = decode::<CreateTenantRequest>(&body).tenant.unwrap();
            entity.id = format!("Tenant-{}", db.tenants.len());
            let id = entity.id.clone();
            assert!(!db.tenants.contains_key(&id), "duplicate create");
            db.tenants.insert(id.clone(), entity);
            db.creates += 1;
            grpc(CreateTenantResponse { id })
        }
        "/api.TenantService/Get" => {
            let request = decode::<GetTenantRequest>(&body);
            match db.tenants.get(&request.id) {
                Some(entity) => grpc(GetTenantResponse {
                    tenant: Some(entity.clone()),
                    ..Default::default()
                }),
                None => grpc_error(5),
            }
        }
        "/api.TenantService/Update" => {
            let entity = decode::<UpdateTenantRequest>(&body).tenant.unwrap();
            assert!(db.tenants.contains_key(&entity.id));
            db.tenants.insert(entity.id.clone(), entity);
            db.updates += 1;
            grpc(())
        }

        "/api.TenantService/List" => {
            let request = decode::<ListTenantsRequest>(&body);

            db.offsets.push(("Tenant".into(), request.offset));
            let selected: Vec<_> = db.tenants.values().collect();
            let total_count = selected.len() as u32;
            let result = selected
                .into_iter()
                .skip(request.offset as usize)
                .take(request.limit as usize)
                .map(|entity| TenantListItem {
                    id: entity.id.clone(),
                    name: entity.name.clone(),
                    ..Default::default()
                })
                .collect();
            grpc(ListTenantsResponse {
                total_count,
                result,
            })
        }

        "/api.ApplicationService/Create" => {
            let mut entity = decode::<CreateApplicationRequest>(&body)
                .application
                .unwrap();
            entity.id = format!("Application-{}", db.applications.len());
            let id = entity.id.clone();
            assert!(!db.applications.contains_key(&id), "duplicate create");
            db.applications.insert(id.clone(), entity);
            db.creates += 1;
            grpc(CreateApplicationResponse { id })
        }
        "/api.ApplicationService/Get" => {
            let request = decode::<GetApplicationRequest>(&body);
            match db.applications.get(&request.id) {
                Some(entity) => grpc(GetApplicationResponse {
                    application: Some(entity.clone()),
                    ..Default::default()
                }),
                None => grpc_error(5),
            }
        }
        "/api.ApplicationService/Update" => {
            let entity = decode::<UpdateApplicationRequest>(&body)
                .application
                .unwrap();
            assert!(db.applications.contains_key(&entity.id));
            db.applications.insert(entity.id.clone(), entity);
            db.updates += 1;
            grpc(())
        }

        "/api.ApplicationService/List" => {
            let request = decode::<ListApplicationsRequest>(&body);

            db.offsets.push(("Application".into(), request.offset));
            let selected: Vec<_> = db
                .applications
                .values()
                .filter(|entity| entity.tenant_id == request.tenant_id)
                .collect();
            let total_count = selected.len() as u32;
            let result = selected
                .into_iter()
                .skip(request.offset as usize)
                .take(request.limit as usize)
                .map(|entity| ApplicationListItem {
                    id: entity.id.clone(),
                    name: entity.name.clone(),
                    ..Default::default()
                })
                .collect();
            grpc(ListApplicationsResponse {
                total_count,
                result,
            })
        }

        "/api.DeviceProfileService/Create" => {
            let mut entity = decode::<CreateDeviceProfileRequest>(&body)
                .device_profile
                .unwrap();
            entity.id = format!("DeviceProfile-{}", db.profiles.len());
            let id = entity.id.clone();
            assert!(!db.profiles.contains_key(&id), "duplicate create");
            db.profiles.insert(id.clone(), entity);
            db.creates += 1;
            grpc(CreateDeviceProfileResponse { id })
        }
        "/api.DeviceProfileService/Get" => {
            let request = decode::<GetDeviceProfileRequest>(&body);
            match db.profiles.get(&request.id) {
                Some(entity) => grpc(GetDeviceProfileResponse {
                    device_profile: Some(entity.clone()),
                    ..Default::default()
                }),
                None => grpc_error(5),
            }
        }
        "/api.DeviceProfileService/Update" => {
            let entity = decode::<UpdateDeviceProfileRequest>(&body)
                .device_profile
                .unwrap();
            assert!(db.profiles.contains_key(&entity.id));
            db.profiles.insert(entity.id.clone(), entity);
            db.updates += 1;
            grpc(())
        }

        "/api.DeviceProfileService/List" => {
            let request = decode::<ListDeviceProfilesRequest>(&body);
            assert!(request.tenant_only);
            assert!(!request.global_only);
            db.offsets.push(("DeviceProfile".into(), request.offset));
            let selected: Vec<_> = db
                .profiles
                .values()
                .filter(|entity| entity.tenant_id == request.tenant_id)
                .collect();
            let total_count = selected.len() as u32;
            let result = selected
                .into_iter()
                .skip(request.offset as usize)
                .take(request.limit as usize)
                .map(|entity| DeviceProfileListItem {
                    id: entity.id.clone(),
                    name: entity.name.clone(),
                    ..Default::default()
                })
                .collect();
            grpc(ListDeviceProfilesResponse {
                total_count,
                result,
            })
        }

        "/api.GatewayService/Create" => {
            let entity = decode::<CreateGatewayRequest>(&body).gateway.unwrap();

            let id = entity.gateway_id.clone();
            assert!(!db.gateways.contains_key(&id), "duplicate create");
            db.gateways.insert(id.clone(), entity);
            db.creates += 1;
            grpc(())
        }
        "/api.GatewayService/Get" => {
            let request = decode::<GetGatewayRequest>(&body);
            match db.gateways.get(&request.gateway_id) {
                Some(entity) => grpc(GetGatewayResponse {
                    gateway: Some(entity.clone()),
                    ..Default::default()
                }),
                None => grpc_error(5),
            }
        }
        "/api.GatewayService/Update" => {
            let entity = decode::<UpdateGatewayRequest>(&body).gateway.unwrap();
            assert!(db.gateways.contains_key(&entity.gateway_id));
            db.gateways.insert(entity.gateway_id.clone(), entity);
            db.updates += 1;
            grpc(())
        }

        "/api.DeviceService/Create" => {
            let entity = decode::<CreateDeviceRequest>(&body).device.unwrap();

            let id = entity.dev_eui.clone();
            assert!(!db.devices.contains_key(&id), "duplicate create");
            db.devices.insert(id.clone(), entity);
            db.creates += 1;
            grpc(())
        }
        "/api.DeviceService/Get" => {
            let request = decode::<GetDeviceRequest>(&body);
            match db.devices.get(&request.dev_eui) {
                Some(entity) => grpc(GetDeviceResponse {
                    device: Some(entity.clone()),
                    ..Default::default()
                }),
                None => grpc_error(5),
            }
        }
        "/api.DeviceService/Update" => {
            let entity = decode::<UpdateDeviceRequest>(&body).device.unwrap();
            assert!(db.devices.contains_key(&entity.dev_eui));
            db.devices.insert(entity.dev_eui.clone(), entity);
            db.updates += 1;
            grpc(())
        }

        "/api.DeviceService/GetKeys" => {
            let request = decode::<GetDeviceKeysRequest>(&body);
            match db.keys.get(&request.dev_eui) {
                Some(keys) => grpc(GetDeviceKeysResponse {
                    device_keys: Some(keys.clone()),
                    ..Default::default()
                }),
                None => grpc_error(5),
            }
        }
        "/api.DeviceService/CreateKeys" => {
            let keys = decode::<CreateDeviceKeysRequest>(&body)
                .device_keys
                .unwrap();
            assert!(!db.keys.contains_key(&keys.dev_eui));
            db.keys.insert(keys.dev_eui.clone(), keys);
            db.creates += 1;
            grpc(())
        }
        "/api.DeviceService/UpdateKeys" => {
            let keys = decode::<UpdateDeviceKeysRequest>(&body)
                .device_keys
                .unwrap();
            assert!(db.keys.contains_key(&keys.dev_eui));
            db.keys.insert(keys.dev_eui.clone(), keys);
            db.updates += 1;
            grpc(())
        }
        "/api.InternalService/GetVersion" => grpc(GetVersionResponse::default()),
        path => panic!("unexpected gRPC method: {path}"),
    }
}

struct MockServer {
    db: SharedDatabase,
    client: ChirpStackClient,
    task: JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn server() -> MockServer {
    let db = Arc::new(Mutex::new(Database::default()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .fallback(post(mock_rpc))
        .with_state(db.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let config = ChirpStackConfig {
        endpoint,
        api_token_env: None,
        bootstrap: BootstrapAuthConfig::default(),
        timeout_seconds: 1,
    };
    MockServer {
        db,
        client: ChirpStackClient::new(&config, "test-token".into()).unwrap(),
        task,
    }
}

fn plan() -> Plan {
    serde_yaml::from_str(
        r#"
version: v1
tenant:
  name: DATUM Lab
device_profiles:
  - key: radio
    name: Brazilian lab
    region: AU915
    region_config_id: custom_au915
    supports_otaa: true
    uplink_interval: 60
applications:
  - key: app
    name: Sensors
gateways:
  - gateway_id: '0102030405060708'
    name: Mist gateway
    stats_interval: 30
devices:
  - dev_eui: '1122334455667788'
    name: Sensor
    application: app
    device_profile: radio
    join_eui: '0000000000000000'
    tags:
      source: simulator
"#,
    )
    .unwrap()
}

#[tokio::test]
async fn grpc_bootstrap_obtains_a_token_and_obeys_timeout() {
    let server = server().await;
    let token = ChirpStackClient::bootstrap_api_token(
        &server.client.endpoint,
        "test-admin",
        "test-password",
        "test-provisioner",
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert_eq!(token, "test-token");
    server.db.lock().await.delay = Duration::from_secs(10);
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        ChirpStackClient::bootstrap_api_token(
            &server.client.endpoint,
            "test-admin",
            "test-password",
            "test-provisioner",
            Duration::from_secs(1),
        ),
    )
    .await;
    assert!(result.expect("bootstrap must obey timeout").is_err());
}

#[tokio::test]
async fn unsupported_region_prevents_all_remote_mutation() {
    let server = server().await;
    let mut plan = plan();
    plan.device_profiles[0].region = Some("invalid".into());
    assert!(
        reconcile_plan(&server.client, &plan, &mut ProvisionState::default())
            .await
            .is_err()
    );
    assert!(server.db.lock().await.calls.is_empty());
}

#[tokio::test]
async fn grpc_creates_and_updates_full_plan_without_duplicates() {
    let server = server().await;
    let mut plan = plan();
    let mut state = ProvisionState::default();
    server.client.health().await.unwrap();
    reconcile_plan(&server.client, &plan, &mut state)
        .await
        .unwrap();
    let first = serde_json::to_value(&state).unwrap();
    {
        let db = server.db.lock().await;
        assert_eq!(db.creates, 5);
        assert_eq!(db.gateways.len(), 1);
        let profile = db.profiles.values().next().unwrap();
        assert_eq!(profile.region, parse_region("AU915").unwrap());
        assert_eq!(profile.region_config_id, "custom_au915");
        assert_eq!(
            db.devices.values().next().unwrap().tags["source"],
            "simulator"
        );
    }
    plan.gateways[0].name = "Updated gateway".into();
    plan.devices[0].tags.insert("stage".into(), "fog".into());
    plan.device_profiles[0].description = "Updated profile".into();
    reconcile_plan(&server.client, &plan, &mut state)
        .await
        .unwrap();
    {
        let db = server.db.lock().await;
        assert_eq!(db.creates, 5);
        assert_eq!(db.updates, 5);
        assert_eq!(db.gateways.values().next().unwrap().name, "Updated gateway");
        assert_eq!(db.devices.values().next().unwrap().tags["stage"], "fog");
        assert_eq!(
            db.profiles.values().next().unwrap().description,
            "Updated profile"
        );
    }
    let second = serde_json::to_value(&state).unwrap();
    for key in [
        "tenant_id",
        "applications",
        "device_profiles",
        "gateways",
        "devices",
    ] {
        assert_eq!(first[key], second[key]);
    }
    // Re-discovery without local state must reuse names and EUIs too.
    let mut recovered = ProvisionState::default();
    reconcile_plan(&server.client, &plan, &mut recovered)
        .await
        .unwrap();
    assert_eq!(server.db.lock().await.creates, 5);
    assert_eq!(recovered.tenant_id, state.tenant_id);
}

#[tokio::test]
async fn grpc_key_create_get_update_uses_official_messages() {
    let server = server().await;
    let path = "/api/devices/1122334455667788/keys";
    let mut body = json!({"device_keys": {
        "dev_eui": "1122334455667788",
        "nwk_key": "00000000000000000000000000000001",
        "app_key": "00000000000000000000000000000002"
    }});
    assert!(!server.client.exists(path).await.unwrap());
    server.client.post(path, body.clone()).await.unwrap();
    assert!(server.client.exists(path).await.unwrap());
    body["device_keys"]["app_key"] = json!("00000000000000000000000000000003");
    server.client.put(path, body).await.unwrap();
    let db = server.db.lock().await;
    assert_eq!(db.creates, 1);
    assert_eq!(db.updates, 1);
    assert_eq!(
        db.keys["1122334455667788"].app_key,
        "00000000000000000000000000000003"
    );
}

#[tokio::test]
async fn grpc_paginates_all_named_collections_and_reuses_last_page() {
    let server = server().await;
    let mut plan = plan();
    plan.gateways.clear();
    plan.devices.clear();
    {
        let mut db = server.db.lock().await;
        for n in 0..101 {
            let id = format!("{n:03}");
            db.tenants.insert(
                id.clone(),
                Tenant {
                    id: id.clone(),
                    name: if n == 100 {
                        "DATUM Lab".into()
                    } else {
                        id.clone()
                    },
                    ..Default::default()
                },
            );
            db.applications.insert(
                id.clone(),
                Application {
                    id: id.clone(),
                    tenant_id: "100".into(),
                    name: if n == 100 {
                        "Sensors".into()
                    } else {
                        id.clone()
                    },
                    ..Default::default()
                },
            );
            db.profiles.insert(
                id.clone(),
                DeviceProfile {
                    id: id.clone(),
                    tenant_id: "100".into(),
                    name: if n == 100 {
                        "Brazilian lab".into()
                    } else {
                        id.clone()
                    },
                    ..Default::default()
                },
            );
        }
    }
    let mut state = ProvisionState::default();
    reconcile_plan(&server.client, &plan, &mut state)
        .await
        .unwrap();
    assert_eq!(state.tenant_id.as_deref(), Some("100"));
    assert_eq!(state.applications["app"], "100");
    assert_eq!(state.device_profiles["radio"], "100");
    let db = server.db.lock().await;
    assert_eq!(db.creates, 0);
    for service in ["Tenant", "Application", "DeviceProfile"] {
        let offsets: Vec<_> = db
            .offsets
            .iter()
            .filter(|(name, _)| name == service)
            .map(|(_, offset)| *offset)
            .collect();
        assert_eq!(offsets, vec![0, 100]);
    }
}

#[tokio::test]
async fn grpc_errors_do_not_trigger_create() {
    let server = server().await;
    for code in [7, 14, 16] {
        {
            let mut db = server.db.lock().await;
            db.fail_code = Some(code);
            db.calls.clear();
        }
        assert!(
            reconcile_plan(&server.client, &plan(), &mut ProvisionState::default())
                .await
                .is_err()
        );
        assert!(
            ensure_gateway(&server.client, &plan().gateways[0], "tenant", None)
                .await
                .is_err()
        );
        let db = server.db.lock().await;
        assert_eq!(db.creates, 0);
        assert!(!db.calls.iter().any(|path| path.ends_with("/Create")));
    }
}

#[tokio::test]
async fn grpc_timeout_bounds_health_and_reconciliation() {
    let server = server().await;
    server.db.lock().await.delay = Duration::from_secs(10);
    let result = tokio::time::timeout(Duration::from_secs(3), server.client.health()).await;
    assert!(result
        .expect("health must obey the configured one-second timeout")
        .is_err());
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        reconcile_plan(&server.client, &plan(), &mut ProvisionState::default()),
    )
    .await;
    assert!(result
        .expect("reconciliation must obey the configured timeout")
        .is_err());
}

#[test]
fn regions_are_explicit_and_invalid_plans_fail_before_rpc() {
    for value in ["eu868", "US915", "us915_0", "au915_7", "AS923_2", "as923-4"] {
        assert!(parse_region(value).is_ok(), "{value}");
    }
    for value in ["unknown", "us915_8", "au915_evil", ""] {
        assert!(parse_region(value).is_err(), "{value}");
    }
    let mut plan = plan();
    plan.device_profiles[0].region = None;
    assert!(validate_plan(&plan).is_err());
    plan.device_profiles[0].region = Some("AU915".into());
    assert!(validate_plan(&plan).is_ok());
}

#[test]
fn incomplete_pagination_is_not_treated_as_resource_absence() {
    let mut result = vec![json!({"id":"1"})];
    assert!(append_page(&mut result, vec![], 2).is_err());
    assert!(append_page(&mut result, vec![json!({"id":"2"})], 2).unwrap());
}
