use anyhow::Result;
use log::info;
use qlib_rs::app::ServiceState;
use qlib_rs::{EntityType, FieldType, NotifyConfig, StoreProxy, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use ctrlc;
use jsonwebtoken::{encode, Header, EncodingKey};

struct ET {
    pub session_controller: EntityType,
    pub session: EntityType,
}

impl ET {
    pub fn new(store: &mut StoreProxy) -> Result<Self> {
        Ok(Self {
            session_controller: store.get_entity_type("SessionController")?,
            session: store.get_entity_type("Session")?,
        })
    }
}

struct FT {
    pub current_user: FieldType,
    pub previous_user: FieldType,
    pub token: FieldType,
    pub expires_at: FieldType,
    pub created_at: FieldType,

    pub jwt_secret: FieldType,
    pub request_login_user: FieldType,
    pub response_login_user: FieldType,
    pub response_login_session: FieldType,

    pub request_logout_user: FieldType,
    pub response_logout_user: FieldType,
    pub response_logout_session: FieldType,

    pub request_refresh_user: FieldType,
    pub response_refresh_user: FieldType,
    pub response_refresh_session: FieldType,
}

impl FT {
    pub fn new(store: &mut StoreProxy) -> Result<Self> {
        Ok(Self {
            current_user: store.get_field_type("CurrentUser")?,
            previous_user: store.get_field_type("PreviousUser")?,
            token: store.get_field_type("Token")?,
            expires_at: store.get_field_type("ExpiresAt")?,
            created_at: store.get_field_type("CreatedAt")?,

            jwt_secret: store.get_field_type("JWTSecret")?,
            request_login_user: store.get_field_type("RequestLoginUser")?,
            response_login_user: store.get_field_type("ResponseLoginUser")?,
            response_login_session: store.get_field_type("ResponseLoginSession")?,

            request_logout_user: store.get_field_type("RequestLogoutUser")?,
            response_logout_user: store.get_field_type("ResponseLogoutUser")?,
            response_logout_session: store.get_field_type("ResponseLogoutSession")?,

            request_refresh_user: store.get_field_type("RequestRefreshUser")?,
            response_refresh_user: store.get_field_type("ResponseRefreshUser")?,
            response_refresh_session: store.get_field_type("ResponseRefreshSession")?,
        })
    }
}

fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let addrs = std::env::var("QCORE_ADDRESS")
        .unwrap_or_else(|_| "localhost:9100,qcore:9100".to_string())
        .split(",")
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>();

    let mut store = {
        let mut s = None;

        for addr in &addrs {
            match StoreProxy::connect(addr) {
                Ok(store) => {
                    info!("Connected to QCore at {}", addr);
                    s = Some(store);
                    break;
                }
                Err(_e) => {

                }
            }
        }

        s.expect(format!("Failed to connect to QCore at all provided addresses: {}", addrs.join(", ")).as_str())
    };

    let et = ET::new(&mut store)?;
    let ft = FT::new(&mut store)?;

    let mut app_state = ServiceState::new(
        &mut store,
        "qsession_manager".into(),
        true,
        1000)?;

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    let (notify_sender, notify_receiver) = crossbeam::channel::unbounded();

    // Find SessionController
    let session_controllers = store.find_entities(et.session_controller, None)?;
    if session_controllers.is_empty() {
        panic!("No SessionController found in store");
    }
    let session_controller_id = session_controllers[0];
    info!("Using SessionController: {:?}", session_controller_id);

    // Load and cache JWT secret
    let mut jwt_secret_cache = match store.read(session_controller_id, &[ft.jwt_secret]) {
        Ok((Value::String(secret), _, _)) => {
            info!("Loaded JWT secret from SessionController");
            secret
        }
        _ => {
            info!("Failed to read JWT secret, using default");
            "default_jwt_secret_change_in_production".to_string()
        }
    };

    // Register notification for JWT secret changes
    store.register_notification(NotifyConfig::EntityId {
        entity_id: session_controller_id,
        field_type: ft.jwt_secret,
        trigger_on_change: true,
        context: vec![],
    }, notify_sender.clone())?;

    store.register_notification(NotifyConfig::EntityType {
        entity_type: et.session_controller,
        field_type: ft.request_login_user,
        trigger_on_change: false,
        context: vec![],
    }, notify_sender.clone())?;

    store.register_notification(NotifyConfig::EntityType {
        entity_type: et.session_controller,
        field_type: ft.request_logout_user,
        trigger_on_change: false,
        context: vec![],
    }, notify_sender.clone())?;

    store.register_notification(NotifyConfig::EntityType {
        entity_type: et.session_controller,
        field_type: ft.request_refresh_user,
        trigger_on_change: false,
        context: vec![],
    }, notify_sender.clone())?;

    app_state.make_me_available(&mut store)?;

    let mut last_cleanup = std::time::Instant::now();

    while running.load(Ordering::SeqCst) {
        store.process_notifications()?;
        app_state.tick(&mut store)?;

        // Periodic session cleanup - run every 5 seconds
        if last_cleanup.elapsed() >= std::time::Duration::from_secs(5) {
            if app_state.is_leader() {
                cleanup_expired_sessions(&mut store, &et, &ft);
            }
            last_cleanup = std::time::Instant::now();
        }

        while let Ok(notification) = notify_receiver.try_recv() {
            let field = notification.current.field_path[0].clone();

            // Handle JWT secret updates (regardless of leader status)
            if field == ft.jwt_secret {
                if let Some(Value::String(secret)) = &notification.current.value {
                    jwt_secret_cache = secret.clone();
                    info!("JWT secret updated in cache");
                }
                continue;
            }

            if app_state.is_leader() {

                if field == ft.request_login_user {
                    let user_id = match &notification.current.value {
                        Some(Value::EntityReference(Some(id))) => *id,
                        _ => {
                            info!("Invalid login request: missing user_id");
                            continue;
                        }
                    };
                    
                    info!("Processing login request for user {:?}", user_id);
                    
                    // Find available sessions (CurrentUser is None)
                    let sessions = match store.find_entities(et.session, None) {
                        Ok(s) => s,
                        Err(e) => {
                            info!("Failed to find sessions: {:?}", e);
                            continue;
                        }
                    };
                    
                    let mut allocated = false;
                    for session in sessions {
                        let (val, _, _) = match store.read(session, &[ft.current_user]) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        
                        if val == Value::EntityReference(None) {
                            // Session is available - allocate it
                            let now = qlib_rs::now();
                            let expires = now + Duration::from_secs(60); // 1 minute
                            
                            // Generate JWT token using cached secret
                            let expiration = chrono::Utc::now() + chrono::Duration::seconds(60);
                            let claims = serde_json::json!({
                                "sub": user_id.0.to_string(),
                                "session_id": session.0.to_string(),
                                "exp": expiration.timestamp() as usize,
                            });
                            let token = match encode(&Header::default(), &claims, &EncodingKey::from_secret(jwt_secret_cache.as_bytes())) {
                                Ok(t) => t,
                                Err(e) => {
                                    info!("Failed to generate JWT token: {:?}", e);
                                    continue;
                                }
                            };
                            
                            let mut pipeline = store.pipeline();
                            pipeline.write(session, &[ft.current_user], Value::EntityReference(Some(user_id)), None, None, None, None).unwrap();
                            pipeline.write(session, &[ft.token], Value::String(token), None, None, None, None).unwrap();
                            pipeline.write(session, &[ft.expires_at], Value::Timestamp(expires), None, None, None, None).unwrap();
                            pipeline.write(session, &[ft.created_at], Value::Timestamp(now), None, None, None, None).unwrap();
                            pipeline.write(notification.current.entity_id, &[ft.response_login_user], Value::EntityReference(Some(user_id)), None, None, None, None).unwrap();
                            pipeline.write(notification.current.entity_id, &[ft.response_login_session], Value::EntityReference(Some(session)), None, None, None, None).unwrap();
                            
                            if let Err(e) = pipeline.execute() {
                                info!("Failed to allocate session: {:?}", e);
                                continue;
                            }
                            
                            info!("Allocated session {:?} for user {:?} with JWT", session, user_id);
                            allocated = true;
                            break;
                        }
                    }
                    
                    if !allocated {
                        info!("No available sessions for user {:?}", user_id);
                    }
                } else if field == ft.request_logout_user {
                    let user_id = match &notification.current.value {
                        Some(Value::EntityReference(Some(id))) => *id,
                        _ => {
                            info!("Invalid logout request: missing user_id");
                            continue;
                        }
                    };
                    
                    info!("Processing logout request for user {:?}", user_id);
                    
                    let sessions = match store.find_entities(et.session, None) {
                        Ok(s) => s,
                        Err(e) => {
                            info!("Failed to find sessions: {:?}", e);
                            continue;
                        }
                    };
                    
                    for session in sessions {
                        let (val, _, _) = match store.read(session, &[ft.current_user]) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        
                        if let Value::EntityReference(Some(id)) = val {
                            if id == user_id {
                                let mut pipeline = store.pipeline();
                                pipeline.write(session, &[ft.previous_user], Value::EntityReference(Some(user_id)), None, None, None, None).unwrap();
                                pipeline.write(session, &[ft.current_user], Value::EntityReference(None), None, None, None, None).unwrap();
                                pipeline.write(session, &[ft.token], Value::String("".into()), None, None, None, None).unwrap();
                                pipeline.write(session, &[ft.expires_at], Value::Timestamp(qlib_rs::epoch()), None, None, None, None).unwrap();
                                pipeline.write(notification.current.entity_id, &[ft.response_logout_user], Value::EntityReference(Some(user_id)), None, None, None, None).unwrap();
                                pipeline.write(notification.current.entity_id, &[ft.response_logout_session], Value::EntityReference(Some(session)), None, None, None, None).unwrap();
                                
                                if let Err(e) = pipeline.execute() {
                                    info!("Failed to logout session: {:?}", e);
                                    continue;
                                }
                                
                                info!("Logged out user {:?} from session {:?}", user_id, session);
                                break;
                            }
                        }
                    }
                } else if field == ft.request_refresh_user {
                    let user_id = match &notification.current.value {
                        Some(Value::EntityReference(Some(id))) => *id,
                        _ => {
                            info!("Invalid refresh request: missing user_id");
                            continue;
                        }
                    };
                    
                    info!("Processing refresh request for user {:?}", user_id);
                    
                    let sessions = match store.find_entities(et.session, None) {
                        Ok(s) => s,
                        Err(e) => {
                            info!("Failed to find sessions: {:?}", e);
                            continue;
                        }
                    };
                    
                    for session in sessions {
                        let (val, _, _) = match store.read(session, &[ft.current_user]) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        
                        if let Value::EntityReference(Some(id)) = val {
                            if id == user_id {
                                let now = qlib_rs::now();
                                let expires = now + Duration::from_secs(60); // 1 minute
                                
                                // Generate new JWT token using cached secret
                                let expiration = chrono::Utc::now() + chrono::Duration::seconds(60);
                                let claims = serde_json::json!({
                                    "sub": user_id.0.to_string(),
                                    "session_id": session.0.to_string(),
                                    "exp": expiration.timestamp() as usize,
                                });
                                let token = match encode(&Header::default(), &claims, &EncodingKey::from_secret(jwt_secret_cache.as_bytes())) {
                                    Ok(t) => t,
                                    Err(e) => {
                                        info!("Failed to generate JWT token: {:?}", e);
                                        continue;
                                    }
                                };
                                
                                let mut pipeline = store.pipeline();
                                pipeline.write(session, &[ft.token], Value::String(token), None, None, None, None).unwrap();
                                pipeline.write(session, &[ft.expires_at], Value::Timestamp(expires), None, None, None, None).unwrap();
                                pipeline.write(notification.current.entity_id, &[ft.response_refresh_user], Value::EntityReference(Some(user_id)), None, None, None, None).unwrap();
                                pipeline.write(notification.current.entity_id, &[ft.response_refresh_session], Value::EntityReference(Some(session)), None, None, None, None).unwrap();
                                
                                if let Err(e) = pipeline.execute() {
                                    info!("Failed to refresh session: {:?}", e);
                                    continue;
                                }
                                
                                info!("Refreshed session {:?} for user {:?} with new JWT", session, user_id);
                                break;
                            }
                        }
                    }
                }
            }
        }

        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    app_state.make_me_unavailable(&mut store)?;

    Ok(())
}

/// Clean up expired sessions by clearing CurrentUser, Token, and resetting ExpiresAt
fn cleanup_expired_sessions(store: &mut StoreProxy, et: &ET, ft: &FT) {
    let now = qlib_rs::now();
    
    // Find all sessions
    let sessions = match store.find_entities(et.session, None) {
        Ok(s) => s,
        Err(e) => {
            info!("Failed to find sessions during cleanup: {:?}", e);
            return;
        }
    };
    
    for session in sessions {
        // Check if session has a CurrentUser (is active)
        let current_user = match store.read(session, &[ft.current_user]) {
            Ok((Value::EntityReference(Some(user_id)), _, _)) => Some(user_id),
            _ => None,
        };
        
        if current_user.is_none() {
            // Session not in use, skip
            continue;
        }
        
        // Check if session has expired
        let expires_at = match store.read(session, &[ft.expires_at]) {
            Ok((Value::Timestamp(timestamp), _, _)) => timestamp,
            _ => continue,
        };
        
        if now > expires_at {
            // Session has expired, clean it up
            info!("Cleaning up expired session {:?} (user: {:?})", session, current_user);
            
            let mut pipeline = store.pipeline();
            
            // Save CurrentUser to PreviousUser
            if let Some(user_id) = current_user {
                let _ = pipeline.write(session, &[ft.previous_user], Value::EntityReference(Some(user_id)), None, None, None, None);
            }
            
            // Clear the session
            let _ = pipeline.write(session, &[ft.current_user], Value::EntityReference(None), None, None, None, None);
            let _ = pipeline.write(session, &[ft.token], Value::String("".to_string()), None, None, None, None);
            let _ = pipeline.write(session, &[ft.expires_at], Value::Timestamp(qlib_rs::epoch()), None, None, None, None);
            
            if let Err(e) = pipeline.execute() {
                info!("Failed to cleanup expired session {:?}: {:?}", session, e);
            } else {
                info!("Successfully cleaned up expired session {:?}", session);
            }
        }
    }
}
