use anyhow::Result;
use log::info;
use qlib_rs::app::ServiceState;
use qlib_rs::{EntityType, FieldType, NotifyConfig, StoreProxy};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use ctrlc;

struct ET {
    pub user: EntityType,
    pub session_controller: EntityType,
    pub session: EntityType,
}

impl ET {
    pub fn new(store: &mut StoreProxy) -> Result<Self> {
        Ok(Self {
            user: store.get_entity_type("User")?,
            session_controller: store.get_entity_type("SessionController")?,
            session: store.get_entity_type("Session")?,
        })
    }
}

struct FT {
    pub name: FieldType,
    pub current_user: FieldType,
    pub previous_user: FieldType,
    pub token: FieldType,
    pub expires_at: FieldType,
    pub created_at: FieldType,

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
            name: store.get_field_type("Name")?,

            current_user: store.get_field_type("CurrentUser")?,
            previous_user: store.get_field_type("PreviousUser")?,
            token: store.get_field_type("Token")?,
            expires_at: store.get_field_type("ExpiresAt")?,
            created_at: store.get_field_type("CreatedAt")?,

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

    while running.load(Ordering::SeqCst) {
        store.process_notifications()?;
        app_state.tick(&mut store)?;

        while let Ok(notification) = notify_receiver.try_recv() {
            if app_state.is_leader() {
                let field = notification.current.field_path[0].clone();

                if field == ft.request_login_user {
                } else if field == ft.request_logout_user {
                } else if field == ft.request_refresh_user {
                }
            }
        }

        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    app_state.make_me_unavailable(&mut store)?;

    Ok(())
}
