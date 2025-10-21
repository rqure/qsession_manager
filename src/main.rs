use anyhow::Result;
use log::info;
use qlib_rs::{EntityType, FieldType, StoreProxy};

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

    loop {
        
    }

    Ok(())
}
