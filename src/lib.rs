use connection_pool::{
    Connection as PooledConnection, ConnectionConnector, GenericConnectionPool, LiveConnection,
};
use lapin::{Channel, CloseOnDrop, Connection, ConnectionProperties};
use smol::block_on;
use std::ops::Deref;

struct RabbitMqConnectionWrapper(CloseOnDrop<Connection>);

pub struct ChannelWrapper(CloseOnDrop<Channel>);

impl PooledConnection for ChannelWrapper {
    fn is_alive(&self) -> bool {
        self.0.status().is_connected()
    }
}
impl PooledConnection for RabbitMqConnectionWrapper {
    fn is_alive(&self) -> bool {
        self.0.status().connected()
    }
}

#[derive(Clone)]
struct RabbitMqConnectionConnector {
    _connection_string: String,
}

#[derive(Clone)]
pub struct ChannelConnector {
    connection_pool: GenericConnectionPool<RabbitMqConnectionConnector>,
}

impl ConnectionConnector for ChannelConnector {
    type Conn = ChannelWrapper;
    fn connect(&self) -> Option<Self::Conn> {
        let connection: LiveConnection<_> = self.connection_pool.get_connection()?;
        let conn_wrapper: &RabbitMqConnectionWrapper = connection.deref();
        match block_on(conn_wrapper.0.create_channel()) {
            Ok(ch) => Some(ChannelWrapper(ch)),
            Err(reason) => {
                println!("error creating channel: {:?}", reason);
                None
            }
        }
    }
}

impl ConnectionConnector for RabbitMqConnectionConnector {
    type Conn = RabbitMqConnectionWrapper;
    fn connect(&self) -> Option<Self::Conn> {
        match block_on(Connection::connect(
            &self._connection_string,
            ConnectionProperties::default(),
        )) {
            Ok(conn) => Some(RabbitMqConnectionWrapper(conn)),
            Err(_) => None,
        }
    }
}

#[derive(Clone)]
pub struct RabbitMqManager {
    _channel_pool: GenericConnectionPool<ChannelConnector>,
}

impl RabbitMqManager {
    pub fn new(max_channels: u8, max_connections: u8, connection_string: String) -> Self {
        let conn_connector = RabbitMqConnectionConnector {
            _connection_string: connection_string,
        };

        let conn_pool = GenericConnectionPool::new(max_connections, conn_connector);
        let chan_connector = ChannelConnector {
            connection_pool: conn_pool,
        };

        let chan_pool = GenericConnectionPool::new(max_channels, chan_connector);
        Self {
            _channel_pool: chan_pool,
        }
    }
}

impl RabbitMqManager {
    pub fn get_channel(&self) -> Option<LiveConnection<ChannelConnector>> {
        self._channel_pool.get_connection()
    }
}
