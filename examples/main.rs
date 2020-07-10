use lapin::Result;
use rabbitmq_manager_sync::*;
use std::thread;
use std::time::Duration;

fn main() -> Result<()> {
    let connection_string = "amqp://guest:guest@127.0.0.1:5672/%2f".to_string();

    let manager = RabbitMqManager::new(3, 1, connection_string);

    for _ in 0..100 {
        let m = manager.clone();
        thread::spawn(move || {
            let chan = m.get_channel();
            if let None = chan {
                println!("could not get channel");
                return;
            }
            println!("created channel");
            thread::sleep(Duration::from_secs(1));
        });
    }
    thread::sleep(Duration::from_secs(120));

    Ok(())
}
