#![cfg(not(target_arch = "wasm32"))]

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use dnet::{codecs::BincodeCodec, rpc::Consume, tcp::TcpTransport};
use futures::StreamExt;
use tokio::{net::TcpStream, spawn, time::sleep};

use crate::api::{Api, Consumer};

pub async fn run(address: &str) -> Result<()> {
    println!("Connecting to server...");
    let stream = TcpStream::connect(address).await?;
    let transport = TcpTransport::buffered(stream, BincodeCodec::default());

    let consumer = Consumer::consume(transport, Default::default(), Default::default());
    println!("Connected.");

    consumer.hello_world().await.unwrap();

    println!("2 + 3 = {}", consumer.add_numbers(2, 3).await.unwrap());

    sleep(Duration::from_secs(1)).await;

    println!("2 + 3 = {}", consumer.add_numbers(2, 3).await.unwrap());

    let consumer = Arc::new(consumer);

    let mut addition = consumer.add_numbers(2, 3);
    let aborter = addition.aborter();
    spawn(async move {
        sleep(Duration::from_secs(1)).await;
        println!("2 + 3 = {:?}", addition.await);
    });
    aborter.abort();

    println!("5 + 6 = {}", consumer.add_numbers(5, 6).await.unwrap());
    println!(
        "'abc' + 'df' = {}",
        consumer
            .concatenate_strings("abc".to_string(), "df".to_string())
            .await
            .unwrap()
    );
    println!("8 + 1 = {}", consumer.add_numbers(8, 1).await.unwrap());

    let mut time_stream = consumer.stream_time(Duration::from_secs(1)).await.unwrap();
    let aborter = time_stream.aborter();
    let consumer_clone = consumer.clone();
    spawn(async move {
        sleep(Duration::from_secs(2)).await;
        println!(
            "45 + 6 = {}",
            consumer_clone.add_numbers(45, 6).await.unwrap()
        );
        sleep(Duration::from_secs(5)).await;
        aborter.abort();
    });

    while let Some(time) = time_stream.next().await {
        println!("Time: {time}");
    }

    println!("2 + 3 = {}", consumer.add_numbers(2, 3).await.unwrap());

    Ok(())
}
