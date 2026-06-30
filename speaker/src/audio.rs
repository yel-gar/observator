use std::collections::HashMap;
use std::sync::Arc;
use anyhow::anyhow;
use common::constants::AUDIO_PACKET_BUFFER_SIZE;
use common::messages::VoicePacket;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{
    Device, FromSample, Sample, SampleFormat, SizedSample, Stream, StreamConfig, default_host,
};
use ringbuf::HeapRb;
use ringbuf::traits::{Consumer, Producer, Split};
use tokio::sync::RwLock;
use tracing::info;

pub type ReceiverList = Arc<RwLock<HashMap<u64, tokio::sync::mpsc::Receiver<VoicePacket>>>>;

async fn audio_mixer_loop(mut prod: impl Producer<Item = i16>, rx: ReceiverList) {
    loop {
        let mut receivers = rx.write().await;
        let mut vals = [0i16; AUDIO_PACKET_BUFFER_SIZE];
        for receiver in receivers.values_mut() {
            if let Ok(p) = receiver.try_recv() {
                vals.iter_mut().enumerate().for_each(|(i, v)| *v += p.packet[i])
            }
        }

        prod.push_slice(&vals);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

pub async fn init_audio(rx: ReceiverList) -> anyhow::Result<Stream> {
    let host = default_host();
    let device = host
        .default_output_device()
        .ok_or(anyhow!("No output device"))?;
    let conf = device.default_output_config()?;
    info!(?device, ?conf, "Audio backend initialized");

    let rb = HeapRb::<i16>::new(AUDIO_PACKET_BUFFER_SIZE * 2);
    let (prod, cons) = rb.split();
    
    tokio::spawn(audio_mixer_loop(prod, rx));
    
    let stream = match conf.sample_format() {
        SampleFormat::F32 => build_stream::<f32>(device, conf.into(), cons),
        SampleFormat::I16 => build_stream::<i16>(device, conf.into(), cons),
        SampleFormat::U16 => build_stream::<u16>(device, conf.into(), cons),
        _ => Err(anyhow!("Unsupported sample format")),
    }?;

    stream.play()?;
    info!("Started audio output");
    Ok(stream)
}

fn build_stream<T>(
    device: Device,
    conf: StreamConfig,
    mut cons: impl Consumer<Item = i16> + Send + 'static,
) -> anyhow::Result<Stream>
where
    T: Sample + SizedSample + FromSample<i16>,
{
    // we assume incoming data is mono, so we need to duplicate it
    let channels = conf.channels as usize;
    device
        .build_output_stream(
            conf,
            move |data: &mut [T], _| {
                for frame in data.chunks_mut(channels) {
                    let s = T::from_sample(cons.try_pop().unwrap_or(0));
                    for sample in frame {
                        *sample = s;
                    }
                }
            },
            |e| eprintln!("Stream error: {e}"),
            None,
        )
        .map_err(|e| anyhow!("Failed to build output stream: {e}"))
}
