use anyhow::anyhow;
use common::constants::{AUDIO_PACKET_BUFFER_SIZE, VoicePacketBuf};
use common::messages::VoicePacket;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{
    Device, FromSample, Sample, SampleFormat, SizedSample, Stream, StreamConfig, default_host,
};
use tracing::{error, info};

pub fn init_audio_recorder(tx: tokio::sync::mpsc::Sender<VoicePacket>) -> anyhow::Result<Stream> {
    let host = default_host();
    let device = host
        .default_input_device()
        .ok_or(anyhow!("No default input device"))?;
    let conf = device.default_input_config()?;
    info!(?conf, "Starting audio recorder");

    let stream = match conf.sample_format() {
        SampleFormat::F32 => build_stream::<f32>(device, conf.into(), tx),
        SampleFormat::I16 => build_stream::<i16>(device, conf.into(), tx),
        SampleFormat::U16 => build_stream::<u16>(device, conf.into(), tx),
        _ => Err(anyhow!(
            "Unsupported sample format {:?}",
            conf.sample_format()
        )),
    }?;
    stream.play()?;

    Ok(stream)
}

fn build_stream<T>(
    device: Device,
    config: StreamConfig,
    tx: tokio::sync::mpsc::Sender<VoicePacket>,
) -> anyhow::Result<Stream>
where
    T: Sample + SizedSample,
    i16: FromSample<T>,
{
    let mut buf: VoicePacketBuf = [0; AUDIO_PACKET_BUFFER_SIZE];
    let mut cur = 0usize;
    device
        .build_input_stream(
            config,
            move |data: &[T], _| {
                for &sample in data {
                    let s = i16::from_sample(sample);
                    buf[cur] = s;
                    cur += 1;
                    if cur >= AUDIO_PACKET_BUFFER_SIZE {
                        let _ = tx.try_send(VoicePacket { packet: buf });
                        cur = 0;
                    }
                }
            },
            |e| {
                error!("Error when recording: {e}");
            },
            None,
        )
        .map_err(|e| anyhow!("Error initializing stream: {e}"))
}
