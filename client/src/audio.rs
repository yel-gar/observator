use anyhow::anyhow;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{
    Device, FromSample, Sample, SampleFormat, SizedSample, Stream, StreamConfig, default_host,
};
use tracing::info;

pub fn init_audio(rx: tokio::sync::mpsc::Receiver<i16>) -> anyhow::Result<Stream> {
    let host = default_host();
    let device = host
        .default_output_device()
        .ok_or(anyhow!("No output device"))?;
    let conf = device.default_output_config()?;
    info!(?device, ?conf, "Audio backend initialized");

    let stream = match conf.sample_format() {
        SampleFormat::F32 => build_stream::<f32>(device, conf.into(), rx),
        SampleFormat::I16 => build_stream::<i16>(device, conf.into(), rx),
        SampleFormat::U16 => build_stream::<u16>(device, conf.into(), rx),
        _ => Err(anyhow!("Unsupported sample format")),
    }?;

    stream.play()?;
    info!("Started audio output");
    Ok(stream)
}

fn build_stream<T>(
    device: Device,
    conf: StreamConfig,
    mut rx: tokio::sync::mpsc::Receiver<i16>,
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
                    let s = T::from_sample(rx.try_recv().unwrap_or(0i16));
                    for out in frame {
                        *out = s;
                    }
                }
            },
            |e| eprintln!("Stream error: {e}"),
            None,
        )
        .map_err(|e| anyhow!("Failed to build output stream: {e}"))
}
