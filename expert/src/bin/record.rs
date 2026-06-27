use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{
    Device, FromSample, Sample, SampleFormat, SizedSample, Stream, StreamConfig, default_host,
};
use hound::WavSpec;
use std::time::{Duration, Instant};

fn main() -> anyhow::Result<()> {
    let host = default_host();

    let device = host
        .default_input_device()
        .expect("No audio device available");

    let conf = device
        .default_input_config()
        .expect("No audio config available");
    println!("Using input config: {conf:?}");

    let (tx, rx) = std::sync::mpsc::channel::<f32>();

    let stream = match conf.sample_format() {
        SampleFormat::F32 => build_stream::<f32>(device, conf.into(), tx),
        SampleFormat::I16 => build_stream::<i16>(device, conf.into(), tx),
        SampleFormat::U16 => build_stream::<u16>(device, conf.into(), tx),
        _ => unimplemented!(),
    };

    stream.play()?;

    let duration = Duration::from_secs(10);
    let start = Instant::now();

    let mut samples = Vec::<f32>::new();

    while start.elapsed() < duration {
        if let Ok(sample) = rx.recv_timeout(Duration::from_millis(100)) {
            samples.push(sample);
        }
    }

    println!("Recorded {} samples", samples.len());
    write_wav("example.wav", &samples, conf.sample_rate())?;

    Ok(())
}

fn build_stream<T>(device: Device, config: StreamConfig, tx: std::sync::mpsc::Sender<f32>) -> Stream
where
    T: Sample + SizedSample,
    f32: FromSample<T>,
{
    device
        .build_input_stream(
            config,
            move |data: &[T], _| {
                for &sample in data {
                    let s: f32 = f32::from_sample(sample);
                    let _ = tx.send(s);
                }
            },
            |e| {
                eprintln!("{e}");
            },
            None,
        )
        .expect("Failed to build input stream")
}

fn write_wav(path: &str, samples: &[f32], sample_rate: u32) -> anyhow::Result<()> {
    let spec = WavSpec {
        channels: 1,
        sample_rate,
        bits_per_sample: 32,
        sample_format: hound::SampleFormat::Float,
    };

    let mut writer = hound::WavWriter::create(path, spec)?;

    for &s in samples {
        writer.write_sample(s)?;
    }

    writer.finalize()?;

    Ok(())
}
