use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{
    Device, FromSample, Sample, SampleFormat, SizedSample, Stream, StreamConfig, default_host,
};
use hound::WavReader;

fn main() -> anyhow::Result<()> {
    let host = default_host();
    let device = host
        .default_output_device()
        .expect("No output device available");
    let mut conf = device.default_output_config()?;
    println!("Output config: {conf:?}");

    let mut reader = WavReader::open("example.wav")?;
    let samples: Vec<f32> = reader.samples::<f32>().map(|s| s.unwrap()).collect();
    let (tx, rx) = std::sync::mpsc::channel::<f32>();

    std::thread::spawn(move || {
        for s in samples {
            let _ = tx.send(s);
        }
    });

    let stream = match conf.sample_format() {
        SampleFormat::F32 => build_stream::<f32>(device, conf.into(), rx),
        SampleFormat::I16 => build_stream::<i16>(device, conf.into(), rx),
        SampleFormat::U16 => build_stream::<u16>(device, conf.into(), rx),
        _ => unimplemented!(),
    };

    stream.play()?;

    std::thread::sleep(std::time::Duration::from_secs(10));

    Ok(())
}

fn build_stream<T>(
    device: Device,
    config: StreamConfig,
    rx: std::sync::mpsc::Receiver<f32>,
) -> Stream
where
    T: Sample + SizedSample + FromSample<f32>,
{
    device
        .build_output_stream(
            config,
            move |data: &mut [T], _| {
                let mut i = 0;
                while i < data.len() - 1 {
                    if let Ok(s) = rx.try_recv() {
                        let sample = T::from_sample(s);

                        data[i] = sample;
                        data[i + 1] = sample;

                        i += 2
                    } else {
                        break;
                    }
                }
            },
            |e| eprintln!("{e}"),
            None,
        )
        .unwrap()
}
