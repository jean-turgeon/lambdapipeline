//! Demonstrate how to build in Rust using AWS API a Lambda.
//!
//! This Lambda expect an event from S3 with a bucket and key name.
//! It will fetch the file and load it to a DataFrame, then do the transformation.
//! Once done will write back to S3 in the output bucket with a calculated key.
//!
//!
//!
//!
use aws_config::SdkConfig;
use aws_config::meta::region::RegionProviderChain;

use aws_sdk_s3::{Client, Region};
use aws_sdk_s3::client::fluent_builders::GetObject;
use aws_sdk_s3::output::GetObjectOutput;
use aws_sdk_s3::types::AggregatedBytes;

use aws_lambda_events::bytes::Bytes;
use aws_lambda_events::s3::{S3Entity, S3Event, S3EventRecord};

use lambda_runtime::{run, service_fn, Error, LambdaEvent};

use std::borrow::Cow;
use std::io::Cursor;

use polars::prelude::*;












/// Lambda response data structure.
///
#[derive(Serialize)]
struct Response {
    req_id:String,
    bucket:String,
    key:String,
    msg:String,
}






/// Download a S3 object given it's key and bucket location, then loads it to a DataFrame.
///
pub async fn read_s3_object(s3_client:Client, bucket:&str, key:&str) -> Result<DataFrame, anyhow::Error> {
    tracing::info!("bucket:      {}", bucket);
    tracing::info!("key:      {}", key);

	let request:GetObject = s3_client.get_object().bucket(bucket).key(key);
	let response:GetObjectOutput = request.clone().send().await;

	match response {
		Ok(response) => {
			tracing::info!("S3 Get Object success, the s3GetObjectResult contains a 'body' property of type ByteStream")

			let bytes_sequence:AggregatedBytes = response.body.collect().await.unwrap();
			let bytes:Bytes = bytes_sequence.into_bytes();

			tracing::info!("Object is downloaded, size is {}", bytes.len());

			let cursor:Cursor<Bytes> = Cursor::new(bytes);
			let df:DataFrame = CsvReader::new(cursor).finish().unwrap();

			Ok(df)
		},
		Err(err) => tracing::info!("Failure with S3 Get Object request")
	}
}



pub async fn write_s3_object(s3_client:&Client, bucket:&str, key:&str, data:&DataFrame) -> Result<PutObjectOutput, SdkError<PutObjectError>> {

	//let csv:String = data.write_csv(include_header=true, separator=",", line_terminator="\n",)

    let csv:String = CsvWriter::new()
        .has_headers(true)
        .with_separator(",")
        .with_quote_char('"')
        .with_line_terminator("\n")
        .finish(data)
        .unwrap();

    // AWS body
    let body:ByteStream = ByteStream::from(csv).await;


	return s3_client.put_object()
        .bucket(bucket)
        .key(key)
        .body(body.unwrap())
        .send()
        .await
}


/// Transform data here.
///
pub fn transform(mut data:&DataFrame) -> DataFrame {
	tracing::info!("{}", data.head(Some(5)));

	return data
}




/// Lambda handler function is called when an S3Event occur. The function is
/// triggered and given an S3 client and the event object. The function terminate
/// an return either a Response object or an error.
///
pub async fn handler(s3_client: &Client, event: LambdaEvent<S3Event>,) -> Result<(), Error> {

    let start_time: Instant = Instant::now();

    if event.payload.records.len() == 0 {
		tracing::info!("Empty S3 event received");
	}
	else {
		tracing::info!(records = ?event.payload.records.len(), "Received request from SQS");
	}

	let s3_event:Option<S3Entity> = event.payload.records.first().map(|event: &S3EventRecord| event.clone().s3);


	if let Some(s3_event) = s3_event {

		let bucket:String = s3_event.bucket.name.unwrap();
		let key:String = s3_event.object.key.unwrap();

		let output_bucket:String = env!("OUTPUT_S3_BUCKET", "OUTPUT_S3_BUCKET is not set");


		tracing::info!("Request is for {} and object {}", bucket, key);

		// Fetch CSV file from S3 and load to DataFrame
		let mut data:DataFrame  = read_s3_object(s3_client, &bucket, &key);

		// Do ETL transformation
		data = transform(&data);

		// Prepare output key
		let output_key:String = "";

		// Write DataFrame to S3
		let result:Result<PutObjectOutput, SdkError<PutObjectError>> = write_s3_object(s3_client:, &output_bucket, &output_key, &data);

       // End the timer
       let elapsed_time:Duration = start_time.elapsed();

        match result {
			Ok(result) => {
				tracing::info!("Completed Processing Data, {:?} seconds!", elapsed_time);

				// prepare the response
				let response = Response {
					req_id: event.context.request_id,
					msg: format!("Completed Processing Data!"),
				};

				Ok(response)
			},
			Err(err) => {
				tracing::info!("SDK Error: {}", err);
				Err(err)
			},
		}
	}
	Ok(())
}




/// Main function
///
#[tokio::main]
async fn main() -> Result<(), Error> {
	// Required to enable CloudWatch error logging by the runtime
    tracing_subscriber::fmt()
        // this needs to be set to false, otherwise ANSI color codes will
        // show up in a confusing manner in CloudWatch logs.
        .with_ansi(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        // remove the name of the function from every log entry
        .with_target(false)
        // this needs to be set to remove duplicated information in the log.
        .with_current_span(false)
        .with_max_level(tracing_subscriber::filter::LevelFilter::DEBUG)
        .init();

	// Initialize a AWS S3 client
    let region_name:&str = env!("AWS_REGION", "AWS_REGION is not set");;
    let region:Region = Region::new(Cow::Borrowed(region_name));
    let region_provider:RegionProviderChain = RegionProviderChain::default_provider().or_else(region);

    let config:SdkConfig = aws_config::load_from_env().region(region_provider).await;
    let s3_client:Client = Client::new(&config);


	// call the actual handler of the request
    run(service_fn(|event: LambdaEvent<S3Event>| {
        handler(&s3_client, event)
    }))
    .await
}

