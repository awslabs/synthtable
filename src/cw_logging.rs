//! # cw_logging
//! This module contains functions to simplify communication with AWS CloudWatch logs.
//! EC2 instance, python script and bash script all use the same logging group and log stream.
//! The log group is called "SytheticData" and the log stream is the name of the AWS Glue table
//! that is being processed.
use crate::PROJECT_NAME;
use aws_sdk_cloudwatchlogs::model::InputLogEvent;
use aws_sdk_cloudwatchlogs::{Client, Error, Region};
use chrono::Local;

/// Create logger to send logs to cloudwatch from CLI
pub struct CWLogSender {
    region_name: String,
    log_group_name: String,
    log_stream_name: String,
}

/// Get cloudwatchlogs client for the region specified in the environment or default region
impl CWLogSender {
    // Here we also create AWS client for cloudwatch logs as part of initialization
    pub async fn new(region_name: String, log_stream_name: String) -> Self {
        let _logging = set_up_cw_logging(PROJECT_NAME, &log_stream_name, &region_name)
            .await
            .expect("Could not set up logging");
        let logger = CWLogSender {
            region_name,
            log_group_name: PROJECT_NAME.to_string(),
            log_stream_name,
        };
        logger
            .send_log("Setting up logging ...")
            .await
            .expect("Could not send log");
        logger
    }
    /// Getting last log line from cloudwatch logs to provide feedback to user
    pub async fn get_last_log_line(&self) -> Result<String, Error> {
        let client = get_cloudwatchlogs_client(&self.region_name)
            .await
            .expect("Could not get client");
        // get last log line
        let last_log_line = client
            .get_log_events()
            .log_group_name(&self.log_group_name)
            .log_stream_name(&self.log_stream_name)
            .start_from_head(false)
            .send()
            .await
            .expect("failed to get log events")
            .events
            .unwrap()
            .iter()
            .rev()
            .take(1)
            .map(|event| event.clone().message.unwrap())
            .collect::<Vec<String>>()
            .join("");
        Ok(last_log_line)
    }

    /// Send log message to cloudwatch logs
    pub async fn send_log(&self, message: &str) -> Result<(), Error> {
        let client = get_cloudwatchlogs_client(&self.region_name)
            .await
            .expect("Could not get client");

        let message = InputLogEvent::builder()
            .message(message)
            .timestamp(Local::now().timestamp_millis())
            .build();
        // send log message to cloudwatch
        let _response = &client
            .put_log_events()
            .log_group_name(&self.log_group_name)
            .log_stream_name(&self.log_stream_name)
            .log_events(message)
            .send()
            .await
            .expect("Could not send log message");
        Ok(())
    }
}

/// Create log group if it does not exist. Log group is called "SytheticData"
async fn create_log_group(log_group_name: &str, region: &str) -> Result<(), Error> {
    let client = get_cloudwatchlogs_client(region)
        .await
        .expect("Could not get client");

    let _response = &client
        .create_log_group()
        .log_group_name(log_group_name)
        .send()
        .await
        .expect("Could not create log group");

    Ok(())
}

/// Create log stream if it does not exist. Log stream is the name of the AWS Glue table
async fn create_log_stream(
    log_group_name: &str,
    log_stream_name: &str,
    region: &str,
) -> Result<(), Error> {
    let client = get_cloudwatchlogs_client(region)
        .await
        .expect("Could not get client");

    let _response = &client
        .create_log_stream()
        .log_group_name(log_group_name)
        .log_stream_name(log_stream_name)
        .send()
        .await
        .expect("Could not create log stream");

    Ok(())
}

/// Set up cloudwatch client
async fn get_cloudwatchlogs_client(region: &str) -> Result<Client, Error> {
    let config = aws_config::from_env()
        .region(Region::new(region.to_string()))
        .load()
        .await;
    Ok(Client::new(&config))
}

/// Checks if log group allready exists
async fn log_group_exists(log_group_name: &str, region: &str) -> Result<bool, Error> {
    let client = get_cloudwatchlogs_client(region)
        .await
        .expect("Could not get client");
    // check if log group exists check for exact match
    let log_group_exists = client
        .describe_log_groups()
        .log_group_name_prefix(log_group_name)
        .send()
        .await
        .expect("Could not get log groups")
        .log_groups
        .unwrap()
        .iter()
        .any(|log_group| log_group.log_group_name.as_ref().unwrap() == log_group_name);
    Ok(log_group_exists)
}

/// Checks if log stream exists
async fn log_stream_exists(
    log_group_name: &str,
    log_stream_name: &str,
    region: &str,
) -> Result<bool, Error> {
    let client = get_cloudwatchlogs_client(region)
        .await
        .expect("Could not get client");
    // check if log stream exists check for exact match
    let log_stream_exists = client
        .describe_log_streams()
        .log_group_name(log_group_name)
        .log_stream_name_prefix(log_stream_name)
        .send()
        .await
        .expect("Could not get log streams")
        .log_streams
        .unwrap()
        .iter()
        .any(|log_stream| log_stream.log_stream_name.as_ref().unwrap() == log_stream_name);
    Ok(log_stream_exists)
}

/// Set up cloudwatch logging
pub async fn set_up_cw_logging(
    log_group_name: &str,
    log_stream_name: &str,
    region: &str,
) -> Result<(), Error> {
    // check if log group exists is FALSE create log group
    if !log_group_exists(log_group_name, region).await? {
        create_log_group(log_group_name, region)
            .await
            .expect("Could not create log group");
    }
    // check if log stream exists is FALSE create log stream
    if !log_stream_exists(log_group_name, log_stream_name, region).await? {
        create_log_stream(log_group_name, log_stream_name, region)
            .await
            .expect("Could not create log stream");
    }
    // wait for log group and log stream to be ready
    loop {
        // check if log group and log stream exists
        let is_log_stream_ready = log_group_exists(log_group_name, region).await?
            && log_stream_exists(log_group_name, log_stream_name, region).await?;
        if is_log_stream_ready {
            break;
        } else {
            println!("Waiting for log group and log stream to be ready...");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
    Ok(())
}
