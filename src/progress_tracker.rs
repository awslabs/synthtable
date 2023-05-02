//! # progress_tracker
//! ProgressTracker is a simple wrapper around the `indicatif` crate
//! to provide a simple progress bar for the user to see the progress of the data generation job.
//! It grabs the last log line from CloudWatch and displays it to the user as the progress bar
use crate::cw_logging::CWLogSender;
use crate::get_glue_data::GlueTable;
use indicatif::{ProgressBar, ProgressStyle};
use std::process::Command;
use std::time::Duration;
#[derive(Debug, Clone, Copy)]

/// JobState is an enum to represent the state of the data generation job
/// Running: The job is still running
/// Completed: The job has completed successfully
/// Failed: The job has failed
pub enum JobState {
    Running,
    Completed,
    Failed,
}
/// This struct contains the progress bar and the CloudWatch logger to be used to update the progress bar
/// The delay_secs is the number of seconds to wait between each update to the progress bar
/// The database_name and table_name are used to display the current table being used as a data source for synthetic data generation
pub struct ProgressTracker {
    tracker: ProgressBar,
    logger: CWLogSender,
    delay_secs: u8,
    database_name: String,
    table_name: String,
    state: JobState,
}

impl ProgressTracker {
    /// Create a new ProgressTracker and customize the progress bar
    pub fn new(logger: CWLogSender, delay_secs: u8, glue_table: &GlueTable) -> ProgressTracker {
        let pb = ProgressBar::new_spinner();
        pb.enable_steady_tick(Duration::from_millis(120));
        pb.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] {spinner:.blue} {msg}")
                .unwrap()
                // For more spinners check out the cli-spinners project:
                // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
                .tick_strings(&[
                    "▹▹▹▹▹",
                    "▸▹▹▹▹",
                    "▹▸▹▹▹",
                    "▹▹▸▹▹",
                    "▹▹▹▸▹",
                    "▹▹▹▹▸",
                    "▪▪▪▪▪",
                ]),
        );
        // clear the screen
        Command::new("clear")
            .status()
            .expect("failed to clear screen");
        let tracker = ProgressTracker {
            logger,
            tracker: pb,
            delay_secs,
            database_name: glue_table.database().name().into(),
            table_name: glue_table.name().into(),
            state: JobState::Running,
        };
        // seed the progress bar with a message
        tracker
            .tracker
            .set_message("Starting Synthetic Data Generation Job ...".to_string());
        tracker
    }
    /// Return the current state of the job
    fn job_state(&self) -> JobState {
        self.state
    }

    /// Modify the state of the job
    fn set_state(&mut self, state: JobState) -> () {
        self.state = state;
    }

    /// Update the progress bar with the last log line from CloudWatch
    pub async fn update_progress(&mut self) -> JobState {
        // get the last log line from CloudWatch
        let last_log_line = self
            .logger
            .get_last_log_line()
            .await
            .expect("failed to get log line");

        // if the last log line is "Done" then the job is complete
        // This has to be coordinated with python code that runs the data generation job
        // found in src/scrprts/single_table.py
        if last_log_line.to_lowercase().eq("done") {
            self.set_state(JobState::Completed);
            self.finish();
            return self.job_state();
        // Same as above but for "Failed"
        } else if last_log_line.to_lowercase().contains("failed") {
            self.set_state(JobState::Failed);
            self.failed();
            return self.job_state();
        // Otherwise update the progress bar with the last log line
        } else {
            let message = format!(
                "Generating synthetic data for {}.{}: \n \t \t {}",
                self.database_name, self.table_name, last_log_line
            );
            self.tracker.set_message(message);
        }
        // sleep is required here as we dont want to be constantly polling CloudWatch
        // state changes are infrequent and we dont want to be charged for excessive API calls
        std::thread::sleep(Duration::from_secs(self.delay_secs.into()));
        self.job_state()
    }

    /// Finish the progress bar if Done and clear the screen
    pub fn finish(&self) -> () {
        self.tracker.finish_and_clear();
    }

    /// Finish the progress bar if Failed and clear the screen
    pub fn failed(&self) -> () {
        self.tracker.finish_and_clear();
    }
}
