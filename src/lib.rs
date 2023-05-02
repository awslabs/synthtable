//! # Synthetic Tabular Data Generator
//!
//! This crate is a command-line tool for generating synthetic tabular data for use in prototyping and development on AWS. It provides a simple interface for creating data that can be used to develop and validate prototypes without exposing sensitive source data.
//!
//! This crate is a wrapper around the [Synthetic Data Vault (SDV)](https://github.com/sdv-dev/SDV) package, which provides more information on synthetic data.
//!
//! ## Usage on AWS from CloudShell
//! To use the `synthetic_data_generator` tool from CloudShell, run the following command:
//!
//! ```bash
//! ./synthetic_data_generator
//! ```
//!
//! Follow the prompts to generate data for a single table or multiple tables that are stored in AWS Glue.

// pub mod aws_common;
pub const PROJECT_NAME: &str = "SynthTable";
mod cw_logging;
mod get_glue_data;
mod get_processing_job;
mod manage_iam;
mod progress_tracker;
pub mod prompts;
