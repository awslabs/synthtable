//! # Prompts
//!
//! Implements the prompts for the CLI for the user to select the data they want to generate.
//! In a linear workflow it asks the user to select the type of data they want to generate, then
//! the database and table they want to generate data for.
use crate::get_glue_data::{self, *};
use crate::get_processing_job::{self, *};
use console::Term;
use dialoguer::{theme::ColorfulTheme, Select};
use std::convert::Into;
use std::error::Error;
use std::iter::Iterator;
use std::process::Command;
use std::str::FromStr;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter, EnumString};

#[derive(Debug, EnumIter, Display, PartialEq, EnumString)]
enum WorkFlowType {
    SingleTable,
    MultiTable,
    TimeSeries,
}

/// Clear screen
fn clear_screen() {
    Command::new("clear").status().unwrap();
}

/// Get the type of data to generate
fn select_workflow_type() -> Result<WorkFlowType, Box<dyn Error>> {
    let items = WorkFlowType::iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>();

    let selection = Select::with_theme(&ColorfulTheme::default())
        .items(&items)
        .default(0)
        .with_prompt("Select the type of data to generate:")
        .report(true)
        .interact_on_opt(&Term::stderr());

    // Get the selection and convert to WorkFlowType
    match selection {
        Ok(Some(index)) => Ok(WorkFlowType::from_str(items.get(index).unwrap()).unwrap()),
        Ok(None) => Err("No selection made".into()),
        Err(err) => Err(err.into()),
    }
}

///  Get the database to generate data for
async fn select_database_name() -> Result<GlueDatabase, Box<dyn Error>> {
    let items = get_glue_data::get_aws_glue_databases().await;
    assert!(items.len() > 0, "No databases found in any region");
    let selection = Select::with_theme(&ColorfulTheme::default())
        .items(&items.iter().map(|x| x.format_choice()).collect::<Vec<_>>())
        .default(0)
        .with_prompt("Select source database for your data:")
        .report(true)
        .interact_on_opt(&Term::stderr());

    match selection {
        Ok(Some(index)) => Ok(items.get(index).unwrap().clone()),
        Ok(None) => Err("No selection made".into()),
        Err(err) => Err(err.into()),
    }
}

/// Get the table to generate data for
async fn select_table_name(database: &GlueDatabase) -> Result<GlueTable, Box<dyn Error>> {
    let items = get_one_glue_table(database).await;
    assert!(items.len() > 0, "No tables found in database");
    let selection = Select::with_theme(&ColorfulTheme::default())
        .items(&items.iter().map(|x| x.format_choice()).collect::<Vec<_>>())
        .default(0)
        .with_prompt("Select source database for your data:")
        .report(true)
        .interact_on_opt(&Term::stderr());

    match selection {
        Ok(Some(index)) => Ok(items.get(index).unwrap().clone()),
        Ok(None) => Err("No selection made".into()),
        Err(err) => Err(err.into()),
    }
}

/// Get valid subnet to run the job in
async fn select_vpc_id(my_region: &str) -> Result<ValidSubnet, Box<dyn Error>> {
    let items = get_processing_job::get_subnet_list(my_region)
        .await
        .expect("Failed to get subnet list");
    let display_items = items
        .iter()
        .map(|x| x.format_for_display())
        .collect::<Vec<_>>();
    let selection = Select::with_theme(&ColorfulTheme::default())
        .items(&display_items)
        .default(0)
        .with_prompt("Select Subnet to run your job in:")
        .report(true)
        .interact_on_opt(&Term::stderr());

    match selection {
        Ok(Some(index)) => Ok(items.get(index).unwrap().clone()),
        Ok(None) => Err("No selection made".into()),
        Err(err) => Err(err.into()),
    }
}
/// Run the workflow for the user to select the data they want to generate
pub async fn run_workflow() -> Result<(), Box<dyn Error>> {
    clear_screen();
    match select_workflow_type().unwrap() {
        WorkFlowType::SingleTable => {
            // Get the database and table to generate data for
            let database = select_database_name()
                .await
                .expect("Failed to get database name");

            // Get the table to generate data for
            let table = select_table_name(&database)
                .await
                .expect("Failed to get table name");

            let valid_subnet = select_vpc_id(database.region())
                .await
                .expect("Failed to get subnet id");

            run_sythetic_data_job(&valid_subnet.get_subnet(), &table)
                .await
                .expect("Failed to create EC2 instance");
            Ok(())
        }
        WorkFlowType::MultiTable => {
            println!("Multi Table");
            Ok(())
        }
        WorkFlowType::TimeSeries => {
            println!("Time Series");
            Ok(())
        }
    }
}
