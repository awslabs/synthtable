//! # get_glue_data
//! This module contains functions to get data from AWS Glue for the CLI.
//! The CLI uses the AWS Glue API to get a list of all AWS Glue databases and tables.
//! The user can then select a database and table to process.
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_ec2::{Client as EC2_Client, Error};
use aws_sdk_glue::Client;
use aws_sdk_sts::Client as StsClient;
use aws_types::region::Region;

/// Returns ec2 client for the region specified in the environment or default region
async fn get_ec2_client() -> Result<EC2_Client, Error> {
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::from_env().region(region_provider).load().await;
    let client = EC2_Client::new(&config);
    Ok(client)
}

/// Returns a list of all allowed AWS regions
pub async fn get_all_regions() -> Result<Vec<String>, Error> {
    let client = get_ec2_client().await.expect("failed to get ec2 client");

    // Get all regions
    let regions = client
        .describe_regions()
        .all_regions(false)
        .send()
        .await
        .expect("failed to get regions");

    // Return a list of region names
    Ok(regions
        .regions
        .unwrap()
        .into_iter()
        .map(|region| region.region_name.unwrap())
        .collect())
}

/// Glue Database struct to hold database name and region
#[derive(Clone)]
pub struct GlueDatabase {
    region: String,
    name: String,
    account_id: String,
}

/// Glue Table struct to hold table name and database
#[derive(Clone)]
pub struct GlueTable {
    database: GlueDatabase, // We need to keep the database to get the region
    name: String,
    s3_location: String,
}
/// Glue Table convinience struct to hold table name and database
impl GlueTable {
    pub async fn new(database: GlueDatabase, name: String) -> Self {
        let mut glue_table = GlueTable {
            database,
            name,
            s3_location: String::new(),
        };

        glue_table.set_table_location().await;

        glue_table
    }
    pub fn s3_location(&self) -> &String {
        &self.s3_location
    }
    pub fn database(&self) -> &GlueDatabase {
        &self.database
    }
    pub fn name(&self) -> &String {
        &self.name
    }
    pub fn format_choice(&self) -> String {
        format!("{}", self.name)
    }
    pub fn s3_arn(&self) -> String {
        self.s3_location
            .replace("s3://", "arn:aws:s3:::")
            .trim_end_matches(|c| c == '/')
            .to_string()
    }

    async fn set_table_location(&mut self) {
        let client = get_glue_client(self.database.region().to_string()).await;
        let table = client
            .get_table()
            .database_name(self.database.name())
            .name(self.name())
            .send()
            .await
            .unwrap()
            .table
            .unwrap();
        self.s3_location = table.storage_descriptor.unwrap().location.unwrap();
    }
}

/// Glue Database convinience struct to hold database name and region
impl GlueDatabase {
    pub fn new(region: String, account_id: String, name: String) -> Self {
        GlueDatabase {
            region,
            account_id,
            name,
        }
    }
    pub fn region(&self) -> &String {
        &self.region
    }
    pub fn account_id(&self) -> &String {
        &self.account_id
    }
    pub fn name(&self) -> &String {
        &self.name
    }
    pub fn format_choice(&self) -> String {
        format!("{} in {}", self.name, self.region)
    }
}

/// Get Glue client for a region
async fn get_glue_client(region: String) -> Client {
    let config = aws_config::from_env()
        .region(Region::new(region))
        .load()
        .await;

    Client::new(&config)
}

async fn get_account_id(region: String) -> String {
    let config = aws_config::from_env()
        .region(Region::new(region))
        .load()
        .await;

    let client = StsClient::new(&config);

    client
        .get_caller_identity()
        .send()
        .await
        .unwrap()
        .account
        .unwrap()
}
/// Get all databases in all regions
pub async fn get_aws_glue_databases() -> Vec<GlueDatabase> {
    // Get all regions
    let my_regions = get_all_regions().await.unwrap();
    // get current account id from sts get_caller_identity
    let accound_id = get_account_id(my_regions[0].to_string()).await;
    let mut databases: Vec<GlueDatabase> = vec![];

    // Get all databases in all regions
    for my_region in &my_regions {
        // Get glue client for the region
        let client = get_glue_client(my_region.to_string()).await;

        // Get all databases in the region
        let mut regional_databases: Vec<GlueDatabase> = client
            .get_databases()
            .send()
            .await
            .expect("failed to get databases")
            .database_list()
            .unwrap()
            .iter()
            .map(|database| {
                GlueDatabase::new(
                    my_region.to_string(),
                    accound_id.to_string(),
                    database.name().unwrap().to_string(),
                )
            })
            .collect();
        databases.append(&mut regional_databases);
    }
    // if no throw error and exit
    if databases.is_empty() {
        println!("No Glue Databases found");
        std::process::exit(1);
    }
    databases
}

/// Get all tables in a database
pub async fn get_one_glue_table(database: &GlueDatabase) -> Vec<GlueTable> {
    // Get glue client for the region
    let client = get_glue_client(database.region().to_string()).await;

    let response = client
        .get_tables()
        .database_name(database.name())
        .send()
        .await
        .expect("failed to get tables");

    let mut tables: Vec<GlueTable> = vec![];
    for table in response.table_list().unwrap().iter() {
        let glue_table = GlueTable::new(database.clone(), table.name().unwrap().to_string()).await;
        // only keep s3 based tables
        if glue_table.s3_location().to_lowercase().starts_with("s3://") {
            tables.push(glue_table);
        }
    }
    // if no tables throw error and exit
    if tables.is_empty() {
        println!("No Glue Tables on S3 found in database {}", database.name());
        std::process::exit(1);
    }
    tables
}
