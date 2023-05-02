//! # manage_iam
//!
//! This module contains functions for managing IAM roles and instance profiles
//! it is used by the CLI to create and delete IAM role and instance profile with a minimal set of permissions
//! required to run the python script on EC2 instance.
const POLICY_DIR: Dir = include_dir!("src/policies");

use crate::get_glue_data::GlueTable;
use crate::PROJECT_NAME;
use aws_sdk_ec2::model::IamInstanceProfileSpecification;
use aws_sdk_iam::{Client as IamClient, Error as IamError};
use aws_types::region::Region;
use include_dir::{include_dir, Dir};
extern crate include_dir;
use tokio::time::Duration;

/// get IAM client for the region specified region
async fn get_iam_client(region: &str) -> Result<IamClient, IamError> {
    let config = aws_config::from_env()
        .region(Region::new(region.to_string()))
        .load()
        .await;
    Ok(IamClient::new(&config))
}

/// Checks if instance profile exists
async fn is_instance_profile_exists(region: &str) -> Result<bool, IamError> {
    let client = get_iam_client(region).await?;

    let is_exists = client
        .list_instance_profiles()
        .send()
        .await
        .expect("Could not list instance profiles")
        .instance_profiles()
        .unwrap()
        .iter()
        .any(|instance_profile| instance_profile.instance_profile_name().unwrap() == PROJECT_NAME);

    Ok(is_exists)
}

/// Deletes instance profile
async fn delete_instance_profile(region: &str) -> Result<(), IamError> {
    let client = get_iam_client(region).await?;
    let _response = client
        .delete_instance_profile()
        .instance_profile_name(PROJECT_NAME)
        .send()
        .await
        .expect("Could not delete instance profile");

    // loop while instance profile is not actually deleted. This is needed because IAM is eventually consistent
    while is_instance_profile_exists(region).await? {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

/// Creates instance profile. Returns instance profile ARN
async fn create_instance_profile(glue_table: &GlueTable) -> Result<String, IamError> {
    let region = glue_table.database().region();
    let client = get_iam_client(region).await?;

    // create instance profile
    let response = client
        .create_instance_profile()
        .instance_profile_name(PROJECT_NAME)
        .send()
        .await
        .expect("Could not create instance profile");

    // loop while instance profile is not created. This is needed because IAM is eventually consistent
    while !is_instance_profile_exists(region).await? {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    tokio::time::sleep(Duration::from_secs(30)).await;

    // add role to instance profile
    add_role_to_instance_profile(region).await.unwrap();

    add_policies_to_role(glue_table).await.unwrap();

    Ok(response
        .instance_profile()
        .unwrap()
        .arn()
        .unwrap()
        .to_string())
}

/// Checks if role exists
async fn is_role_exists(region: &str) -> Result<bool, String> {
    let client = get_iam_client(region)
        .await
        .expect("Could not get IAM client");
    let response = &client.get_role().role_name(PROJECT_NAME).send().await;

    match response {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

/// Deletes role
async fn delete_role(region: &str) -> Result<(), IamError> {
    let client = get_iam_client(region).await?;
    let _response = client
        .delete_role()
        .role_name(PROJECT_NAME)
        .send()
        .await
        .expect("Could not delete role");

    // loop while role is not deleted. This is needed because IAM is eventually consistent
    while !is_role_exists(region)
        .await
        .expect("Could not check if role exists")
    {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

/// Creates role for EC2 instance
async fn create_ec2_role(region: &str) -> Result<(), IamError> {
    let client = get_iam_client(region).await?;

    // create role. This role will be used by EC2 instance
    // assume role policy document allows EC2 to assume this role and run the python script. Hence, it is hardcoded.
    // https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_principal.html

    let _response = client
        .create_role()
        .role_name(PROJECT_NAME)
        .assume_role_policy_document(
            r#"{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "ec2.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }"#,
        )
        .send()
        .await
        .expect("Could not create role");

    // loop while role is not created. This is needed because IAM is eventually consistent
    while !is_role_exists(region)
        .await
        .expect("Could not check if role exists")
    {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}

/// This function adds all required policies to the role associated with EC2 instance
/// it reads the policy documents from folder src/policies one by one and adds them to the role inline
/// it adjust each policy document to the region, account id, database name and table name as needed to make privillages
/// absolutely MINIMAL.
async fn add_policies_to_role(glue_table: &GlueTable) -> Result<(), IamError> {
    let region = glue_table.database().region();
    // list all files in src/policies folder
    let client = get_iam_client(region).await.expect("cannot get IAM client");

    for (policy_name, policy_document) in generate_policy_docs(glue_table) {
        let _response = client
            .put_role_policy()
            .role_name(PROJECT_NAME)
            .policy_name(&policy_name)
            .policy_document(policy_document)
            .send()
            .await
            .expect("Could not add policy to role");
    }

    Ok(())
}

async fn add_role_to_instance_profile(region: &str) -> Result<(), IamError> {
    let client = get_iam_client(region).await?;
    create_ec2_role(region).await.unwrap();
    let _response = client
        .add_role_to_instance_profile()
        .instance_profile_name(PROJECT_NAME)
        .role_name(PROJECT_NAME)
        .send()
        .await
        .expect("Could not add policy to instance profile");
    Ok(())
}
/// remove all roles from instance profile
async fn remove_role_from_instance_profile(region: &str) -> Result<(), IamError> {
    let client = get_iam_client(region).await?;
    let _response = client
        .remove_role_from_instance_profile()
        .instance_profile_name(PROJECT_NAME)
        .role_name(PROJECT_NAME)
        .send()
        .await
        .expect("Could not remove role from instance profile");
    Ok(())
}

/// Get all polices from a folder and return them as a vector of tuples
/// first element of the tuple is the name of the policy
/// second element is the content of the policy
fn get_all_policies() -> impl Iterator<Item = (String, String)> {
    let json_files = POLICY_DIR
        .files()
        .map(|f| (f.path(), f.contents_utf8().unwrap()))
        .filter(|f| f.0.extension().map(|ext| ext == "json").unwrap_or(false))
        .map(|f| {
            (
                f.0.file_stem().unwrap().to_str().unwrap().to_string(),
                f.1.to_string(),
            )
        });
    json_files
}
/// given a policy name adjust for specific table
fn generate_policy_docs(glue_table: &GlueTable) -> Vec<(String, String)> {
    let json_files = get_all_policies();
    let mut policy_docs: Vec<(String, String)> = Vec::new();
    for (file_name, json_file_contents) in json_files {
        let policy_document = json_file_contents
            .replace("<your region>", &glue_table.database().region())
            .replace("<your account>", &glue_table.database().account_id())
            .replace("<your database>", &glue_table.database().name())
            .replace("<your table>", &glue_table.name())
            .replace("<your project>", PROJECT_NAME)
            .replace("<your s3arn>", &glue_table.s3_arn())
            .replace(
                "<your bucket>",
                glue_table
                    .s3_arn()
                    .split(":::")
                    .nth(1)
                    .unwrap()
                    .split("/")
                    .nth(0)
                    .unwrap(),
            );
        let policy_name = format!("{}{}", PROJECT_NAME, file_name);
        policy_docs.push((policy_name, policy_document));
    }
    policy_docs
}

///removes all policies from role
/// this is needed because we cannot delete role if it has policies attached
async fn remove_all_policies_role(glue_table: &GlueTable) -> Result<(), IamError> {
    let client = get_iam_client(glue_table.database().region()).await?;

    let _response = &client
        .detach_role_policy()
        .role_name(PROJECT_NAME)
        .policy_arn("arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore")
        .send()
        .await
        .expect("Could not remove policy from role");

    let attached_policies = &client
        .list_role_policies()
        .role_name(PROJECT_NAME)
        .send()
        .await
        .expect("Could not list attached policies")
        .policy_names()
        .unwrap()
        .to_vec();

    for policy_name in attached_policies {
        let _response = client
            .delete_role_policy()
            .role_name(PROJECT_NAME)
            .policy_name(policy_name)
            .send()
            .await
            .expect("Could not remove policy from role");
    }
    Ok(())
}

/// check if role exists and instance profile exists
pub async fn cleanup_aim(glue_table: &GlueTable) -> Result<(), IamError> {
    let region = glue_table.database().region();
    if is_role_exists(region)
        .await
        .expect("Could not check if role exists")
    {
        remove_all_policies_role(glue_table)
            .await
            .expect("Could not remove policies from role");
        if is_instance_profile_exists(region).await? {
            remove_role_from_instance_profile(region).await.unwrap();
            delete_instance_profile(region).await.unwrap();
        }
        delete_role(region).await.unwrap();
    }
    Ok(())
}
pub async fn get_iam_instance_profile_specification(
    glue_table: &GlueTable,
) -> Result<IamInstanceProfileSpecification, IamError> {
    cleanup_aim(glue_table)
        .await
        .expect("Could not cleanup IAM");

    let instance_profile_arn = create_instance_profile(glue_table)
        .await
        .expect("Could not create instance profile");

    Ok(IamInstanceProfileSpecification::builder()
        .arn(instance_profile_arn)
        .build())
}
