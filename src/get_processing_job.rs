//! # get_processing_job
use crate::cw_logging::CWLogSender;
/// This module contains the code to create an EC2 instance and run the workload on it
/// The EC2 instance is created in the same region as the source data and in the private subnet
/// with available IP addresses.
/// The EC2 instance is created with the IAM role that has minimal permissions to run the workload
/// and to write the output to the S3 bucket.
/// shell script created that wraps SytheticTabularDataGenerator python script and runs as part of user data of the EC2 instance
use crate::get_glue_data::GlueTable;
use crate::manage_iam::{cleanup_aim, get_iam_instance_profile_specification};
use crate::progress_tracker::{JobState, ProgressTracker};
use crate::PROJECT_NAME;
use aws_sdk_ec2::model::Filter;
use aws_sdk_ec2::model::{
    BlockDeviceMapping, EbsBlockDevice, InstanceStateName, InstanceType, ResourceType, Tag,
    TagSpecification,
};
use aws_sdk_ec2::{Client, Error};

use aws_types::region::Region;
use base64::{engine::general_purpose, Engine as _};
use colored::*;
#[derive(Clone)]
pub struct ValidSubnet {
    vpc: String,
    subnet: String,
}
impl ValidSubnet {
    fn new(vpc: String, subnet: String) -> Self {
        Self { vpc, subnet }
    }
    pub fn get_vpc(&self) -> &String {
        &self.vpc
    }
    pub fn get_subnet(&self) -> &String {
        &self.subnet
    }
    pub fn format_for_display(&self) -> String {
        format!("Subnet: {} in VPC: {}", self.get_subnet(), self.get_vpc())
    }
}
/// Returns ec2 client for the region specified in the environment or default region
async fn get_ec2_client(region: &str) -> Client {
    let config = aws_config::from_env()
        .region(Region::new(region.to_string()))
        .load()
        .await;
    Client::new(&config)
}
/// get vpc list and pick a suitable subnet
/// Suitable subnet is a private subnet with
/// 1) available IP addresses
/// 2)  NAT gateway in a vpc
/// 3) route to the NAT gateway from the subnet
pub async fn get_subnet_list(my_region: &str) -> Result<Vec<ValidSubnet>, Error> {
    let client = get_ec2_client(my_region).await;

    let vpc_list = &client
        .describe_vpcs()
        .send()
        .await
        .expect("failed to get vpc list")
        .vpcs()
        .unwrap()
        .iter()
        .map(|vpc| vpc.vpc_id().unwrap().to_string())
        .collect::<Vec<String>>();

    let _my_vpc = vpc_list.get(0).expect("You have no VPCs in this region");

    let mut valid_subnets: Vec<ValidSubnet> = vec![];
    for vpc in vpc_list {
        let private_subnets = &client
            .describe_subnets()
            .filters(Filter::builder().name("vpc-id").values(vpc).build())
            .send()
            .await
            .expect("failed to get subnets")
            .subnets()
            .unwrap()
            .iter()
            .filter(|subnet| {
                subnet.map_public_ip_on_launch().unwrap() == false
                    && subnet.available_ip_address_count().unwrap() > 0
            })
            .map(|subnet| subnet.subnet_id().unwrap().to_string())
            .collect::<Vec<String>>();
        let nat_gatways = &client
            .describe_nat_gateways()
            .filter(Filter::builder().name("vpc-id").values(vpc).build())
            .send()
            .await
            .expect("failed to get nat gateways")
            .nat_gateways()
            .unwrap()
            .iter()
            .map(|nat_gateway| nat_gateway.nat_gateway_id().unwrap().to_string())
            .collect::<Vec<String>>();
        // wepossibly have some private subnets and nat gateways in this vpc
        if private_subnets.len() > 0 && nat_gatways.len() > 0 {
            // check if we have a route table with a route to the nat gateway
            for private_subnet in private_subnets {
                // get all route tables associated with the subnet
                let route_tables = &client
                    .describe_route_tables()
                    .filters(
                        Filter::builder()
                            .name("association.subnet-id")
                            .values(private_subnet)
                            .build(),
                    )
                    .send()
                    .await
                    .expect("failed to get route tables")
                    .route_tables()
                    .unwrap()
                    .iter()
                    .filter_map(|route_table| match route_table.route_table_id() {
                        Some(route_table_id) => Some(route_table_id.to_string()),
                        None => None,
                    })
                    .collect::<Vec<String>>();

                // check if the route table has a route to the nat gateway
                for route_table in route_tables {
                    let routes = &client
                        .describe_route_tables()
                        .route_table_ids(route_table)
                        .send()
                        .await
                        .expect("failed to get routes")
                        .route_tables()
                        .unwrap_or(&vec![])
                        .iter()
                        .map(|route_table| {
                            route_table
                                .routes()
                                .unwrap_or(&vec![])
                                .iter()
                                .filter_map(|route| match route.nat_gateway_id() {
                                    Some(nat_gateway_id) => Some(nat_gateway_id.to_string()),
                                    None => None,
                                })
                                .collect::<Vec<String>>()
                        })
                        .collect::<Vec<Vec<String>>>();

                    for route in routes {
                        for nat_gateway in nat_gatways {
                            if route.contains(nat_gateway) {
                                valid_subnets.push(ValidSubnet::new(
                                    vpc.to_string(),
                                    private_subnet.to_string(),
                                ))
                            };
                        }
                    }
                }
            }
        }
    }
    if valid_subnets.len() == 0 {
        println!(
            "{}",
            "No suitable subnets found. Please create a private subnet with a route to a NAT gateway in the VPC where the source data is stored"
                .red()
        );
        std::process::exit(1);
    }
    Ok(valid_subnets)
}

/// Returns a suitable AMI for the region specified in the environment or default region
/// The AMI is the latest Amazon Linux 2 AMI
/// The AMI is used to create the EC2 instance to run the workload
async fn get_suitable_ami(my_region: &str) -> Result<String, Error> {
    let client = get_ec2_client(my_region).await;
    let suitable_ami = client
        .describe_images()
        .owners("amazon")
        .filters(
            Filter::builder()
                .name("name")
                .values("amzn2-ami-hvm-*-x86_64-gp2")
                .build(),
        )
        .send()
        .await
        .expect("failed to get images")
        .images()
        .unwrap()
        .get(0)
        .unwrap()
        .image_id()
        .unwrap()
        .to_string();
    Ok(suitable_ami)
}

/// Returns a script to be run on the EC2 instance that generates the synthetic data
fn get_script(glue_table: &GlueTable) -> String {
    let bash_script = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/scripts/ec2_bash.sh"
    ));
    let python_script = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/scripts/single_table.py"
    ));

    let script = bash_script
        .replace("<your python script>", python_script)
        .replace("<your database>", glue_table.database().name())
        .replace("<your table>", glue_table.name())
        .replace("<your project>", PROJECT_NAME);

    general_purpose::STANDARD.encode(script) // base64 encode the script
}

/// Issues a request to create an EC2 instance with the specified AMI and runs the script on it
/// Returns the instance id of the created instance
async fn run_ec2_instance(subnet_id: &str, glue_table: &GlueTable) -> Result<String, Error> {
    // get all the required parameters
    let my_region = glue_table.database().region();
    let latest_ami = get_suitable_ami(my_region).await?;
    let script = get_script(glue_table);
    let tag = Tag::builder().key("Name").value(PROJECT_NAME).build();
    let client = get_ec2_client(my_region).await;

    // TODO: change this to take table structure as input
    let iam_instance_profile = get_iam_instance_profile_specification(glue_table)
        .await
        .unwrap();
    // let iam_instance_profile = IamInstanceProfileSpecification::builder()
    //    .arn("arn:aws:iam::050532831725:instance-profile/PowerUser")
    //    .build();

    // create instance and get instance id of it

    let instance_id = client
        .run_instances()
        .image_id(latest_ami.to_string())
        .instance_type(InstanceType::C6i4xlarge)
        .max_count(1)
        .min_count(1)
        .block_device_mappings(
            BlockDeviceMapping::builder()
                .device_name("/dev/xvda")
                .ebs(EbsBlockDevice::builder().volume_size(1000).build())
                .build(),
        )
        .tag_specifications(
            TagSpecification::builder()
                .resource_type(ResourceType::Instance)
                .tags(tag)
                .build(),
        )
        .subnet_id(subnet_id)
        .iam_instance_profile(iam_instance_profile)
        .user_data(&script)
        .send()
        .await
        .expect("failed to create instance")
        .instances
        .unwrap()
        .get(0)
        .unwrap()
        .instance_id()
        .unwrap()
        .to_string();

    Ok(instance_id)
}

/// Returns the instance state name of the specified instance
async fn get_instance_state_name(
    instance_id: &str,
    my_region: &str,
) -> Result<InstanceStateName, Error> {
    let client = get_ec2_client(my_region).await;
    let instance_state_name = &client
        .describe_instances()
        .instance_ids(instance_id.to_string())
        .send()
        .await
        .expect("failed to get instance")
        .reservations()
        .unwrap()
        .get(0)
        .unwrap()
        .instances()
        .unwrap()
        .get(0)
        .unwrap()
        .state()
        .unwrap()
        .name()
        .unwrap()
        .clone();

    Ok(instance_state_name.clone())
}

/// Runs the synthetic data job creation on ec2 instance using the specified parameters
/// Job uses the specified database and table as the source
/// Outputs the progress of the job to CloudWatch logs and displays it on the console
/// Returns an error if the job fails
pub async fn run_sythetic_data_job(subnet_id: &str, glue_table: &GlueTable) -> Result<(), Error> {
    // Declare a CloudWatch log "helper" for this task
    let my_region = glue_table.database().region();
    let logger = CWLogSender::new(my_region.into(), glue_table.name().into()).await;
    // Create a progress bar
    let mut pb = ProgressTracker::new(logger, 10, &glue_table);

    // create ec2 instance and get instance id
    let instance_id = run_ec2_instance(subnet_id, glue_table).await?;

    // wait for the instance to fail or complete the job.
    // Terminate the instance once the job is complete or failed
    loop {
        // get the instance state name
        let instance_state_name = get_instance_state_name(&instance_id, my_region).await?;

        match instance_state_name {
            // if the instance is running, update the progress bar
            InstanceStateName::Running => {
                let state = pb.update_progress().await;
                // if the job is completed, terminate the instance and break the loop
                match state {
                    JobState::Completed => {
                        // terminate ec2 instance
                        terminate_ec2_instance(&instance_id, my_region).await?;
                        // clean up iam role
                        cleanup_aim(glue_table)
                            .await
                            .expect("failed to clean up iam role");

                        let summary_message = format!(
                            "Synthetic Data Generation Job Completed. \
                            \nPlease check the database {} and table {}_synthetic for the generated data.",
                            glue_table.database().name(), glue_table.name()
                        );
                        println!("{}", summary_message.green());
                        break;
                    }
                    // if the job is running, continue the loop
                    JobState::Running => {}
                    // if the job is failed, terminate the instance and break the loop
                    JobState::Failed => {
                        // terminate_ec2_instance(&instance_id, my_region).await?;
                        let summary_message = format!(
                            "Synthetic Data Generation Job Failed. \
                            \nPlease check logs on CloudWatch - {} and Instance - {} for more details.",
                            PROJECT_NAME, instance_id
                        );
                        println!("{}", summary_message.red().bold());
                        break;
                    }
                }
            }
            // if the instance is pending, continue the loop
            InstanceStateName::Pending => {}
            // if the instance is terminated, break the loop
            _ => {
                println!("Instance is in an unknown state");
                break;
            }
        }
    }

    Ok(())
}

/// Terminates the ec2 instance with the specified instance id
async fn terminate_ec2_instance(instance_id: &str, my_region: &str) -> Result<(), Error> {
    let client = get_ec2_client(my_region).await;

    client
        .terminate_instances()
        .instance_ids(instance_id.to_string())
        .send()
        .await
        .expect("failed to terminate instance");

    Ok(())
}
