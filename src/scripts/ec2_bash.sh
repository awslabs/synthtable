#!/bin/bash 
set -x

region=$(curl http://169.254.169.254/latest/meta-data/placement/availability-zone | sed 's/.$//')



function send_cw_logs {
    aws logs put-log-events --region $region --log-group-name <your project> --log-stream-name <your table> --log-events timestamp=$(date +%s%3N),message="$1"
}   



send_cw_logs "Installing Python 3.8 ..."

sudo amazon-linux-extras install -y python3.8
version_python=$(python3.8 --version)

send_cw_logs "Python 3.8 Installed ..."

send_cw_logs "Installing required packages ..."
pip3.8  install sdv
pip3.8 install awswrangler
send_cw_logs "Required packages installed ..."

send_cw_logs "Starting Data Creation Script ..."
cat > script.py << EOF
<your python script>
EOF
# Run the script and redirect the stderr to a variable
error_message=$(python3.8 script.py $region <your database> <your table> <your project> <your table> 2>&1)

# check the exit status and store it in a variable
result=$?

# check for success or failure
if [ $result -eq 0 ]; then
    send_cw_logs "Script ran successfully"
    send_cw_logs "Done"
else
    send_cw_logs "Script failed"
fi 