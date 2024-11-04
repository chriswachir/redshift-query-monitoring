# Redshift Long-Running Query Monitoring Script

## Overview
This Python script is designed to monitor long-running queries in an Amazon Redshift cluster. If queries have been running for over a given time threshold, the script sends alert notifications via both email and Slack. This helps maintain performance by identifying potential issues with resource-intensive queries.

## How It Works
1. **Connects to Redshift**: The script establishes a connection to an Amazon Redshift cluster using credentials provided in a configuration file.
2. **Fetches Long-Running Queries**: Queries running for more than 30 minutes within the last 30-minute period are identified.
3. **Sends Notifications**: For each long-running query, the script sends detailed notifications through:
   - **Email**: Provides query details like user ID, username, start time, execution time, and database name.
   - **Slack**: Posts the same information to a specified Slack channel using a webhook URL.

## Configuration
### Required Files:
1. **`r_emailConfig.ini`**: Stores SMTP and Slack configuration details.
2. **`r_conf.ini`**: Contains Redshift connection credentials.

## Details Sent in Alerts:
- **User ID and Username**: Identifies the user running the query.
- **Start and Estimated End Time**: Helps determine how long the query has been running.
- **Query Text**: Shows the actual SQL query.
- **Execution Time**: Expressed in minutes.
- **Database Name**: Indicates which database the query is running on.
