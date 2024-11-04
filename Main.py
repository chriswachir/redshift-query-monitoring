import psycopg2 as pg
from configparser import ConfigParser
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
import json
from datetime import datetime, timedelta

# Function to read server configuration
def server_config(filename, section):
    parser = ConfigParser()
    parser.read(filename)
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return config

# Define the function to fetch long-running queries and send alerts
def fetch_long_running_queries():
    try:
        # Read email and Slack configuration
        email_config = server_config('/config/r_emailConfig.ini', 'email_config')
        smtp_host = email_config['smtp_host']
        smtp_port = int(email_config['smtp_port'])
        smtp_username = email_config['smtp_username']
        smtp_password = email_config['smtp_password']
        fromaddr = email_config['sender_email']
        toaddr = 'christopher.wachira@cellulant.io'
        slack_webhook_url = email_config['slack_webhook_url']

        # Set up email subject and message
        today = str(datetime.now())
        subject = "Redshift Long-Running Query Alert"
        msg = MIMEMultipart("alternative")
        msg['From'] = fromaddr
        msg['To'] = toaddr
        msg['Subject'] = subject + " " + today

        # Get Redshift server details
        redshift_details = server_config('/config/r_conf.ini', 'yoda_r_lake')
        host = redshift_details["host"]
        dbname = redshift_details["database"]
        user = redshift_details["user"]
        password = redshift_details["password"]
        port = redshift_details["port"]

        # Connect to Redshift
        conn = pg.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port
        )
        cursor = conn.cursor()

        # Fetch long-running queries from the last 30 minutes
        query = """
        SELECT 
            userid,
            query,  -- This contains the query text
            pid,
            starttime,
            duration as time_taken,
            user_name,
            db_name
        FROM 
            stv_recents
        WHERE 
            status = 'Running'
        AND 
            starttime > (CURRENT_TIMESTAMP - interval '30 minutes')  -- Last 30 minutes window
        AND
            userid != 100  -- Exclude queries from user ID 100
        ORDER BY 
            duration DESC;
        """
        cursor.execute(query)
        results = cursor.fetchall()

        if results:
            for result in results:
                userid, query_text, pid, start_time, time_taken, user_name, db_name = result

                # Calculate end time and execution time in minutes
                end_time = start_time + timedelta(seconds=(time_taken / 1000))  # Convert milliseconds to seconds
                exec_time_in_minutes = time_taken / 60000  # Convert milliseconds to minutes

                # Prepare alert message with required details
                message_body = f"""
                Redshift Query Alert:
                - User ID: {userid}
                - Username: {user_name}
                - Start Time: {start_time}
                - End Time: {end_time}
                - Query: {query_text}
                - Execution Time: {exec_time_in_minutes:.2f} minutes
                - Database: {db_name}
                """

                # Attach the message to the email
                part = MIMEText(message_body, "plain")
                msg.attach(part)

                # Send Email
                server = smtplib.SMTP_SSL(smtp_host, smtp_port)
                server.login(smtp_username, smtp_password)
                server.sendmail(fromaddr, toaddr, msg.as_string())
                server.quit()

                # Send Slack Notification
                slack_message = {
                    "text": message_body
                }
                requests.post(slack_webhook_url, data=json.dumps(slack_message))

                print(f"Notification sent for query by User ID: {userid}, Username: {user_name}.")

        else:
            print("No long-running queries found.")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error fetching or sending notification: {str(e)}")

# Run the function if this script is executed
if __name__ == "__main__":
    fetch_long_running_queries()
