import os
import pandas as pd
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Configure logging
def setup_logging(log_dir='logs', log_file='processing.log'):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logging.basicConfig(filename=os.path.join(log_dir, log_file),
                        level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(message)s')

# Send email notification
def send_email(subject, body, to_email, from_email, smtp_server, smtp_port, smtp_user, smtp_password):
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        text = msg.as_string()
        server.sendmail(from_email, to_email, text)
        server.quit()
        logging.info("Email sent successfully.")
    except Exception as e:
        logging.error(f"Error sending email: {e}")

# Read CSV and handle errors
def read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Successfully read file {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        raise

# Process df2 and add truncated_global_id
def prepare_df2(df2_file):
    try:
        df2 = read_csv_file(df2_file)
        df2['truncated_global_id'] = df2['global_id'].astype(str).str[:-1]
        logging.info("Successfully prepared df2")
        return df2
    except Exception as e:
        logging.error(f"Error preparing df2: {e}")
        raise

# Add hsbc_indicator to df1
def add_hsbc_indicator(df1, df2):
    try:
        df1['hsbc_indicator'] = df1['PersNo'].isin(df2['truncated_global_id']).map({True: 'TRUE', False: 'FALSE'})
        logging.info("Successfully added hsbc_indicator to df1")
        return df1
    except Exception as e:
        logging.error(f"Error adding hsbc_indicator to df1: {e}")
        raise

# Write DataFrame to XLSX
def write_xlsx_file(df, output_path):
    try:
        df.to_excel(output_path, index=False)
        logging.info(f"File written to {output_path}")
    except Exception as e:
        logging.error(f"Error writing file {output_path}: {e}")
        raise

# Get processed files from log
def get_processed_files(log_file='processed_files.log'):
    try:
        if os.path.exists(log_file):
            with open(log_file, 'r') as file:
                processed_files = set(file.read().splitlines())
                logging.info(f"Successfully read processed files from {log_file}")
                return processed_files
        logging.info("No processed files log found, starting fresh.")
        return set()
    except Exception as e:
        logging.error(f"Error reading processed files log {log_file}: {e}")
        raise

# Update log with processed file
def log_processed_file(file_path, log_file='processed_files.log'):
    try:
        with open(log_file, 'a') as file:
            file.write(file_path + '\n')
        logging.info(f"Logged processed file {file_path}")
    except Exception as e:
        logging.error(f"Error logging processed file {file_path}: {e}")
        raise

# Verify if all elements from df2 were found in df1
def verify_all_elements_found(df1, df2):
    try:
        missing_elements = df2[~df2['truncated_global_id'].isin(df1['PersNo'])]
        if not missing_elements.empty:
            logging.warning(f"Missing elements in df1: {missing_elements['truncated_global_id'].tolist()}")
            return False
        logging.info("All elements from df2 found in df1")
        return True
    except Exception as e:
        logging.error(f"Error verifying elements in df1 against df2: {e}")
        raise

# Process individual file
def process_file(file_path, df2, output_dir):
    try:
        df1 = read_csv_file(file_path)
        df1 = add_hsbc_indicator(df1, df2)
        
        if verify_all_elements_found(df1, df2):
            output_file = os.path.join(output_dir, os.path.splitext(os.path.basename(file_path))[0] + '.xlsx')
            write_xlsx_file(df1, output_file)
        else:
            logging.warning(f"File {file_path} did not contain all elements from df2 and was not written to output.")
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")

# Process new files in directory
def process_new_files(df1_dir, df2_file, output_dir, log_file='processed_files.log'):
    try:
        df2 = prepare_df2(df2_file)
        processed_files = get_processed_files(log_file)
        
        for root, _, files in os.walk(df1_dir):
            for file in files:
                file_path = os.path.join(root, file)
                if file_path not in processed_files:
                    process_file(file_path, df2, output_dir)
                    log_processed_file(file_path, log_file)
        logging.info("All new files processed successfully.")
    except Exception as e:
        logging.error(f"Error processing new files: {e}")
        raise

# Main execution function
def main():
    setup_logging()
    df1_directory = 'path/to/df1_files'
    df2_file_path = 'path/to/df2_file.csv'
    output_directory = 'path/to/output_files'

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    try:
        process_new_files(df1_directory, df2_file_path, output_directory)
        send_email(
            subject="Processing Completed Successfully",
            body="All files have been processed successfully.",
            to_email='recipient@example.com',
            from_email='sender@example.com',
            smtp_server='smtp.example.com',
            smtp_port=587,
            smtp_user='your_smtp_user',
            smtp_password='your_smtp_password'
        )
    except Exception as e:
        send_email(
            subject="Processing Failed",
            body=f"An error occurred during processing: {e}",
            to_email='recipient@example.com',
            from_email='sender@example.com',
            smtp_server='smtp.example.com',
            smtp_port=587,
            smtp_user='your_smtp_user',
            smtp_password='your_smtp_password'
        )

if __name__ == '__main__':
    main()
