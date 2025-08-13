import streamlit as st
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import io
import os

from src.reporting import process_ikitchen_data, extract_date_from_metadata
from src.process_pos_data import process_pos_data

from io import StringIO

# Configuration email via secrets
sender_email = st.secrets["email"]["sender_email"]
sender_password = st.secrets["email"]["sender_password"]
receiver_emails = st.secrets["email"]["receiver_emails"]  # Liste des emails séparés par des virgules

# Configuration of the page
st.set_page_config(
    page_title="IKitchen Daily Sales Report Generator",
    layout="wide"
)

# Application title
st.title("IKitchen Sales Report Generator")


def send_email_to_multiple_recipients(sender_email, sender_password, receiver_emails_list, subject, body, csv_content, filename):
    """Send email with CSV attachment to multiple recipients"""
    try:
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = ", ".join(receiver_emails_list)
        message["Subject"] = subject
        message.attach(MIMEText(body, "plain"))
        
        csv_buffer = io.StringIO()
        csv_buffer.write(csv_content)
        csv_data = csv_buffer.getvalue().encode('utf-8')
        
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(csv_data)
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', f'attachment; filename={filename}')
        message.attach(attachment)
        
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, sender_password)
            server.send_message(message, to_addrs=receiver_emails_list)
        
        return True, f"Email sent successfully to {len(receiver_emails_list)} recipients!"
    except Exception as e:
        return False, f"Error sending email: {str(e)}"

# File uploader with automatic processing
uploaded_file = st.file_uploader(
    "Upload Sales Summary CSV File",
    type=['csv'],
    help="""**Expected CSV Format**: 
- Line 1: Headers or title
- Line 2: Metadata with dates
- Line 3: Empty or headers
- Line 4+: Sales data with columns including 'Status', 'Amount', 'Sale date', 'Ordertype name'""",
    label_visibility="hidden"
)

if uploaded_file is not None:
    try:
        st.success(f"✅ File '{uploaded_file.name}' uploaded successfully!")
        
        # Persist uploaded file to a temporary file for ingestion
        try:
            # Determine the file type
            file_extension = os.path.splitext(uploaded_file.name)[1].lower()

            # Extract the original file name (without extension)
            original_file_name = os.path.splitext(uploaded_file.name)[0]

            # Construct a temporary file path including the original file name
            temp_file_path = os.path.join(f"temp_{original_file_name}{file_extension}")
            with open(temp_file_path, "wb") as temp_file:
                temp_file.write(uploaded_file.getbuffer())

            log_buffer = StringIO()

            log_placeholder = st.empty()  # Placeholder for real-time logs

            def log_function(message):
                """Append message to the log buffer and update Streamlit UI."""
                log_buffer.write(message + "\n")
                log_placeholder.text(log_buffer.getvalue())


            with st.spinner("Processing the uploaded file..."):
                process_pos_data(temp_file_path, logger=log_function)

                st.success("File processed and data inserted into Supabase successfully!")
        except Exception as e:
            st.warning(f"⚠️ POS ingestion skipped or failed: {type(e).__name__}: {str(e)}")
        
        # Automatic processing when file is uploaded
        with st.spinner("Processing sales data automatically..."):
            report_data, df_processed, error_message = process_ikitchen_data(uploaded_file)
            
            if error_message:
                st.error(f"❌ {error_message}")
            elif report_data is None:
                st.error("❌ Failed to process the file")
            else:
                st.success("✅ Sales data processed successfully!")
                
                # Parse receiver emails from secrets
                try:
                    if isinstance(receiver_emails, str):
                        receiver_emails_list = [email.strip() for email in receiver_emails.split(',')]
                    else:
                        receiver_emails_list = receiver_emails
                    
                    st.info(f"📤 Sending to {len(receiver_emails_list)} recipients: {', '.join(receiver_emails_list)}")
                    
                    with st.spinner("Sending email..."):
                        formatted_date = extract_date_from_metadata(report_data['metadata_line'])
                        email_subject = f"Daily sales report : {formatted_date}"
                        email_body = f"""Please find the daily sales report attached.

Report Summary:
- Date: {formatted_date}
- Total Sales: {report_data['lahore_total_sales']:,.2f} (Lahore), {report_data['santorini_total_sales']:,.2f} (Santorini)

Best regards,
IKitchen Sales Report System"""
                        
                        report_filename = f"sales_report_{report_data['sales_date']}.csv"
                        
                        success, message = send_email_to_multiple_recipients(
                            sender_email=sender_email,
                            sender_password=sender_password,
                            receiver_emails_list=receiver_emails_list,
                            subject=email_subject,
                            body=email_body,
                            csv_content=report_data['final_report'],
                            filename=report_filename
                        )
                        
                        if success:
                            st.success(f"✅ {message}")
                            st.balloons()
                        else:
                            st.error(f"❌ {message}")
                            
                except Exception as e:
                    st.error(f"❌ Error with email configuration: {str(e)}")
                
    except Exception as e:
        st.error(f"❌ Error processing file: {str(e)}")

else:
    st.info("From the ServQuick Console, go to 'Reports' then 'Transaction Summary' then 'Sales details by receipt'. Select the desired timeframe + 1 extra day. Export as CSV and upload the file - processing and email sending will be automatic!")

