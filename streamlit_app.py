import streamlit as st
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import io
import re
from datetime import datetime, time

import streamlit as st

# Configuration email via secrets
sender_email = st.secrets["email"]["sender_email"]
sender_password = st.secrets["email"]["sender_password"]
receiver_emails = st.secrets["email"]["receiver_emails"]  # Liste des emails séparés par des virgules

# Configuration of the page
st.set_page_config(
    page_title="IKitchen Sales Report Generator",
    page_icon="🍽️",
    layout="wide"
)

# Application title
st.title("🍽️ IKitchen Sales Report Generator")

# Helper functions from your original code
def clean_amount(amount_str):
    """Clean and convert amount strings to float"""
    try:
        cleaned = str(amount_str).replace(',', '').replace(' ', '').strip()
        if cleaned == '' or cleaned.lower() == 'nan':
            return 0.0
        return float(cleaned)
    except:
        return 0.0

def parse_time(time_str):
    """Parse time string to time object"""
    if pd.isna(time_str):
        return None
    try:
        dt = datetime.strptime(str(time_str), '%Y-%m-%d %H:%M:%S')
        return dt.time()
    except:
        return None

def is_weekend(date_str):
    """Check if date is weekend"""
    try:
        dt = datetime.strptime(str(date_str), '%Y-%m-%d %H:%M:%S')
        return dt.weekday() >= 5  # 5=saturday, 6=sunday
    except:
        return False

def categorize_meal_period(sale_time):
    """Categorize sale time into meal periods"""
    if sale_time is None:
        return "Unknown"

    breakfast_start = time(6, 0)   # 6:00
    breakfast_end = time(12, 30)   # 12:30
    lunch_end = time(17, 0)        # 17:00

    if sale_time < breakfast_start:  # 0h00 to 5h59
        return "Dinner"
    elif breakfast_start <= sale_time < breakfast_end: # 6h00 to 12:29
        return "Breakfast"
    elif breakfast_end <= sale_time <= lunch_end:  # 12h29 to 17h00
        return "Lunch"
    else:  # after 17h00
        return "Dinner"

def extract_date_from_metadata(metadata_line):
    """Extract and format date from metadata line"""
    try:
        # Extract dates from metadata
        date_pattern = r'\d{2}-\d{2}-\d{4}'
        dates_found = re.findall(date_pattern, metadata_line)
        if dates_found:
            # Convert first date from DD-MM-YYYY to DD/MM/YYYY
            date_str = dates_found[0]
            return date_str.replace('-', '/')
        return datetime.now().strftime('%d/%m/%Y')
    except:
        return datetime.now().strftime('%d/%m/%Y')

def format_report_new_style(report_data, metadata_line):
    """Format report in the new requested style"""
    period_totals = report_data['period_totals']
    ordertype_totals = report_data['ordertype_totals']
    total_sales = report_data['total_sales']
    
    # Extract formatted date
    formatted_date = extract_date_from_metadata(metadata_line)
    
    # Build report lines
    report_lines = []
    report_lines.append(f"DATE : {formatted_date};")
    
    # Meal periods
    lunch_amount = period_totals.get('Lunch', 0.0)
    dinner_amount = period_totals.get('Dinner', 0.0)
    breakfast_amount = period_totals.get('Breakfast', 0.0)
    
    report_lines.append(f"Lunch sales (12:30pm to 5:00pm);{lunch_amount:,.2f}")
    report_lines.append(f"Dinner sales (5:00pm onwards);{dinner_amount:,.2f}")
    
    # Order types - standardize names
    delivery_amount = 0.0
    eatin_amount = 0.0
    takeaway_amount = 0.0
    
    for order_type, amount in ordertype_totals.items():
        order_type_lower = order_type.lower()
        if 'delivery' in order_type_lower :
            delivery_amount += amount
        elif 'eat in' in order_type_lower :
            eatin_amount += amount
        elif 'take away' in order_type_lower :
            takeaway_amount += amount
        else:
            # Default to eat in if not clear
            eatin_amount += amount
    
    report_lines.append(f"Delivery;{delivery_amount:,.2f}")
    report_lines.append(f"Eat in;{eatin_amount:,.2f}")
    report_lines.append(f"Take away;{takeaway_amount:,.2f}")
    
    # Weekend breakfast
    report_lines.append(f"Weekend (breakfast total amount sales until 12:30pm);{breakfast_amount:,.2f}")
    
    # Total
    report_lines.append(f"TOTAL SALES;{total_sales:,.2f}")
    
    return '\n'.join(report_lines)

def process_ikitchen_data(uploaded_file):
    """Process the iKitchen CSV file and generate report"""
    try:
        # Read the file content
        content = uploaded_file.getvalue().decode('utf-8')
        lines = content.split('\n')
        
        # Extract metadata from line 2 (index 1)
        if len(lines) < 4:
            return None, None, "File format is incorrect. Expected at least 4 lines."
        
        metadata_line = lines[1].strip()
        
        # Extract dates from metadata
        date_pattern = r'\d{2}-\d{2}-\d{4}'
        dates_found = re.findall(date_pattern, metadata_line)
        if dates_found:
            sales_date = str(dates_found[0])
        else:
            sales_date = ""
        
        # Read CSV data starting from line 4 (index 3)
        csv_content = '\n'.join(lines[3:])
        df = pd.read_csv(io.StringIO(csv_content))
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        # Filter valid sales
        valid_sales = df[df['Status'] == 'Ordered'].copy()
        
        # Clean amounts
        valid_sales['Amount_clean'] = valid_sales['Amount'].apply(clean_amount)
        
        # Process time and meal periods
        valid_sales['Sale_time'] = valid_sales['Sale date'].apply(parse_time)
        #valid_sales['Is_weekend'] = valid_sales['Sale date'].apply(is_weekend)
        valid_sales['Meal_period'] = valid_sales.apply(
            lambda row: categorize_meal_period(row['Sale_time']),
            axis=1
        )
        
        # Generate report
        period_totals = valid_sales.groupby('Meal_period')['Amount_clean'].sum()
        ordertype_totals = valid_sales.groupby('Ordertype name')['Amount_clean'].sum()
        total_sales = valid_sales['Amount_clean'].sum()
        
        report_data = {
            'metadata_line': metadata_line,
            'sales_date': sales_date,
            'valid_sales': valid_sales,
            'period_totals': period_totals,
            'ordertype_totals': ordertype_totals,
            'total_sales': total_sales
        }
        
        # Format report in new style
        final_report = format_report_new_style(report_data, metadata_line)
        report_data['final_report'] = final_report
        
        return report_data, valid_sales, None
        
    except Exception as e:
        return None, None, f"Error processing file: {str(e)}"

def send_email_to_multiple_recipients(sender_email, sender_password, receiver_emails_list, subject, body, csv_content, filename):
    """Send email with CSV attachment to multiple recipients"""
    try:
        # Create email message
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = ", ".join(receiver_emails_list)  # Join all recipients
        message["Subject"] = subject
        message.attach(MIMEText(body, "plain"))
        
        # Create CSV attachment
        csv_buffer = io.StringIO()
        csv_buffer.write(csv_content)
        csv_data = csv_buffer.getvalue().encode('utf-8')
        
        # Create the attachment
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(csv_data)
        encoders.encode_base64(attachment)
        attachment.add_header(
            'Content-Disposition',
            f'attachment; filename={filename}'
        )
        message.attach(attachment)
        
        # Send email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, sender_password)
            # Send to all recipients
            server.send_message(message, to_addrs=receiver_emails_list)
        
        return True, f"Email sent successfully to {len(receiver_emails_list)} recipients!"
        
    except Exception as e:
        return False, f"Error sending email: {str(e)}"

# File uploader with automatic processing
uploaded_file = st.file_uploader(
    "",
    type=['csv'],
    help="""**Expected CSV Format**: 
- Line 1: Headers or title
- Line 2: Metadata with dates
- Line 3: Empty or headers
- Line 4+: Sales data with columns including 'Status', 'Amount', 'Sale date', 'Ordertype name'"""
)

if uploaded_file is not None:
    try:
        st.success(f"✅ File '{uploaded_file.name}' uploaded successfully!")
        
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
                        # Generate email subject and filename
                        formatted_date = extract_date_from_metadata(report_data['metadata_line'])
                        email_subject = f"Daily sales report : {formatted_date}"
                        email_body = f"""Please find the daily sales report attached.

Report Summary:
- Date: {formatted_date}
- Total Sales: {report_data['total_sales']:,.2f}

Best regards,
IKitchen Sales Report System"""
                        
                        report_filename = f"sales_report_{report_data['sales_date']}.csv"
                        
                        # Send email
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
                
                # # Download option
                # st.subheader("💾 Download Report")
                # st.download_button(
                #     label="📥 Download CSV Report",
                #     data=report_data['final_report'],
                #     file_name=f"sales_report_{report_data['sales_date']}.csv",
                #     mime="text/csv"
                # )
    
    except Exception as e:
        st.error(f"❌ Error processing file: {str(e)}")

else:
    st.info("👆 Please upload your IKitchen sales CSV file - processing and email sending will be automatic!")

