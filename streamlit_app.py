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
receiver_email = st.secrets["email"]["receiver_email"]

# Configuration of the page
st.set_page_config(
    page_title="iKitchen Sales Report Generator",
    page_icon="🍽️",
    layout="wide"
)

# Application title
st.title("🍽️ iKitchen Sales Report Generator")
st.markdown("📁 Upload your sales CSV file and receive a formatted report by email!")

# # Sidebar for email configuration
# st.sidebar.header("📧 Email Configuration")
# sender_email = st.sidebar.text_input("Sender Email", value="emma.pierre96@gmail.com")
# sender_password = st.sidebar.text_input("Email Password (App Password)", type="password", 
#                                        help="Use your Gmail App Password, not your regular password")
# receiver_email = st.sidebar.text_input("Receiver Email", value="pierrelidia@gmail.com")

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
        sales_date = "_".join(dates_found)
        
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
        valid_sales['Is_weekend'] = valid_sales['Sale date'].apply(is_weekend)
        valid_sales['Meal_period'] = valid_sales.apply(
            lambda row: categorize_meal_period(row['Sale_time']),
            axis=1
        )
        
        # Generate report
        period_totals = valid_sales.groupby('Meal_period')['Amount_clean'].sum()
        ordertype_totals = valid_sales.groupby('Ordertype name')['Amount_clean'].sum()
        total_sales = valid_sales['Amount_clean'].sum()
        
        # Create report lines
        report_lines = []
        report_lines.append(f"Daily sales report : {metadata_line}")
        report_lines.append("")
        
        if 'Lunch' in period_totals:
            report_lines.append(f"Lunch sales (12:30pm to 5:00pm): {period_totals['Lunch']:,.2f}")
        
        if 'Dinner' in period_totals:
            report_lines.append(f"Dinner sales (5:00pm onwards): {period_totals['Dinner']:,.2f}")
        
        report_lines.append("")
        report_lines.append("Sales by order type:")
        for order_type, amount in ordertype_totals.items():
            report_lines.append(f"  {order_type}: {amount:,.2f}")
        report_lines.append("")
        
        weekend_str = "Weekend (breakfast total amount sales until 12:30pm): "
        if 'Breakfast' in period_totals:
            weekend_str += f" {period_totals['Breakfast']:,.2f}"
        else:
            weekend_str += "0.00"
        report_lines.append(weekend_str)
        report_lines.append("")
        report_lines.append(f"TOTAL SALES: {total_sales:,.2f}")
        
        final_report = '\n'.join(report_lines)
        
        return {
            'metadata_line': metadata_line,
            'sales_date': sales_date,
            'valid_sales': valid_sales,
            'period_totals': period_totals,
            'ordertype_totals': ordertype_totals,
            'total_sales': total_sales,
            'final_report': final_report,
            'report_lines': report_lines
        }, valid_sales, None
        
    except Exception as e:
        return None, None, f"Error processing file: {str(e)}"

def send_email_with_csv(sender_email, sender_password, receiver_email, subject, body, csv_content, filename):
    """Send email with CSV attachment"""
    try:
        # Create email message
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email
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
            server.send_message(message)
        
        return True, "Email sent successfully!"
        
    except Exception as e:
        return False, f"Error sending email: {str(e)}"

# File uploader
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
        
        # Process data button
        if st.button("🔄 Process Sales Data", type="primary"):
            with st.spinner("Processing sales data..."):
                report_data, df_processed, error_message = process_ikitchen_data(uploaded_file)
                
                if error_message:
                    st.error(f"❌ {error_message}")
                elif report_data is None:
                    st.error("❌ Failed to process the file")
                else:
                    st.success("✅ Sales data processed successfully!")
                    
                    # Display key metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Sales", f"{report_data['total_sales']:,.2f}")
                    with col2:
                        st.metric("Valid Orders", len(df_processed))
                    with col3:
                        st.metric("Sales Date", report_data['sales_date'])
                    
                    # Display period totals
                    st.subheader("📊 Sales by Meal Period")
                    period_df = pd.DataFrame({
                        'Meal Period': report_data['period_totals'].index,
                        'Total Sales': report_data['period_totals'].values
                    })
                    st.dataframe(period_df, use_container_width=True)
                    
                    # Display order type totals
                    st.subheader("🛒 Sales by Order Type")
                    ordertype_df = pd.DataFrame({
                        'Order Type': report_data['ordertype_totals'].index,
                        'Total Sales': report_data['ordertype_totals'].values
                    })
                    st.dataframe(ordertype_df, use_container_width=True)
                    
                    # Display the full report
                    st.subheader("📋 Complete Sales Report")
                    st.text_area("Report Content", report_data['final_report'], height=300)
                    
                    # Store in session state
                    st.session_state.report_data = report_data
                    st.session_state.processed_df = df_processed
        
        # Email sending section
        if 'report_data' in st.session_state:
            st.header("📧 Send Report by Email")
            
            report_data = st.session_state.report_data
            
            # Pre-fill email details
            email_subject = st.text_input(
                "Email Subject", 
                value=report_data['report_lines'][0]
            )
            email_body = st.text_area(
                "Email Body",
                value="Please find the sales report attached as CSV file.",
                height=100
            )
            
            # Report filename
            report_filename = st.text_input(
                "Report Filename",
                value=f"sales_report_{report_data['sales_date']}.csv"
            )
            
            # Send email button
            if st.button("📤 Send Email Report", type="secondary"):
                # Validate email configuration
                if not sender_email or not sender_password or not receiver_email:
                    st.error("⚠️ Please fill in all email configuration fields in the sidebar!")
                else:
                    with st.spinner("Sending email..."):
                        # Send email
                        success, message = send_email_with_csv(
                            sender_email=sender_email,
                            sender_password=sender_password,
                            receiver_email=receiver_email,
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
            
            # Download option
            st.subheader("💾 Download Report")
            st.download_button(
                label="📥 Download CSV Report",
                data=report_data['final_report'],
                file_name=report_filename,
                mime="text/csv"
            )
    
    except Exception as e:
        st.error(f"❌ Error processing file: {str(e)}")

else:
    st.info("👆 Please upload your iKitchen sales CSV file to get started")

# # Instructions
# st.markdown("---")
# st.markdown("📝 **How to use:**")
# st.markdown("""
# 1. **Configure Email**: Fill in your Gmail credentials in the sidebar (use App Password for Gmail)
# 2. **Upload CSV**: Select your iKitchen sales CSV file
# 3. **Process Data**: Click 'Process Sales Data' to generate the report
# 4. **Review Results**: Check the sales metrics and report content
# 5. **Send Email**: Configure email details and send the report
# 6. **Download**: Optionally download the report directly as CSV

# **Expected CSV Format**: 
# - Line 1: Headers or title
# - Line 2: Metadata with dates
# - Line 3: Empty or headers
# - Line 4+: Sales data with columns including 'Status', 'Amount', 'Sale date', 'Ordertype name'
# """)

# st.markdown("⚠️ **Security Note**: Use Gmail App Password, not your regular password. Go to Google Account Settings → Security → App Passwords")
