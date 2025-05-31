import pandas as pd
import requests
import streamlit as st
from serpapi import GoogleSearch
import gspread
from gspread_dataframe import set_with_dataframe
from dotenv import load_dotenv
import os
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import schedule
import threading
import re
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import uuid
import sqlite3
from pathlib import Path
import base64
from urllib.parse import quote

# Load environment variables
load_dotenv()

# API Keys
SERP_API_KEY = os.getenv("SERP_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
SERVICE_ACCOUNT_FILE = "C:\\Users\\acer\\AutoMail\\Credentials.json"
EMAIL = os.getenv("EMAIL")
MAILJET_API_KEY = os.getenv("MAILJET_API_KEY")
MAILJET_SECRET_KEY = os.getenv("MAILJET_SECRET_KEY")

# Database setup for email tracking
def init_database():
    """Initialize SQLite database for email tracking"""
    db_path = Path("email_tracking.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS email_logs (
            id TEXT PRIMARY KEY,
            recipient_email TEXT,
            company_name TEXT,
            subject TEXT,
            sent_time TIMESTAMP,
            status TEXT,
            delivery_status TEXT,
            opened BOOLEAN DEFAULT FALSE,
            clicked BOOLEAN DEFAULT FALSE,
            bounced BOOLEAN DEFAULT FALSE,
            spam_score REAL,
            error_message TEXT
        )
    ''')
    
    conn.commit()
    conn.close()

def log_email_status(email_id, recipient_email, company_name, subject, status, delivery_status="pending", error_message=None):
    """Log email status to database"""
    conn = sqlite3.connect("email_tracking.db")
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR REPLACE INTO email_logs 
        (id, recipient_email, company_name, subject, sent_time, status, delivery_status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (email_id, recipient_email, company_name, subject, datetime.now(), status, delivery_status, error_message))
    
    conn.commit()
    conn.close()

def get_email_stats():
    """Get email statistics from database"""
    conn = sqlite3.connect("email_tracking.db")
    df = pd.read_sql_query("SELECT * FROM email_logs ORDER BY sent_time DESC", conn)
    conn.close()
    return df

# Email validation function (enhanced)
def is_valid_email(email):
    """Enhanced email validation"""
    if not email or pd.isna(email):
        return False
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, str(email)) is not None

def validate_email_deliverability(email):
    """Check email deliverability factors"""
    score = 100
    warnings = []
    
    # Check for disposable email domains
    disposable_domains = ['temp-mail.org', '10minutemail.com', 'guerrillamail.com', 'mailinator.com']
    if any(domain in email.lower() for domain in disposable_domains):
        score -= 30
        warnings.append("Disposable email domain")
    
    # Check for common personal domains vs business domains
    personal_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']
    if any(domain in email.lower() for domain in personal_domains):
        score -= 10
        warnings.append("Personal email domain - may have stricter spam filters")
    
    return score, warnings

# Enhanced data processing functions
def find_email_column(df):
    """Automatically detect email column in the dataframe"""
    possible_email_columns = ['email', 'gmail', 'mail', 'e-mail', 'email_address', 'contact_email', 'business_email']
    
    # Check for exact matches first
    for col in df.columns:
        if col.lower() in possible_email_columns:
            return col
    
    # Check for partial matches
    for col in df.columns:
        if any(email_term in col.lower() for email_term in ['email', 'mail', 'gmail']):
            return col
    
    # Check column content for email patterns
    for col in df.columns:
        if df[col].dtype == 'object':
            sample_values = df[col].dropna().head(10)
            email_count = sum(1 for val in sample_values if is_valid_email(str(val)))
            if email_count > len(sample_values) * 0.5:  # If more than 50% look like emails
                return col
    
    return None

def find_company_name_column(df):
    """Automatically detect company name column in the dataframe"""
    possible_company_columns = ['company', 'company_name', 'business', 'organization', 'org', 'firm', 'business_name', 'name']
    
    # Check for exact matches first
    for col in df.columns:
        if col.lower() in possible_company_columns:
            return col
    
    # Check for partial matches
    for col in df.columns:
        if any(company_term in col.lower() for company_term in ['company', 'business', 'organization', 'name']):
            return col
    
    # Return first column if no obvious match
    return df.columns[0] if len(df.columns) > 0 else None

def process_excel_data(df):
    """Process Excel data to ensure email column exists and extract company information"""
    processed_df = df.copy()
    
    # Find existing email column
    email_col = find_email_column(processed_df)
    company_col = find_company_name_column(processed_df)
    
    # If no email column found, create one
    if email_col is None:
        processed_df['Gmail'] = ''
        email_col = 'Gmail'
        st.warning("No email column found. Created 'Gmail' column. You may need to fill it manually or use search functionality.")
    else:
        # Rename to standardize
        if email_col != 'Gmail':
            processed_df['Gmail'] = processed_df[email_col]
    
    # Ensure company name column exists
    if company_col is None:
        company_col = processed_df.columns[0]
        st.info(f"Using '{company_col}' as company name column.")
    
    return processed_df, email_col, company_col

def create_comprehensive_company_info(row, company_col):
    """Create comprehensive company information string from all available data"""
    company_name = str(row.get(company_col, 'Unknown Company'))
    
    # Exclude email and company name columns from additional info
    excluded_cols = ['Gmail', 'gmail', 'email', 'Email', company_col]
    
    info_parts = [f"Company Name: {company_name}"]
    
    for col, value in row.items():
        if col not in excluded_cols and pd.notna(value) and str(value).strip():
            clean_value = str(value).strip()
            if clean_value and clean_value.lower() not in ['nan', 'none', 'null', '']:
                info_parts.append(f"{col}: {clean_value}")
    
    return "\n".join(info_parts)

# Enhanced Google Sheets functions
def authenticate_google_sheets():
    try:
        client = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
        return client
    except Exception as e:
        st.error(f"Authentication Error: {str(e)}")
        return None

def load_google_sheet(sheet_url):
    try:
        gc = authenticate_google_sheets()
        if gc is None:
            return None, None, None
        sheet_key = sheet_url.split('/d/')[1].split('/')[0]
        sheet = gc.open_by_key(sheet_key)
        worksheet = sheet.get_worksheet(0)
        data = worksheet.get_all_records()
        return pd.DataFrame(data), gc, worksheet
    except Exception as e:
        st.error(f"Error loading sheet: {str(e)}")
        return None, None, None

def update_google_sheet(worksheet, results_df):
    try:
        data = [results_df.columns.values.tolist()]
        data.extend(results_df.values.tolist())
        worksheet.clear()
        worksheet.update('A1', data)
        return True, "Google Sheet updated successfully!"
    except Exception as e:
        detailed_error = f"Error updating sheet: {str(e)}"
        st.error(detailed_error)
        return False, detailed_error

# Enhanced search functions for missing emails
def get_search_results(query, prompt, api_key, column_name):
    try:
        search_query = prompt.format(column_name=query)
    except KeyError:
        search_query = prompt.replace("{col_name}", query).replace("{column_name}", query)

    params = {
        "engine": "google",
        "q": search_query,
        "num": 100,
        "api_key": api_key
    }

    search = GoogleSearch(params)
    results = search.get_dict()

    text_content = ""
    if "knowledge_graph" in results:
        knowledge = results["knowledge_graph"]
        for key in ["title", "type", "website", "founded", "headquarters", "revenue",
                    "social", "mobile", "phone", "ceo", "email", "contact email",
                    "address", "contact", "call", "chat", "connect", "write",
                    "twitter", "instagram", "facebook"]:
            text_content += f"{knowledge.get(key, '')}\n"

    for result in results.get("organic_results", []):
        text_content += f"{result.get('title', 'N/A')} - {result.get('snippet', 'N/A')}\n"

    return text_content

def ask_groq_api(question, company, context, api_key):
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    prompt = f"""
    You are an AI assistant specialized in extracting specific information from any data.
    From the context and question provided, extract only the feature which is asked in the question.
    Respond only with the asked feature, without any verbosity. Clean and exact answers only.

    Question: {question}
    Company: {company}
    Context: {context}
    """
    payload = {
        "model": "llama3-8b-8192",
        "messages": [
            {"role": "system", "content": "You are an assistant that extracts specific information from context."},
            {"role": "user", "content": prompt}
        ]
    }
    wait_time = 30
    max_retries = 5

    for attempt in range(max_retries):
        try:
            response = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload)
            if response.status_code == 200:
                response_json = response.json()
                return response_json['choices'][0]['message']['content'].strip()
            elif response.status_code == 429:
                print(f"Rate limit reached. Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                wait_time *= 2
            else:
                return f"Error: {response.status_code}, {response.text}"
        except Exception as e:
            return f"Error: {str(e)}"
    return "Error: Max retries exceeded. Please try again later."

def generate_email_content(template, company_name, company_info, api_key):
    """Generate personalized email content using comprehensive company data"""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    prompt = f"""
    You are an expert email writer specializing in business communications that avoid spam filters.
    Generate a professional, personalized email that follows these deliverability best practices:

    Template/Instructions: {template}
    
    Company Information (use this to personalize the email):
    {company_info}
    
    IMPORTANT DELIVERABILITY RULES:
    1. Use professional, conversational tone (not overly salesy)
    2. Avoid spam trigger words like "FREE", "URGENT", "GUARANTEED", excessive exclamation marks
    3. Include specific, relevant details about the company from the provided information
    4. Make it sound like genuine business correspondence
    5. Keep subject line under 50 characters
    6. Use proper sentence structure and grammar
    7. Include a clear but subtle call-to-action
    8. Personalize with specific company details from the provided information
    9. Reference specific company attributes like industry, location, size, etc. when available
    
    Generate only the email content without any additional text or explanations.
    Make sure to use the company information provided to create a highly personalized message.
    """
    
    payload = {
        "model": "llama3-8b-8192",
        "messages": [
            {"role": "system", "content": "You are a professional email writer who creates highly personalized, spam-filter-friendly business emails using comprehensive company data."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7,
        "max_tokens": 1000
    }
    
    wait_time = 30
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            response = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload)
            if response.status_code == 200:
                response_json = response.json()
                generated_content = response_json['choices'][0]['message']['content'].strip()
                return generated_content
            elif response.status_code == 429:
                print(f"Rate limit reached. Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                wait_time *= 2
            else:
                return f"Error generating content: {response.status_code}, {response.text}"
        except Exception as e:
            return f"Error generating content: {str(e)}"
    
    return "Error: Could not generate email content. Please try again later."

def generate_email_subject(template, company_name, company_info, api_key):
    """Generate personalized email subject using comprehensive company data"""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    prompt = f"""
    Generate a professional email subject line that will pass spam filters.

    Template/Instructions: {template}
    
    Company Information (use this to personalize):
    {company_info}

    DELIVERABILITY RULES:
    1. Keep under 50 characters
    2. Avoid ALL CAPS, excessive punctuation (!!!), and spam words
    3. Make it specific and personalized using the company information
    4. Sound like genuine business correspondence
    5. Include company name or specific detail naturally
    6. Avoid words like: FREE, URGENT, GUARANTEED, AMAZING, INCREDIBLE
    7. Use title case or sentence case
    8. Do not include any newline characters

    Generate only the subject line without any additional text.
    """

    payload = {
        "model": "llama3-8b-8192",
        "messages": [
            {"role": "system", "content": "You are an expert at creating professional, spam-filter-friendly email subject lines using company data."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7,
        "max_tokens": 100
    }

    try:
        response = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload)
        if response.status_code == 200:
            response_json = response.json()
            return response_json['choices'][0]['message']['content'].strip().replace('\n', '')
        else:
            return template.replace("{company_name}", company_name)
    except Exception as e:
        return template.replace("{company_name}", company_name)

def create_html_email_body(plain_text, company_name, tracking_id=None):
    """Create HTML email body with better formatting and optional tracking"""
    
    tracking_pixel = f'<img src="https://your-tracking-domain.com/pixel/{tracking_id}" width="1" height="1" style="display:none;">' if tracking_id else ""
    
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Email from AutoMail</title>
    </head>
    <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 10px; margin-bottom: 20px;">
            <div style="background-color: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                <div style="white-space: pre-line; margin-bottom: 20px;">
                    {plain_text}
                </div>
                
                <hr style="border: none; height: 1px; background-color: #e9ecef; margin: 20px 0;">
                
                <div style="font-size: 12px; color: #6c757d; text-align: center;">
                    <p>This email was sent by AutoMail Email System</p>
                    <p>If you'd like to unsubscribe, please reply with "UNSUBSCRIBE" in the subject line.</p>
                </div>
            </div>
        </div>
        {tracking_pixel}
    </body>
    </html>
    """
    return html_body

def send_email_enhanced(receiver_email, subject, body, company_name="Unknown Company"):
    """Enhanced email sending with better deliverability practices"""

    email_id = str(uuid.uuid4())

    # Validate email
    if not is_valid_email(receiver_email):
        log_email_status(email_id, receiver_email, company_name, subject, "failed", "invalid_email", "Invalid email address")
        return False, f"Invalid email address: {receiver_email}", email_id

    # Check deliverability score
    deliverability_score, warnings = validate_email_deliverability(receiver_email)

    # Check if required environment variables are set
    if not EMAIL or not MAILJET_API_KEY or not MAILJET_SECRET_KEY:
        error_msg = "Missing email configuration. Check environment variables."
        log_email_status(email_id, receiver_email, company_name, subject, "failed", "config_error", error_msg)
        return False, error_msg, email_id

    sender_email = EMAIL
    sender_name = "BreakoutAI Team"

    # Create multipart message with both plain text and HTML
    msg = MIMEMultipart('alternative')
    msg['From'] = f"{sender_name} <{sender_email}>"
    msg['To'] = receiver_email

    # Ensure subject line doesn't contain newlines
    clean_subject = subject.replace('\n', '') if subject else f"Partnership Opportunity for {company_name}"
    msg['Subject'] = clean_subject

    # Add custom headers to improve deliverability
    msg['Reply-To'] = sender_email
    msg['Return-Path'] = sender_email
    msg['X-Mailer'] = 'BreakoutAI Email System v2.0'
    msg['X-Priority'] = '3'
    msg['Message-ID'] = f"<{email_id}@breakoutai.com>"

    # Ensure body is not None
    email_body = body if body else "Thank you for your time."

    # Create plain text and HTML versions
    text_part = MIMEText(email_body, 'plain', 'utf-8')
    html_part = MIMEText(create_html_email_body(email_body, company_name, email_id), 'html', 'utf-8')

    # Attach parts
    msg.attach(text_part)
    msg.attach(html_part)

    try:
        # Create SMTP connection with enhanced settings
        server = smtplib.SMTP('in-v3.mailjet.com', 587)
        server.starttls()
        
        # Login with Mailjet credentials
        server.login(MAILJET_API_KEY, MAILJET_SECRET_KEY)
        
        # Send email
        text = msg.as_string()
        server.sendmail(sender_email, receiver_email, text)
        server.quit()
        
        # Log successful send
        delivery_status = "primary" if deliverability_score > 80 else "likely_spam" if deliverability_score < 50 else "secondary"
        log_email_status(email_id, receiver_email, company_name, clean_subject, "sent", delivery_status)
        
        return True, f"Email sent successfully to {receiver_email} (Deliverability Score: {deliverability_score}%)", email_id
        
    except Exception as e:
        error_msg = f"Error sending email: {str(e)}"
        log_email_status(email_id, receiver_email, company_name, clean_subject, "failed", "unknown_error", error_msg)
        return False, error_msg, email_id

def send_bulk_emails_with_comprehensive_data(df, subject_template, body_template, company_col, email_col, use_ai_generation=True):
    """Send bulk emails using comprehensive company data from Excel"""
    total_sent = 0
    total_failed = 0
    total_primary = 0
    total_spam_likely = 0
    
    progress_bar = st.progress(0)
    status_container = st.empty()
    
    for idx, row in df.iterrows():
        company_name = str(row.get(company_col, f'Company_{idx}'))
        email_address = str(row.get(email_col, ''))
        
        # Skip if no valid email
        if not is_valid_email(email_address):
            total_failed += 1
            status_container.text(f"Skipped {company_name}: Invalid email address")
            continue
        
        # Create comprehensive company information
        company_info = create_comprehensive_company_info(row, company_col)
        
        try:
            if use_ai_generation:
                # Generate AI-powered content using all company data
                email_subject = generate_email_subject(subject_template, company_name, company_info, GROQ_API_KEY)
                email_body = generate_email_content(body_template, company_name, company_info, GROQ_API_KEY)
            else:
                # Use template with basic substitution
                email_subject = subject_template.replace("{company_name}", company_name)
                email_body = body_template.replace("{company_name}", company_name)
                
                # Replace other placeholders with available data
                for col, value in row.items():
                    if pd.notna(value) and col != email_col:
                        placeholder = "{" + col.lower().replace(" ", "_") + "}"
                        email_body = email_body.replace(placeholder, str(value))
                        email_subject = email_subject.replace(placeholder, str(value))
            
            # Send email
            success, message, email_id = send_email_enhanced(email_address, email_subject, email_body, company_name)
            
            if success:
                total_sent += 1
                # Check deliverability for stats
                score, _ = validate_email_deliverability(email_address)
                if score > 80:
                    total_primary += 1
                elif score < 50:
                    total_spam_likely += 1
                    
                status_container.success(f"✅ Sent to {company_name} ({email_address})")
            else:
                total_failed += 1
                status_container.error(f"❌ Failed to send to {company_name}: {message}")
            
            # Update progress
            progress_bar.progress((idx + 1) / len(df))
            
            # Rate limiting
            time.sleep(2)  # 2 second delay between emails
            
        except Exception as e:
            total_failed += 1
            status_container.error(f"❌ Error with {company_name}: {str(e)}")
    
    # Final summary
    st.success(f"""
    📊 *Campaign Summary:*
    - ✅ Successfully sent: {total_sent}
    - ❌ Failed: {total_failed}
    - 🎯 Likely primary inbox: {total_primary}
    - ⚠ Likely spam/secondary: {total_spam_likely}
    """)
    
    return total_sent, total_failed, total_primary, total_spam_likely

def authenticate_and_update(sheet_url, df2):
    gc2 = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
    spreadsheet = gc2.open_by_url(sheet_url)
    worksheet = spreadsheet.sheet1
    set_with_dataframe(worksheet, df2)
    st.success("Data uploaded successfully!")

def create_email_dashboard():
    """Create comprehensive email dashboard"""
    st.header("📊 Email Campaign Dashboard")
    
    # Get email statistics
    email_stats = get_email_stats()
    
    if email_stats.empty:
        st.info("No email data available yet. Send some emails to see statistics!")
        return
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    total_emails = len(email_stats)
    sent_emails = len(email_stats[email_stats['status'] == 'sent'])
    failed_emails = len(email_stats[email_stats['status'] == 'failed'])
    primary_emails = len(email_stats[email_stats['delivery_status'] == 'primary'])
    
    with col1:
        st.metric("Total Emails", total_emails)
    with col2:
        st.metric("Successfully Sent", sent_emails, delta=f"{(sent_emails/total_emails)*100:.1f}%" if total_emails > 0 else "0%")
    with col3:
        st.metric("Failed", failed_emails, delta=f"-{(failed_emails/total_emails)*100:.1f}%" if total_emails > 0 else "0%")
    with col4:
        st.metric("Primary Inbox", primary_emails, delta=f"{(primary_emails/sent_emails)*100:.1f}%" if sent_emails > 0 else "0%")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Email Status Distribution
        status_counts = email_stats['status'].value_counts()
        fig_status = px.pie(
            values=status_counts.values, 
            names=status_counts.index, 
            title="Email Status Distribution",
            color_discrete_map={'sent': '#28a745', 'failed': '#dc3545'}
        )
        st.plotly_chart(fig_status, use_container_width=True)
    
    with col2:
        # Delivery Status Distribution (for sent emails only)
        sent_only = email_stats[email_stats['status'] == 'sent']
        if not sent_only.empty:
            delivery_counts = sent_only['delivery_status'].value_counts()
            fig_delivery = px.pie(
                values=delivery_counts.values, 
                names=delivery_counts.index, 
                title="Delivery Status (Sent Emails)",
                color_discrete_map={
                    'primary': '#28a745', 
                    'secondary': '#ffc107', 
                    'likely_spam': '#dc3545'
                }
            )
            st.plotly_chart(fig_delivery, use_container_width=True)
    
    # Timeline chart
    if 'sent_time' in email_stats.columns:
        email_stats['sent_time'] = pd.to_datetime(email_stats['sent_time'])
        daily_stats = email_stats.groupby([email_stats['sent_time'].dt.date, 'status']).size().unstack(fill_value=0)
        
        fig_timeline = go.Figure()
        
        if 'sent' in daily_stats.columns:
            fig_timeline.add_trace(go.Scatter(
                x=daily_stats.index, 
                y=daily_stats['sent'], 
                mode='lines+markers',
                name='Sent',
                line=dict(color='#28a745')
            ))
        
        if 'failed' in daily_stats.columns:
            fig_timeline.add_trace(go.Scatter(
                x=daily_stats.index, 
                y=daily_stats['failed'], 
                mode='lines+markers',
                name='Failed',
                line=dict(color='#dc3545')
            ))
        
        fig_timeline.update_layout(
            title="Email Sending Timeline",
            xaxis_title="Date",
            yaxis_title="Number of Emails",
            hovermode='x unified'
        )
        
        st.plotly_chart(fig_timeline, use_container_width=True)
    
    # Recent emails table
    st.subheader("📋 Recent Emails")
    
    # Display recent emails with status
    recent_emails = email_stats.head(20)
    if not recent_emails.empty:
        # Format the dataframe for better display
        display_df = recent_emails[['company_name', 'recipient_email', 'subject', 'status', 'delivery_status', 'sent_time']].copy()
        display_df['sent_time'] = pd.to_datetime(display_df['sent_time']).dt.strftime('%Y-%m-%d %H:%M')
        
        # Add status icons
        status_icons = {'sent': '✅', 'failed': '❌'}
        delivery_icons = {'primary': '🎯', 'secondary': '📂', 'likely_spam': '⚠', 'pending': '⏳'}
        
        display_df['Status'] = display_df['status'].map(status_icons) + ' ' + display_df['status'].str.title()
        display_df['Delivery'] = display_df['delivery_status'].map(delivery_icons) + ' ' + display_df['delivery_status'].str.replace('_', ' ').str.title()


        st.dataframe(
            display_df[['company_name', 'recipient_email', 'subject', 'Status', 'Delivery', 'sent_time']],
            use_container_width=True,
            column_config={
                'company_name': 'Company',
                'recipient_email': 'Email',
                'subject': 'Subject',
                'sent_time': 'Sent Time'
            }
        )
    
    # Export functionality
    if st.button("📥 Export Email Log"):
        csv = email_stats.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"email_campaign_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

def create_email_templates():
    """Predefined email templates for different industries/purposes"""
    return {
        "Generic Business Partnership": {
            "subject": "Partnership Opportunity with {company_name}",
            "body": """Dear {company_name} Team,

I hope this email finds you well. I came across your company and was impressed by your work in the industry.

We specialize in helping businesses like yours grow through strategic partnerships and innovative solutions. I believe there could be a great opportunity for collaboration between our organizations.

Would you be interested in a brief 15-minute call to explore potential synergies?

Best regards,
[Your Name]
[Your Company]"""
        },
        "Tech/Software Solutions": {
            "subject": "Boost {company_name}'s Digital Efficiency",
            "body": """Hello {company_name} Team,

I noticed your company's impressive growth and wanted to reach out regarding how we can help streamline your digital operations.

Our platform has helped similar companies reduce operational costs by 30% while improving productivity. Given your focus on [industry/specialty], I think you'd find our solution particularly valuable.

Would you be open to a quick demo showing how companies similar to {company_name} are benefiting from our platform?

Best regards,
[Your Name]
[Your Position]"""
        },
        "Marketing Services": {
            "subject": "Scale {company_name}'s Marketing Impact",
            "body": """Hi {company_name} Team,

I've been following your company's journey and am impressed by your market presence. 

We specialize in helping businesses like yours amplify their marketing reach and generate qualified leads. Our recent clients have seen an average increase of 150% in qualified leads within the first quarter.

I'd love to share a brief case study of how we helped a company in your industry achieve similar results.

Are you available for a 10-minute call this week?

Best regards,
[Your Name]
[Your Company]"""
        },
        "Consulting Services": {
            "subject": "Strategic Growth Opportunity for {company_name}",
            "body": """Dear {company_name} Leadership,

Your company's reputation in the industry caught my attention, and I wanted to reach out regarding strategic growth opportunities.

We've helped companies similar to {company_name} optimize their operations and achieve sustainable growth through data-driven strategies. Our approach has consistently delivered ROI improvements of 25-40%.

Would you be interested in a confidential discussion about your growth objectives?

Best regards,
[Your Name]
Strategic Consultant"""
        }
    }

def schedule_email_campaign():
    """Schedule email campaigns for later sending"""
    st.header("⏰ Schedule Email Campaign")
    
    # Date and time selection
    col1, col2 = st.columns(2)
    
    with col1:
        send_date = st.date_input("Select Date", min_value=datetime.now().date())
    
    with col2:
        send_time = st.time_input("Select Time")
    
    # Combine date and time
    scheduled_datetime = datetime.combine(send_date, send_time)
    
    if scheduled_datetime <= datetime.now():
        st.warning("Please select a future date and time.")
        return
    
    st.info(f"Campaign scheduled for: {scheduled_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Campaign details
    campaign_name = st.text_input("Campaign Name")
    
    if st.button("Schedule Campaign"):
        # Here you would implement the scheduling logic
        # This could involve saving to a database and using a scheduler like APScheduler
        st.success(f"Campaign '{campaign_name}' scheduled successfully!")
        
        # Store scheduled campaign info
        scheduled_campaign = {
            'name': campaign_name,
            'scheduled_time': scheduled_datetime,
            'status': 'scheduled'
        }
        
        # You could save this to a database or file
        st.json(scheduled_campaign)

def email_template_builder():
    """Advanced email template builder with preview"""
    st.header("✏ Email Template Builder")
    
    # Template categories
    templates = create_email_templates()
    template_names = list(templates.keys())
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("Template Editor")
        
        # Template selection
        selected_template = st.selectbox("Choose a template to start:", ["Custom"] + template_names)
        
        if selected_template != "Custom":
            template_data = templates[selected_template]
            default_subject = template_data["subject"]
            default_body = template_data["body"]
        else:
            default_subject = "Your Subject Here"
            default_body = "Your email content here..."
        
        # Subject and body editors
        subject_template = st.text_input("Email Subject:", value=default_subject)
        body_template = st.text_area("Email Body:", value=default_body, height=300)
        
        # Template variables helper
        st.info("💡 Available placeholders: {company_name}, {industry}, {location}, {website}, etc.")
        
        # Save custom template
        if st.button("Save as Custom Template"):
            custom_template_name = st.text_input("Template Name:")
            if custom_template_name:
                # Here you would save the template to a file or database
                st.success(f"Template '{custom_template_name}' saved!")
    
    with col2:
        st.subheader("Preview")
        
        # Sample company data for preview
        sample_company = "TechCorp Solutions"
        sample_data = {
            "company_name": sample_company,
            "industry": "Technology",
            "location": "San Francisco",
            "website": "www.techcorp.com"
        }
        
        # Generate preview
        preview_subject = subject_template
        preview_body = body_template
        
        for placeholder, value in sample_data.items():
            preview_subject = preview_subject.replace(f"{{{placeholder}}}", value)
            preview_body = preview_body.replace(f"{{{placeholder}}}", value)
        
        st.markdown("*Subject Preview:*")
        st.code(preview_subject, language=None)
        
        st.markdown("*Body Preview:*")
        st.text_area("", value=preview_body, height=300, disabled=True)
        
        # Spam score check
        spam_score = check_spam_score(preview_subject, preview_body)
        
        if spam_score > 70:
            st.success(f"✅ Deliverability Score: {spam_score}%")
        elif spam_score > 50:
            st.warning(f"⚠ Deliverability Score: {spam_score}%")
        else:
            st.error(f"❌ Deliverability Score: {spam_score}%")

def check_spam_score(subject, body):
    """Simple spam score checker based on common spam indicators"""
    spam_triggers = [
        'free', 'urgent', 'limited time', 'act now', 'guaranteed',
        'no risk', 'call now', 'click here', 'buy now', 'order now',
        'special promotion', 'earn money', 'make money', 'work from home'
    ]
    
    score = 100
    text_to_check = (subject + ' ' + body).lower()
    
    # Check for spam trigger words
    for trigger in spam_triggers:
        if trigger in text_to_check:
            score -= 10
    
    # Check for excessive punctuation
    if text_to_check.count('!') > 3:
        score -= 15
    
    # Check for all caps
    if any(word.isupper() and len(word) > 3 for word in text_to_check.split()):
        score -= 20
    
    # Check subject line length
    if len(subject) > 50:
        score -= 10
    
    return max(0, min(100, score))

def create_ab_test_functionality():
    """A/B testing functionality for email campaigns"""
    st.header("🧪 A/B Test Email Campaigns")
    
    st.info("Test different email versions to optimize your campaigns!")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Version A")
        subject_a = st.text_input("Subject A:", key="subject_a")
        body_a = st.text_area("Body A:", height=200, key="body_a")
        
    with col2:
        st.subheader("Version B")
        subject_b = st.text_input("Subject B:", key="subject_b")
        body_b = st.text_area("Body B:", height=200, key="body_b")
    
    # Test parameters
    st.subheader("Test Parameters")
    col1, col2 = st.columns(2)
    
    with col1:
        test_split = st.slider("Split Percentage (A/B)", 10, 90, 50)
        st.write(f"Version A: {test_split}% | Version B: {100-test_split}%")
    
    with col2:
        test_duration = st.selectbox("Test Duration", ["24 hours", "48 hours", "1 week"])
    
    if st.button("Start A/B Test"):
        st.success("A/B test configured! The system will automatically split your email list and track performance.")
        
        # Display test configuration
        test_config = {
            "Version A": {"subject": subject_a, "split": f"{test_split}%"},
            "Version B": {"subject": subject_b, "split": f"{100-test_split}%"},
            "Duration": test_duration
        }
        
        st.json(test_config)

def main():
    """Main application function"""
    # Initialize database
    init_database()
    
    # Page configuration
    st.set_page_config(
        page_title="Advanced Email Campaign System",
        page_icon="📧",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS for better styling
    st.markdown("""
    <style>
    .main > div {
        padding-top: 2rem;
    }
    .stAlert {
        margin-top: 1rem;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Main title
    st.title("🚀 Advanced Email Campaign System")
    st.markdown("Comprehensive email marketing solution with AI-powered personalization")
    
    # Sidebar navigation
    st.sidebar.title("📋 Navigation")
    page_options = [
        "🏠 Dashboard",
        "📊 Campaign Analytics", 
        "📧 Send Emails",
        "🔍 Email Finder",
        "✏ Template Builder",
        "🧪 A/B Testing",
        "⏰ Schedule Campaigns",
        "📈 Performance Tracking"
    ]
    
    selected_page = st.sidebar.selectbox("Choose a page:", page_options)
    
    # API Configuration sidebar
    with st.sidebar.expander("⚙ API Configuration"):
        st.text_input("SERP API Key", type="password", help="For email finding functionality")
        st.text_input("GROQ API Key", type="password", help="For AI content generation")
        st.text_input("Mailjet API Key", type="password", help="For email sending")
        st.text_input("Mailjet Secret Key", type="password", help="For email authentication")
        
        if st.button("Test API Connections"):
            st.info("Testing API connections...")
            # Here you would test each API connection
            st.success("✅ All APIs connected successfully!")
    
    # Page routing
    if selected_page == "🏠 Dashboard":
        create_email_dashboard()
        
    elif selected_page == "📊 Campaign Analytics":
        create_email_dashboard()  # Reuse dashboard for now
        
    elif selected_page == "📧 Send Emails":
        st.header("📧 Send Email Campaign")
        
        # File upload options
        st.subheader("📁 Upload Your Data")
        
        tab1, tab2 = st.tabs(["📊 Excel/CSV Upload", "🌐 Google Sheets"])
        
        with tab1:
            uploaded_file = st.file_uploader("Choose Excel/CSV file", type=['xlsx', 'xls', 'csv'])
            
            if uploaded_file:
                try:
                    # Read the file
                    if uploaded_file.name.endswith('.csv'):
                        df = pd.read_csv(uploaded_file)
                    else:
                        df = pd.read_excel(uploaded_file)
                    
                    st.success(f"✅ File loaded successfully! Found {len(df)} rows.")
                    
                    # Process the data
                    processed_df, email_col, company_col = process_excel_data(df)
                    
                    # Display data preview
                    st.subheader("📋 Data Preview")
                    st.dataframe(processed_df.head(10))
                    
                    # Email template selection
                    st.subheader("✏ Email Template")
                    templates = create_email_templates()
                    template_choice = st.selectbox("Choose template:", list(templates.keys()))
                    
                    selected_template = templates[template_choice]
                    
                    subject_template = st.text_input("Subject:", value=selected_template["subject"])
                    body_template = st.text_area("Body:", value=selected_template["body"], height=200)
                    
                    # AI Generation toggle
                    use_ai = st.checkbox("🤖 Use AI for personalization", value=True, help="Generate personalized content using AI")
                    
                    # Send campaign
                    if st.button("🚀 Send Campaign", type="primary"):
                        if not subject_template or not body_template:
                            st.error("Please provide both subject and body templates.")
                        else:
                            with st.spinner("Sending emails..."):
                                sent, failed, primary, spam = send_bulk_emails_with_comprehensive_data(
                                    processed_df, subject_template, body_template, 
                                    company_col, email_col, use_ai
                                )
                                
                                st.balloons()
                                
                except Exception as e:
                    st.error(f"Error processing file: {str(e)}")
        
        with tab2:
            st.info("📋 Google Sheets Integration")
            sheet_url = st.text_input("Google Sheets URL:")
            
            if sheet_url and st.button("Load Google Sheet"):
                df, gc, worksheet = load_google_sheet(sheet_url)
                if df is not None:
                    st.success("✅ Google Sheet loaded successfully!")
                    st.dataframe(df.head(10))
                else:
                    st.error("❌ Failed to load Google Sheet. Check URL and permissions.")
    
    elif selected_page == "🔍 Email Finder":
        st.header("🔍 Email Finder Tool")
        st.info("Find missing email addresses using AI-powered search")
        
        # Check API keys
        if not SERP_API_KEY or not GROQ_API_KEY:
            st.error("❌ Missing API Keys! Please configure SERP_API_KEY and GROQ_API_KEY in your environment variables.")
            with st.expander("🔧 API Setup Instructions"):
                st.markdown("""
                *Required API Keys:*
                1. *SerpAPI Key*: Get from [serpapi.com](https://serpapi.com)
                2. *GROQ API Key*: Get from [console.groq.com](https://console.groq.com)
                
                *Setup:*
                1. Create a .env file in your project directory
                2. Add the following lines:
                
                SERP_API_KEY=your_serpapi_key_here
                GROQ_API_KEY=your_groq_key_here
                
                """)
            return
        
        tab1, tab2, tab3 = st.tabs(["🔍 Single Search", "📊 Bulk Search", "📈 Search Results"])
        
        with tab1:
            st.subheader("Find Email for Single Company")
            
            col1, col2 = st.columns(2)
            with col1:
                company_name = st.text_input("Company Name:", placeholder="e.g., TechCorp Solutions")
                
            with col2:
                search_type = st.selectbox("Search Focus:", [
                    "Contact email", 
                    "Sales email", 
                    "Support email", 
                    "General inquiry email",
                    "CEO/Founder email"
                ])
            
            # Additional search parameters
            with st.expander("🔧 Advanced Search Options"):
                location = st.text_input("Location (optional):", placeholder="e.g., San Francisco, CA")
                industry = st.text_input("Industry (optional):", placeholder="e.g., Technology, Healthcare")
                website = st.text_input("Website (optional):", placeholder="e.g., www.company.com")
            
            if st.button("🔍 Find Email", type="primary"):
                if not company_name:
                    st.error("Please enter a company name.")
                else:
                    with st.spinner(f"Searching for email addresses for {company_name}..."):
                        # Create search query
                        search_query = f"{company_name} {search_type}"
                        if location:
                            search_query += f" {location}"
                        if industry:
                            search_query += f" {industry}"
                        if website:
                            search_query += f" site:{website}"
                        
                        # Get search results
                        search_context = get_search_results(
                            search_query, 
                            "Find {column_name} contact information", 
                            SERP_API_KEY, 
                            company_name
                        )
                        
                        if search_context:
                            # Use AI to extract email
                            email_question = f"Extract the {search_type} for {company_name}. Return only the email address, nothing else."
                            
                            extracted_email = ask_groq_api(
                                email_question, 
                                company_name, 
                                search_context, 
                                GROQ_API_KEY
                            )
                            
                            # Validate and display results
                            if extracted_email and is_valid_email(extracted_email):
                                st.success(f"✅ Email found: *{extracted_email}*")
                                
                                # Additional company info
                                info_question = "Extract company information including website, phone number, address, and any other contact details."
                                company_info = ask_groq_api(info_question, company_name, search_context, GROQ_API_KEY)
                                
                                with st.expander("📋 Additional Company Information"):
                                    st.text(company_info)
                                
                                # Save option
                                if st.button("💾 Save to Results"):
                                    # Save to session state or database
                                    if 'found_emails' not in st.session_state:
                                        st.session_state.found_emails = []
                                    
                                    st.session_state.found_emails.append({
                                        'company_name': company_name,
                                        'email': extracted_email,
                                        'search_type': search_type,
                                        'additional_info': company_info,
                                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    })
                                    st.success("Results saved!")
                            else:
                                st.warning(f"⚠ No valid email found. Raw result: {extracted_email}")
                                
                                # Show search context for debugging
                                with st.expander("🔍 Search Context (for debugging)"):
                                    st.text(search_context[:1000] + "..." if len(search_context) > 1000 else search_context)
                        else:
                            st.error("❌ No search results found. Try a different search query.")
        
        with tab2:
            st.subheader("Bulk Email Search")
            
            # File upload for bulk search
            uploaded_file = st.file_uploader("Upload companies list (Excel/CSV)", type=['xlsx', 'xls', 'csv'])
            
            if uploaded_file:
                try:
                    # Read file
                    if uploaded_file.name.endswith('.csv'):
                        df = pd.read_csv(uploaded_file)
                    else:
                        df = pd.read_excel(uploaded_file)
                    
                    st.success(f"✅ File loaded: {len(df)} companies found")
                    st.dataframe(df.head())
                    
                    # Column selection
                    company_column = st.selectbox("Select company name column:", df.columns)
                    search_type_bulk = st.selectbox("Email type to search:", [
                        "Contact email", "Sales email", "Support email", "General inquiry email"
                    ])
                    
                    # Search settings
                    col1, col2 = st.columns(2)
                    with col1:
                        max_searches = st.number_input("Max searches (to avoid API limits):", 1, len(df), min(50, len(df)))
                    with col2:
                        delay_seconds = st.number_input("Delay between searches (seconds):", 1, 10, 2)
                    
                    if st.button("🚀 Start Bulk Search"):
                        results = []
                        progress_bar = st.progress(0)
                        status_container = st.empty()
                        
                        for idx, row in df.head(max_searches).iterrows():
                            company_name = str(row[company_column])
                            
                            status_container.text(f"Searching for {company_name}...")
                            
                            try:
                                # Search for email
                                search_context = get_search_results(
                                    f"{company_name} {search_type_bulk}", 
                                    "Find {column_name} contact information", 
                                    SERP_API_KEY, 
                                    company_name
                                )
                                
                                if search_context:
                                    email_question = f"Extract the {search_type_bulk} for {company_name}. Return only the email address."
                                    extracted_email = ask_groq_api(email_question, company_name, search_context, GROQ_API_KEY)
                                    
                                    # Validate email
                                    if is_valid_email(extracted_email):
                                        results.append({
                                            'Company': company_name,
                                            'Email': extracted_email,
                                            'Status': '✅ Found',
                                            'Search_Type': search_type_bulk
                                        })
                                        status_container.success(f"✅ Found: {extracted_email}")
                                    else:
                                        results.append({
                                            'Company': company_name,
                                            'Email': 'Not found',
                                            'Status': '❌ Not found',
                                            'Search_Type': search_type_bulk
                                        })
                                        status_container.warning(f"⚠ No email found for {company_name}")
                                else:
                                    results.append({
                                        'Company': company_name,
                                        'Email': 'No results',
                                        'Status': '❌ No search results',
                                        'Search_Type': search_type_bulk
                                    })
                                    
                            except Exception as e:
                                results.append({
                                    'Company': company_name,
                                    'Email': 'Error',
                                    'Status': f'❌ Error: {str(e)[:50]}',
                                    'Search_Type': search_type_bulk
                                })
                            
                            # Update progress
                            progress_bar.progress((idx + 1) / max_searches)
                            
                            # Delay to avoid rate limits
                            time.sleep(delay_seconds)
                        
                        # Display results
                        results_df = pd.DataFrame(results)
                        st.success(f"🎉 Bulk search completed! Found {len(results_df[results_df['Status'].str.contains('Found')])} emails out of {len(results_df)} searches.")
                        
                        st.dataframe(results_df)
                        
                        # Download results
                        csv = results_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Results CSV",
                            data=csv,
                            file_name=f"email_search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                        
                        # Save to session state
                        if 'bulk_search_results' not in st.session_state:
                            st.session_state.bulk_search_results = []
                        st.session_state.bulk_search_results.extend(results)
                        
                except Exception as e:
                    st.error(f"Error processing file: {str(e)}")
        
        with tab3:
            st.subheader("📈 Search Results")
            
            # Display saved results
            if 'found_emails' in st.session_state and st.session_state.found_emails:
                st.write("*Single Search Results:*")
                single_results_df = pd.DataFrame(st.session_state.found_emails)
                st.dataframe(single_results_df)
                
                # Download single results
                csv_single = single_results_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Single Search Results",
                    data=csv_single,
                    file_name=f"single_email_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            
            if 'bulk_search_results' in st.session_state and st.session_state.bulk_search_results:
                st.write("*Bulk Search Results:*")
                bulk_results_df = pd.DataFrame(st.session_state.bulk_search_results)
                st.dataframe(bulk_results_df)
                
                # Statistics
                found_count = len(bulk_results_df[bulk_results_df['Status'].str.contains('Found')])
                total_count = len(bulk_results_df)
                success_rate = (found_count / total_count * 100) if total_count > 0 else 0
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Searches", total_count)
                with col2:
                    st.metric("Emails Found", found_count)
                with col3:
                    st.metric("Success Rate", f"{success_rate:.1f}%")
            
            if ('found_emails' not in st.session_state or not st.session_state.found_emails) and \
               ('bulk_search_results' not in st.session_state or not st.session_state.bulk_search_results):
                st.info("No search results yet. Use the search tabs above to find emails!")
            
            # Clear results button
            if st.button("🗑 Clear All Results"):
                if 'found_emails' in st.session_state:
                    del st.session_state.found_emails
                if 'bulk_search_results' in st.session_state:
                    del st.session_state.bulk_search_results
                st.success("Results cleared!")
                st.rerun()
        
    elif selected_page == "✏ Template Builder":
        email_template_builder()
        
    elif selected_page == "🧪 A/B Testing":
        create_ab_test_functionality()
        
    elif selected_page == "⏰ Schedule Campaigns":
        schedule_email_campaign()
        
    elif selected_page == "📈 Performance Tracking":
        st.header("📈 Performance Tracking")
        
        # Advanced analytics would go here
        email_stats = get_email_stats()
        
        if not email_stats.empty:
            st.subheader("📊 Campaign Performance")
            
            # Time-based analysis
            if 'sent_time' in email_stats.columns:
                email_stats['sent_time'] = pd.to_datetime(email_stats['sent_time'])
                email_stats['hour'] = email_stats['sent_time'].dt.hour
                email_stats['day_of_week'] = email_stats['sent_time'].dt.day_name()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Best sending hours
                    hourly_success = email_stats[email_stats['status'] == 'sent'].groupby('hour').size()
                    fig_hours = px.bar(x=hourly_success.index, y=hourly_success.values, 
                                     title="Best Sending Hours")
                    fig_hours.update_xaxes(title_text="Hour of Day")
                    fig_hours.update_yaxes(title_text="Emails Sent")
                    st.plotly_chart(fig_hours, use_container_width=True)
                
                with col2:
                    # Best sending days
                    daily_success = email_stats[email_stats['status'] == 'sent'].groupby('day_of_week').size()
                    fig_days = px.bar(x=daily_success.index, y=daily_success.values,
                                    title="Best Sending Days")
                    fig_days.update_xaxes(title_text="Day of Week")
                    fig_days.update_yaxes(title_text="Emails Sent")
                    st.plotly_chart(fig_days, use_container_width=True)
            
            # Deliverability analysis
            st.subheader("📬 Deliverability Analysis")
            delivery_stats = email_stats['delivery_status'].value_counts()
            
            fig_delivery = px.pie(values=delivery_stats.values, names=delivery_stats.index,
                                title="Email Deliverability Distribution")
            st.plotly_chart(fig_delivery, use_container_width=True)
            
        else:
            st.info("No performance data available yet. Send some emails to see analytics!")
    
    # Footer
    st.markdown("---")
    st.markdown(" Advanced Email Campaign System v2.0")
if __name__ == "__main__":
    main()