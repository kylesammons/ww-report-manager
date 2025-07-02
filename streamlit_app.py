import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import base64
from io import BytesIO
import os
import urllib.parse
import json
from google.cloud import bigquery
from google.oauth2 import service_account

# BigQuery Configuration
PROJECT_ID = "trimark-tdp"

# Status persistence file
STATUS_FILE = "status_data.json"

# Set page config
st.set_page_config(
    page_title="Report Management Dashboard",
    page_icon="📧",
    layout="wide"
)

# Enhanced Custom CSS for better aesthetics
st.markdown("""
<style>
    /* Main container styling */
    .main-container {
        background-color: #f8f9fa;
        padding: 2rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    /* Status summary container */
    .status-summary {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
    
    /* Progress container styling */
    .progress-section {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        border-left: 4px solid #28a745;
    }
    
    /* Data table container */
    .data-table-container {
        background: white;
        padding: 2rem;
        border-radius: 15px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        margin-top: 2rem;
    }
    
    /* Table header styling */
    .table-header {
        background: linear-gradient(90deg, #4a90e2, #357abd);
        color: white;
        padding: 1rem;
        border-radius: 10px 10px 0 0;
        margin-bottom: 0;
    }
    
    /* Table row styling */
    .table-row {
        background: white;
        padding: 1rem;
        border-bottom: 1px solid #e9ecef;
        transition: background-color 0.2s ease;
    }
    
    .table-row:hover {
        background-color: #f8f9fa;
    }
    
    .table-row:nth-child(even) {
        background-color: #fafbfc;
    }
    
    /* Metric card styling */
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        border-top: 3px solid #4a90e2;
        margin: 0.5rem 0;
    }
    
    .metric-number {
        font-size: 2rem;
        font-weight: bold;
        color: #2c3e50;
    }
    
    .metric-label {
        font-size: 0.9rem;
        color: #7f8c8d;
        margin-top: 0.5rem;
    }
    
    /* Progress bar improvements */
    .progress-container {
        background: linear-gradient(90deg, #e9ecef, #f8f9fa);
        border-radius: 25px;
        padding: 4px;
        height: 40px;
        margin: 1rem 0;
        box-shadow: inset 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .progress-bar {
        background: linear-gradient(90deg, #28a745, #20c997, #17a2b8);
        height: 100%;
        border-radius: 20px;
        transition: width 0.5s ease;
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        font-weight: bold;
        font-size: 14px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }
    
    .progress-text {
        text-align: center;
        font-size: 16px;
        font-weight: 600;
        color: #2c3e50;
        margin-top: 0.5rem;
    }
    
    /* Button styling */
    .gmail-button {
        background: linear-gradient(45deg, #4285f4, #34a853);
        color: white;
        border: none;
        padding: 10px 20px;
        border-radius: 25px;
        cursor: pointer;
        font-size: 14px;
        font-weight: 600;
        text-decoration: none;
        display: inline-block;
        transition: all 0.3s ease;
        box-shadow: 0 2px 8px rgba(66, 133, 244, 0.3);
    }
    
    .gmail-button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(66, 133, 244, 0.4);
    }
    
    /* Link styling */
    .report-link {
        color: #4a90e2;
        text-decoration: none;
        font-weight: 600;
        padding: 8px 16px;
        background: #e3f2fd;
        border-radius: 20px;
        transition: all 0.3s ease;
        display: inline-block;
    }
    
    .report-link:hover {
        background: #bbdefb;
        transform: translateY(-1px);
    }
    
    /* Status dropdown styling */
    .stSelectbox > div > div {
        background: white;
        border-radius: 8px;
        border: 2px solid #e9ecef;
        transition: border-color 0.3s ease;
    }
    
    .stSelectbox > div > div:focus-within {
        border-color: #4a90e2;
        box-shadow: 0 0 0 3px rgba(74, 144, 226, 0.1);
    }
    
    /* Section dividers */
    .section-divider {
        height: 2px;
        background: linear-gradient(90deg, transparent, #4a90e2, transparent);
        margin: 2rem 0;
        border-radius: 1px;
    }
    
    /* Footer styling */
    .footer {
        background: #2c3e50;
        color: white;
        padding: 2rem;
        border-radius: 10px;
        margin-top: 3rem;
        text-align: center;
    }
    
    /* Title styling */
    .dashboard-title {
        text-align: center;
        color: #2c3e50;
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 2rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
    }
    
    /* Subtitle styling */
    .section-subtitle {
        color: white;
        font-size: 1.5rem;
        font-weight: 600;
        margin-bottom: 1rem;
        text-align: center;
    }
    
    /* Data table specific styling */
    .data-header {
        background: #4a90e2;
        color: white;
        padding: 12px;
        font-weight: 600;
        text-align: center;
        border-radius: 8px;
        margin-bottom: 1rem;
    }
    
    /* Compact table cell */
    .table-cell {
        padding: 8px;
        vertical-align: middle;
        border-bottom: 1px solid #e9ecef;
    }
    
    /* Status-specific colors */
    .status-data-refreshed { background: linear-gradient(45deg, #e3f2fd, #bbdefb) !important; }
    .status-qa { background: linear-gradient(45deg, #fff3e0, #ffcc02) !important; }
    .status-data-team { background: linear-gradient(45deg, #f3e5f5, #e1bee7) !important; }
    .status-reviewed { background: linear-gradient(45deg, #e8f5e8, #c8e6c9) !important; }
    .status-email-drafted { background: linear-gradient(45deg, #e1f5fe, #b3e5fc) !important; }
    .status-report-sent { background: linear-gradient(45deg, #e8f5e8, #a5d6a7) !important; }
</style>
""", unsafe_allow_html=True)

# Status options
STATUS_OPTIONS = ['Data Refreshed', 'QA', 'Data Team', 'Reviewed', 'Email Drafted', 'Report Sent']

def save_status_data():
    """Save status data to JSON file"""
    try:
        status_data = {
            'row_statuses': st.session_state.get('row_statuses', {}),
            'last_status_reset': st.session_state.get('last_status_reset', None),
            'last_saved': datetime.now().isoformat()
        }
        
        with open(STATUS_FILE, 'w') as f:
            json.dump(status_data, f, indent=2)
        
        return True
    except Exception as e:
        st.warning(f"Could not save status data: {str(e)}")
        return False

def load_status_data():
    """Load status data from JSON file"""
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, 'r') as f:
                status_data = json.load(f)
            
            # Load data into session state
            st.session_state.row_statuses = status_data.get('row_statuses', {})
            st.session_state.last_status_reset = status_data.get('last_status_reset', None)
            
            return True
        else:
            # Initialize empty status data if file doesn't exist
            st.session_state.row_statuses = {}
            st.session_state.last_status_reset = None
            return False
    except Exception as e:
        st.warning(f"Could not load status data: {str(e)}")
        st.session_state.row_statuses = {}
        st.session_state.last_status_reset = None
        return False

# Initialize session state
if 'show_popup' not in st.session_state:
    st.session_state.show_popup = False
if 'selected_row' not in st.session_state:
    st.session_state.selected_row = None
if 'email_subject' not in st.session_state:
    st.session_state.email_subject = ""
if 'email_body' not in st.session_state:
    st.session_state.email_body = ""
if 'row_statuses' not in st.session_state:
    st.session_state.row_statuses = {}
if 'last_status_reset' not in st.session_state:
    st.session_state.last_status_reset = None
if 'status_data_loaded' not in st.session_state:
    st.session_state.status_data_loaded = False

# Load status data on first run
if not st.session_state.status_data_loaded:
    load_status_data()
    st.session_state.status_data_loaded = True

def initialize_status_for_month():
    """Initialize or reset statuses to 'Data Refreshed' on the 3rd of each month"""
    current_date = datetime.now()
    current_month_key = f"{current_date.year}-{current_date.month}"
    
    # Check if we need to reset statuses (on or after the 3rd of the month)
    if (current_date.day >= 3 and 
        st.session_state.last_status_reset != current_month_key):
        
        # Reset all statuses to 'Data Refreshed'
        for key in list(st.session_state.row_statuses.keys()):
            st.session_state.row_statuses[key] = 'Data Refreshed'
        
        st.session_state.last_status_reset = current_month_key
        
        # Save the reset status data
        save_status_data()
        
        st.success(f"✅ Statuses reset to 'Data Refreshed' for {current_date.strftime('%B %Y')}")

def get_status_for_row(client_id):
    """Get status for a specific row, defaulting to 'Data Refreshed'"""
    return st.session_state.row_statuses.get(str(client_id), 'Data Refreshed')

def set_status_for_row(client_id, status):
    """Set status for a specific row and save to file"""
    st.session_state.row_statuses[str(client_id)] = status
    # Save immediately when status changes
    save_status_data()

def create_progress_bar(sent_count, total_count):
    """Create a progress bar HTML for reports sent"""
    if total_count == 0:
        percentage = 0
    else:
        percentage = (sent_count / total_count) * 100
    
    progress_html = f"""
    <div class="progress-container">
        <div class="progress-bar" style="width: {percentage}%;">
            {percentage:.0f}%
        </div>
    </div>
    <div class="progress-text">
        {sent_count} of {total_count} Reports Sent
    </div>
    """
    return progress_html

@st.cache_resource
def init_bigquery_client():
    """Initialize BigQuery client with service account credentials"""
    try:
        credentials = None
        
        # Method 1: Try Streamlit secrets (for deployment)
        try:
            if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
                credentials = service_account.Credentials.from_service_account_info(
                    st.secrets["gcp_service_account"]
                )
        except Exception as e:
            pass  # Continue to next method
        
        # Method 2: Try environment variable (recommended for local)
        if not credentials:
            try:
                credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
                if credentials_path and os.path.exists(credentials_path):
                    credentials = service_account.Credentials.from_service_account_file(credentials_path)
                    st.success(f"Using environment variable credentials: {credentials_path}")
            except Exception as e:
                pass  # Continue to next method
        
        # Method 3: Try hardcoded path (fallback for local development)
        if not credentials:
            try:
                hardcoded_path = '/Users/trimark/Desktop/Jupyter_Notebooks/trimark-tdp-87c89fbd0816.json'
                if os.path.exists(hardcoded_path):
                    credentials = service_account.Credentials.from_service_account_file(hardcoded_path)
            except Exception as e:
                pass  # Continue to next method
        
        # Method 4: Try default credentials (for Google Cloud environments)
        if not credentials:
            try:
                credentials, project = service_account.default()
                st.info("Using default Google Cloud credentials")
            except Exception as e:
                pass
        
        if not credentials:
            raise Exception("No valid credentials found. Please check your setup.")
        
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        
        # Test the connection
        try:
            # Simple query to test connection
            test_query = f"SELECT 1 as test_connection LIMIT 1"
            client.query(test_query).result()
        except Exception as e:
            st.warning(f"BigQuery connection test failed: {str(e)}")
        
        return client
        
    except Exception as e:
        st.error(f"Error initializing BigQuery client: {str(e)}")
        st.error("Please ensure you have set up authentication properly:")
        st.code("""
        For local development, set environment variable:
        export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"
        
        Or place your credentials file at:
        /Users/trimark/Desktop/Jupyter_Notebooks/trimark-tdp-87c89fbd0816.json
        
        For deployment, add your service account key to Streamlit secrets.
        """)
        return None

#start
@st.cache_data
def load_data_from_bigquery():
    """Load data from BigQuery table"""
    client = init_bigquery_client()
    if not client:
        return pd.DataFrame()
    
    try:
        query = """
        WITH leads_summary AS (
            SELECT
                client_id,
                client_name,
                Client_Group,
                
                -- Paid Form Leads  
                ROUND(SUM(CASE WHEN previous_month IS TRUE AND Medium != 'Organic' THEN form_leads ELSE 0 END)) as paid_form_leads,
                ROUND(SUM(CASE WHEN previous_month_previous_year IS TRUE AND Medium != 'Organic' THEN form_leads ELSE 0 END)) as paid_form_leads_yoy,
                ROUND(SUM(CASE WHEN previous_month IS TRUE AND Medium != 'Organic' THEN form_leads ELSE 0 END)) - 
                ROUND(SUM(CASE WHEN previous_month_previous_year IS TRUE AND Medium != 'Organic' THEN form_leads ELSE 0 END)) as paid_form_leads_diff,
                
                -- Organic Form Leads
                ROUND(SUM(CASE WHEN previous_month IS TRUE AND Medium = 'Organic' THEN form_leads ELSE 0 END)) as organic_form_leads,
                ROUND(SUM(CASE WHEN previous_month_previous_year IS TRUE AND Medium = 'Organic' THEN form_leads ELSE 0 END)) as organic_form_leads_yoy,
                ROUND(SUM(CASE WHEN previous_month IS TRUE AND Medium = 'Organic' THEN form_leads ELSE 0 END)) - 
                ROUND(SUM(CASE WHEN previous_month_previous_year IS TRUE AND Medium = 'Organic' THEN form_leads ELSE 0 END)) as organic_form_leads_diff,
                
                -- Total Form Leads
                ROUND(SUM(CASE WHEN previous_month IS TRUE THEN form_leads ELSE 0 END)) as total_form_leads,
                ROUND(SUM(CASE WHEN previous_month_previous_year IS TRUE THEN form_leads ELSE 0 END)) as total_form_leads_yoy,
                ROUND(SUM(CASE WHEN previous_month IS TRUE THEN form_leads ELSE 0 END)) - 
                ROUND(SUM(CASE WHEN previous_month_previous_year IS TRUE THEN form_leads ELSE 0 END)) as total_form_leads_diff,
                
                -- Paid Call Leads
                ROUND(SUM(CASE WHEN previous_month IS TRUE THEN qualified_call_leads ELSE 0 END)) as paid_call_leads,
                ROUND(SUM(CASE WHEN previous_month_previous_year IS TRUE THEN qualified_call_leads ELSE 0 END)) as paid_call_leads_yoy,
                ROUND(SUM(CASE WHEN previous_month IS TRUE THEN qualified_call_leads ELSE 0 END)) - 
                ROUND(SUM(CASE WHEN previous_month_previous_year IS TRUE THEN qualified_call_leads ELSE 0 END)) as paid_call_leads_diff,
                
                -- Organic Call Leads
                ROUND(SUM(CASE WHEN previous_month IS TRUE THEN organic_calls ELSE 0 END)) as organic_call_leads,
                ROUND(SUM(CASE WHEN previous_month_previous_year IS TRUE THEN organic_calls ELSE 0 END)) as organic_call_leads_yoy,
                ROUND(SUM(CASE WHEN previous_month IS TRUE THEN organic_calls ELSE 0 END)) - 
                ROUND(SUM(CASE WHEN previous_month_previous_year IS TRUE THEN organic_calls ELSE 0 END)) as organic_call_leads_diff,
                
                -- Total Leads
                ROUND(SUM(CASE WHEN previous_month IS TRUE THEN 
                COALESCE(organic_calls, 0) + COALESCE(qualified_call_leads, 0) + COALESCE(form_leads, 0) 
                ELSE 0 END)) as total_leads,
                ROUND(SUM(CASE WHEN previous_month_previous_year IS TRUE THEN 
                COALESCE(organic_calls, 0) + COALESCE(qualified_call_leads, 0) + COALESCE(form_leads, 0) 
                ELSE 0 END)) as total_leads_yoy,
                ROUND(SUM(CASE WHEN previous_month IS TRUE THEN 
                COALESCE(organic_calls, 0) + COALESCE(qualified_call_leads, 0) + COALESCE(form_leads, 0) 
                ELSE 0 END)) - 
                ROUND(SUM(CASE WHEN previous_month_previous_year IS TRUE THEN 
                COALESCE(organic_calls, 0) + COALESCE(qualified_call_leads, 0) + COALESCE(form_leads, 0) 
                ELSE 0 END)) as total_leads_diff
            
            FROM `trimark-tdp.master.all_leads`
            WHERE business = 'Window World'
            GROUP BY client_id, client_name, Client_Group
            ),

            cost_summary AS (
            SELECT
                client_id,
                -- Cost Metrics
                ROUND(SUM(CASE WHEN previous_month IS TRUE THEN COALESCE(cost, 0) ELSE 0 END), 2) as total_cost,
                ROUND(SUM(CASE WHEN previous_month_previous_year IS TRUE THEN COALESCE(cost, 0) ELSE 0 END), 2) as total_cost_yoy,
                ROUND(SUM(CASE WHEN previous_month IS TRUE THEN COALESCE(cost, 0) ELSE 0 END), 2) - 
                ROUND(SUM(CASE WHEN previous_month_previous_year IS TRUE THEN COALESCE(cost, 0) ELSE 0 END), 2) as total_cost_diff
            
            FROM `trimark-tdp.master.all_paidmedia`
            WHERE business = 'Window World'
            GROUP BY client_id
            )

            SELECT
            l.*,
            COALESCE(c.total_cost, 0) as total_cost,
            COALESCE(c.total_cost_yoy, 0) as total_cost_yoy,
            COALESCE(c.total_cost_diff, 0) as total_cost_diff,
            
            -- Cost Per Lead
            CASE 
                WHEN l.total_leads > 0 
                THEN ROUND(COALESCE(c.total_cost, 0) / l.total_leads, 2)
                ELSE 0 
            END as cost_per_lead,
            
            CASE 
                WHEN l.total_leads_yoy > 0 
                THEN ROUND(COALESCE(c.total_cost_yoy, 0) / l.total_leads_yoy, 2)
                ELSE 0 
            END as cost_per_lead_yoy,
            
            CASE 
                WHEN l.total_leads > 0 
                THEN ROUND(COALESCE(c.total_cost, 0) / l.total_leads, 2)
                ELSE 0 
            END - 
            CASE 
                WHEN l.total_leads_yoy > 0 
                THEN ROUND(COALESCE(c.total_cost_yoy, 0) / l.total_leads_yoy, 2)
                ELSE 0 
            END as cost_per_lead_diff

            FROM leads_summary l
            LEFT JOIN cost_summary c ON l.client_id = c.client_id

        """
        
        # Execute query
        query_job = client.query(query)
        df = query_job.to_dataframe()
        
        return df
        
    except Exception as e:
        st.error(f"Error loading data from BigQuery: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def load_email_table():
    """Load email table data from CSV file"""
    csv_path = 'WW Email Table - Sheet1.csv'
    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"Error loading email table CSV file: {e}")
        return pd.DataFrame()

@st.cache_data
def merge_bigquery_with_email_data():
    """Merge BigQuery metrics data with email table data"""
    # Load both datasets
    bigquery_df = load_data_from_bigquery()
    email_df = load_email_table()
    
    if bigquery_df.empty:
        return email_df
    
    if email_df.empty:
        st.warning("No email table data available")
        return bigquery_df
    
    # Ensure client_id columns are the same type for merging
    if 'Client ID' in email_df.columns:
        email_df['Client ID'] = email_df['Client ID'].astype(str)
    if 'client_id' in bigquery_df.columns:
        bigquery_df['client_id'] = bigquery_df['client_id'].astype(str)
    
    # Merge on client_id
    try:
        merged_df = pd.merge(
            email_df, 
            bigquery_df, 
            left_on='Client ID', 
            right_on='client_id', 
            how='left'
        )
        return merged_df
        
    except Exception as e:
        st.error(f"Error merging datasets: {str(e)}")
        return email_df

def make_clickable_link(url, text="View Report"):
    """Create a clickable HTML link"""
    if pd.isna(url) or url == "" or url is None:
        return "No link available"
    return f'<a href="{url}" target="_blank" class="report-link">{text}</a>'

def calculate_percentage_change(current, previous):
    """Calculate percentage change between two values"""
    if previous == 0:
        return 0 if current == 0 else 100
    return round(((current - previous) / previous) * 100)

def generate_automated_message(row):
    """Generate automated email message based on BigQuery data"""
    # Use Client_Group as the company name, fall back to client_name or Client Name
    company_name = row.get('client_name', row.get('Client Name', row.get('client_name', row.get('Client Name', ''))))
    
    # Get current month data (assuming this is previous_month in the query)
    total_leads = row.get('total_leads', 0)
    paid_form_leads = row.get('paid_form_leads', 0)
    organic_form_leads = row.get('organic_form_leads', 0)
    paid_call_leads = row.get('paid_call_leads', 0)
    organic_call_leads = row.get('organic_call_leads', 0)
    
    # Get year-over-year data
    total_leads_yoy = row.get('total_leads_yoy', 0)
    paid_form_leads_yoy = row.get('paid_form_leads_yoy', 0)
    organic_form_leads_yoy = row.get('organic_form_leads_yoy', 0)
    paid_call_leads_yoy = row.get('paid_call_leads_yoy', 0)
    organic_call_leads_yoy = row.get('organic_call_leads_yoy', 0)
    
    # Get differences
    paid_form_leads_diff = row.get('paid_form_leads_diff', 0)
    organic_form_leads_diff = row.get('organic_form_leads_diff', 0)
    paid_call_leads_diff = row.get('paid_call_leads_diff', 0)
    organic_call_leads_diff = row.get('organic_call_leads_diff', 0)
    total_leads_diff = row.get('total_leads_diff', 0)
    
    # Get cost per lead data
    cost_per_lead = row.get('cost_per_lead', 0)
    cost_per_lead_yoy = row.get('cost_per_lead_yoy', 0)
    cost_per_lead_diff = row.get('cost_per_lead_diff', 0)
    
    # Calculate total paid leads (form + call)
    total_paid_leads = paid_form_leads + paid_call_leads
    total_organic_leads = organic_form_leads + organic_call_leads
    
    # Start building the message
    current_month = datetime.now().strftime("%B")
    previous_month = (datetime.now().replace(day=1) - pd.Timedelta(days=1)).strftime("%B")
    
    message = f"Hope you're doing well! Attached you'll find {previous_month}'s performance reports for your markets.\n\n"
    
    # Add total leads summary
    message += f"Last month, your campaigns generated a total of {int(total_leads)} leads "
    message += f"({int(total_paid_leads)} paid + {int(total_organic_leads)} organic).\n\n"
    
    # Add YoY comparison header
    message += f"Below you'll find YoY performance highlights. Compared to {previous_month} 2024:\n"
    
    # Generate bullet points for positive changes
    bullet_points = []
    
    # Cost per lead - only add if there's a decrease (negative difference means lower cost)
    if cost_per_lead_diff < 0 and cost_per_lead_yoy > 0:  # Ensure we have valid YoY data
        cost_decrease = abs(cost_per_lead_diff)
        percentage_decrease = calculate_percentage_change(cost_per_lead, cost_per_lead_yoy)
        # Since it's a decrease, we want the absolute value of the percentage
        percentage_decrease = abs(percentage_decrease)
        bullet_points.append(f"• Cost per lead decreased by ${cost_decrease:.2f} (or a {percentage_decrease}% decrease), improving efficiency.")
    
    # Total leads
    if total_leads_diff > 0:
        percentage = calculate_percentage_change(total_leads, total_leads_yoy)
        bullet_points.append(f"• {company_name} generated {int(total_leads_diff)} more total leads (or a {percentage}% increase).")

    # Paid form leads
    if paid_form_leads_diff > 0:
        percentage = calculate_percentage_change(paid_form_leads, paid_form_leads_yoy)
        bullet_points.append(f"• {company_name} generated {int(paid_form_leads_diff)} more paid form leads (or a {percentage}% increase).")
    
    # Organic form leads
    if organic_form_leads_diff > 0:
        percentage = calculate_percentage_change(organic_form_leads, organic_form_leads_yoy)
        bullet_points.append(f"• {company_name} generated {int(organic_form_leads_diff)} more organic form leads (or a {percentage}% increase).")
    
    # Paid call leads
    if paid_call_leads_diff > 0:
        percentage = calculate_percentage_change(paid_call_leads, paid_call_leads_yoy)
        bullet_points.append(f"• {company_name} generated {int(paid_call_leads_diff)} more paid call leads (or a {percentage}% increase).")
    
    # Organic call leads
    if organic_call_leads_diff > 0:
        percentage = calculate_percentage_change(organic_call_leads, organic_call_leads_yoy)
        bullet_points.append(f"• {company_name} generated {int(organic_call_leads_diff)} more organic call leads (or a {percentage}% increase).")
    
    # Add bullet points to message
    if bullet_points:
        message += "\n".join(bullet_points)
    else:
        message += "• Performance metrics are being analyzed for additional insights."
    
    message += "\n\nPlease let me know if you have any questions. Have a great weekend!\n\nThanks,"
    
    return message

def generate_gmail_url(row):
    """Generate Gmail compose URL with pre-populated fields"""
    
    # Use Client_Name as company name
    company_name = row.get('client_name', row.get('Client Name', row.get('client_name', row.get('Client Name', ''))))
    recipient_names = row.get('Recipient Name(s)', '')
    notes = row.get('Notes', '')
    recipient_email = row.get('Recipient Email', '')
    
    # Email subject
    previous_month = (datetime.now().replace(day=1) - pd.Timedelta(days=1)).strftime("%B")
    subject = f"{previous_month} Performance Report - {company_name}"
    
    # Generate automated email body based on data
    if 'total_leads' in row and pd.notna(row.get('total_leads')):
        # Use automated message for merged BigQuery + Email data
        body = generate_automated_message(row)
    else:
        # Fallback to original message for CSV data only
        body = f"""Dear {recipient_names},

I hope this email finds you well.

Please find attached the latest report for {company_name}. This report includes {notes.lower() if pd.notna(notes) else 'key performance metrics and insights'}.

Key highlights from this reporting period:
• Performance metrics and analytics
• Actionable insights and recommendations  
• Progress toward established goals

Please review the attached report and feel free to reach out if you have any questions or would like to schedule a call to discuss the findings in more detail.

Best regards,
Your Account Team
Trimark Digital"""
    
    # CC emails
    cc_emails = "matt@trimarkdigital.com,will@trimarkdigital.com"
    
    # URL encode the parameters
    to_encoded = urllib.parse.quote(recipient_email if recipient_email else "")
    cc_encoded = urllib.parse.quote(cc_emails)
    subject_encoded = urllib.parse.quote(subject)
    body_encoded = urllib.parse.quote(body)
    
    # Construct Gmail compose URL
    gmail_url = f"https://mail.google.com/mail/?view=cm&fs=1&to={to_encoded}&cc={cc_encoded}&su={subject_encoded}&body={body_encoded}"
    
    return gmail_url


def main():
    # Initialize status tracking for the month
    initialize_status_for_month()
    
    # App title (always shown)
    title_col1, title_col2 = st.columns([1, 6])
    
    with title_col1:
        st.image("WW_2018 LOGO.png", width=200)
    
    with title_col2:
        st.markdown('<h1 style="color: #2c3e50; font-size: 2.5rem; font-weight: 700; margin-top: 10px; text-shadow: 2px 2px 4px rgba(0,0,0,0.1);">Report Management Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("<br>",unsafe_allow_html=True)
    
    # Authentication check
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        # Create columns to center and size the login form
        login_col, left_spacer, right_spacer = st.columns([2, 3, 2])
        
        with login_col:
            st.markdown("<br>",unsafe_allow_html=True)
            st.markdown("<br>",unsafe_allow_html=True)
            st.markdown("<br>",unsafe_allow_html=True)
            st.markdown('<p style="font-size: 1.2rem; color: #2c3e50; margin-bottom: 18px;">🔐 Please enter the password to access the dashboard</p>', unsafe_allow_html=True)
            
            password = st.text_input("Password:", type="password", key="password_input")
            
            if st.button("Login", type="primary", use_container_width=True):
                if password == "waves2025":
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("❌ Incorrect password. Please try again.")
        
        # Stop execution here if not authenticated
        return
    
    # Load merged data automatically
    df = merge_bigquery_with_email_data()
    if df.empty:
        df = load_email_table()
    
    if df.empty:
        st.error("No data available from any source.")
        st.error("Please ensure your data sources are properly configured.")
        return
    
    # Filter to only include rows with both Client ID and Report Link
    df = df[
        (df['Client ID'].notna()) & 
        (df['Client ID'] != '') & 
        (df['Report Link'].notna()) & 
        (df['Report Link'] != '')
    ]

    if df.empty:
        st.warning("No rows found with both Client ID and Report Link values.")
        return
    
    # Status Summary Section - simplified without containers, left aligned
    st.markdown('<h2 style="color: #2c3e50; margin-bottom: 2rem;">Status Summary</h2>', unsafe_allow_html=True)
    
    # Count statuses - need to account for all rows, not just those in session state
    status_counts = {status: 0 for status in STATUS_OPTIONS}
    
    # Count each row's status (including defaults)
    for idx, row in df.iterrows():
        client_id = row.get('Client ID', idx)
        current_status = get_status_for_row(client_id)
        status_counts[current_status] += 1
    
    # Create three columns: scorecards, spacer, progress bar
    summary_col1, spacer_col, summary_col2 = st.columns([4, 0.25, 2])
    
    with summary_col1:
        # Display status counts in styled metric cards
        status_cols = st.columns(len(STATUS_OPTIONS))
        for i, (status, count) in enumerate(status_counts.items()):
            with status_cols[i]:
                st.markdown(f"""
                <div style="background: white; padding: 1.5rem; border-radius: 10px; 
                            text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.1); 
                            border-top: 3px solid #4a90e2; margin: 0.5rem 0;">
                    <div style="font-size: 2rem; font-weight: bold; color: #2c3e50;">{count}</div>
                    <div style="font-size: 0.9rem; color: #7f8c8d; margin-top: 0.5rem;">{status}</div>
                </div>
                """, unsafe_allow_html=True)
    
    with spacer_col:
        # Empty spacer column
        st.write("")
    
    with summary_col2:
        st.markdown('<h4 style="color: #2c3e50; margin-bottom: 1rem;">Report Progress</h4>', unsafe_allow_html=True)
        
        # Progress bar for Report Sent
        total_reports = len(df)
        sent_reports = status_counts.get('Report Sent', 0)
        
        # Create sleeker progress bar
        if total_reports == 0:
            percentage = 0
        else:
            percentage = (sent_reports / total_reports) * 100
        
        sleek_progress_html = f"""
        <div style="background: #f0f0f0; border-radius: 15px; padding: 2px; height: 20px; margin: 1rem 0;">
            <div style="background: linear-gradient(90deg, #28a745, #20c997); height: 100%; border-radius: 13px; 
                        width: {percentage}%; transition: width 0.5s ease; display: flex; align-items: center; 
                        justify-content: center; color: white; font-weight: bold; font-size: 11px;">
                {percentage:.0f}%
            </div>
        </div>
        <div style="text-align: center; font-size: 14px; font-weight: 600; color: #2c3e50;">
            {sent_reports} of {total_reports} Reports Sent
        </div>
        """
        st.markdown(sleek_progress_html, unsafe_allow_html=True)
    
    # Section divider
    st.markdown("""
    <div style="height: 2px; background: linear-gradient(90deg, transparent, #4a90e2, transparent); 
                margin: 2rem 0; border-radius: 1px;"></div>
    """, unsafe_allow_html=True)
    
        # Add search filter at the top
    search_col1, search_col2 = st.columns([3, 1])
    with search_col2:
        search_term = st.text_input(
            "Search",
            placeholder="Search clients, groups, recipients...",
            key="search_filter",
            help="Search across client names, groups, and recipients",
            label_visibility="hidden"
        )

    # Filter the dataframe based on search term
    if search_term:
        # Reset index to ensure alignment
        df_reset = df.reset_index(drop=True)
        
        # Create a mask for filtering with matching index
        mask = pd.Series([False] * len(df_reset), index=df_reset.index)
        
        # Search across multiple columns
        search_columns = ['Client Name', 'Client_Group', 'Client Group', 'Recipient Name(s)', 'Notes']
        
        for col in search_columns:
            if col in df_reset.columns:
                mask |= df_reset[col].astype(str).str.contains(search_term, case=False, na=False)
        
        # Apply the filter
        filtered_df = df_reset[mask]
    else:
        filtered_df = df.reset_index(drop=True)
    
    # Data Table Section with styled container for title
    st.markdown("""
    <div style="background: linear-gradient(90deg, #4a90e2, #357abd); color: white; 
                padding: 1rem; border-radius: 10px; margin-top: 2rem; margin-bottom: 1rem; 
                text-align: center; font-weight: 600; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
        Client Report Management
    </div>
    """, unsafe_allow_html=True)
    st.markdown("<br>",unsafe_allow_html=True)

    # Display message if no results found
    if len(filtered_df) == 0 and search_term:
        st.warning(f"No results found for '{search_term}'. Try a different search term.")
    elif search_term:
        st.info(f"Showing {len(filtered_df)} of {len(df)} results for '{search_term}'")
    st.markdown("<br>",unsafe_allow_html=True)

    # Create the data table with clean structure
    if 'total_leads' in filtered_df.columns:
        # Display each row for merged data with clean styling
        for idx, row in filtered_df.iterrows():
            client_id = row.get('Client ID', idx)
            
            # Row layout without containers or background banding
            row_cols = st.columns([1, 2, 2, 1.5, 2, 1, 2, 2])
            
            with row_cols[0]:
                st.markdown(f"**ID:** {row.get('Client ID', '')}")
                
            with row_cols[1]:
                st.markdown(f"**Client:** {row.get('Client Name', '')}")
                
            with row_cols[2]:
                client_group = row.get('Client_Group', row.get('Client Group', ''))
                st.markdown(f"**Group:** {client_group if pd.notna(client_group) else ''}")
                
            with row_cols[3]:
                report_link = row.get('Report Link', '')
                if pd.notna(report_link) and report_link:
                    st.markdown(f"""
                    <a href="{report_link}" target="_blank" 
                    style="color: #4a90e2; text-decoration: none; font-weight: 600; 
                            padding: 8px 16px; background: #e3f2fd; border-radius: 20px; 
                            transition: all 0.3s ease; display: inline-block;">
                        📊 View Report
                    </a>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown("❌ No link")
                    
            with row_cols[4]:
                st.markdown(f"**Recipients:** {row.get('Recipient Name(s)', '')}")
                
            with row_cols[5]:
                total_leads = row.get('total_leads', '')
                if pd.notna(total_leads):
                    st.markdown(f"**Leads:** {int(total_leads)}")
                else:
                    st.markdown("**Leads:** No data")
                    
            with row_cols[6]:
                # Status dropdown
                current_status = get_status_for_row(client_id)
                status_key = f"status_{client_id}_{idx}"
                
                selected_status = st.selectbox(
                    "Status",
                    STATUS_OPTIONS,
                    index=STATUS_OPTIONS.index(current_status),
                    key=status_key,
                    label_visibility="collapsed"
                )
                
                # Update status if changed
                if selected_status != current_status:
                    set_status_for_row(client_id, selected_status)
                    st.rerun()
                    
            with row_cols[7]:
                gmail_url = generate_gmail_url(row)
                st.markdown(f"""
                <a href="{gmail_url}" target="_blank" 
                style="background: linear-gradient(45deg, #4285f4, #34a853); color: white; 
                        border: none; padding: 10px 20px; border-radius: 25px; cursor: pointer; 
                        font-size: 14px; font-weight: 600; text-decoration: none; 
                        display: inline-block; transition: all 0.3s ease; 
                        box-shadow: 0 2px 8px rgba(66, 133, 244, 0.3);">
                    📧 Gmail
                </a>
                """, unsafe_allow_html=True)
            
            # Simple divider between rows
            st.markdown("---")

    else:
        # Display each row for email table data with clean styling
        for idx, row in filtered_df.iterrows():
            client_id = row.get('Client ID', idx)
            
            # Row layout without containers or background banding
            row_cols = st.columns([1, 2, 2, 1.5, 2, 1, 2, 2])
            
            with row_cols[0]:
                st.markdown(f"**ID:** {row.get('Client ID', '')}")
                
            with row_cols[1]:
                st.markdown(f"**Client:** {row.get('Client Name', '')}")
                
            with row_cols[2]:
                st.markdown(f"**Group:** {row.get('Client Group', '')}")
                
            with row_cols[3]:
                report_link = row.get('Report Link', '')
                if pd.notna(report_link) and report_link:
                    st.markdown(f"""
                    <a href="{report_link}" target="_blank" 
                    style="color: #4a90e2; text-decoration: none; font-weight: 600; 
                            padding: 8px 16px; background: #e3f2fd; border-radius: 20px; 
                            transition: all 0.3s ease; display: inline-block;">
                        📊 View Report
                    </a>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown("❌ No link")
                    
            with row_cols[4]:
                st.markdown(f"**Recipients:** {row.get('Recipient Name(s)', '')}")
                
            with row_cols[5]:
                notes = row.get('Notes', '')
                st.markdown(f"**Notes:** {notes if pd.notna(notes) else ''}")
                
            with row_cols[6]:
                # Status dropdown
                current_status = get_status_for_row(client_id)
                status_key = f"status_{client_id}_{idx}"
                
                selected_status = st.selectbox(
                    "Status",
                    STATUS_OPTIONS,
                    index=STATUS_OPTIONS.index(current_status),
                    key=status_key,
                    label_visibility="collapsed"
                )
                
                # Update status if changed
                if selected_status != current_status:
                    set_status_for_row(client_id, selected_status)
                    st.rerun()
                    
            with row_cols[7]:
                gmail_url = generate_gmail_url(row)
                st.markdown(f"""
                <a href="{gmail_url}" target="_blank" 
                style="background: linear-gradient(45deg, #4285f4, #34a853); color: white; 
                        border: none; padding: 10px 20px; border-radius: 25px; cursor: pointer; 
                        font-size: 14px; font-weight: 600; text-decoration: none; 
                        display: inline-block; transition: all 0.3s ease; 
                        box-shadow: 0 2px 8px rgba(66, 133, 244, 0.3);">
                    📧 Gmail
                </a>
                """, unsafe_allow_html=True)
            
            # Simple divider between rows
            st.markdown("---")
    
    # Add status data indicator
    status_file_exists = os.path.exists(STATUS_FILE)
    if status_file_exists:
        try:
            with open(STATUS_FILE, 'r') as f:
                status_data = json.load(f)
            last_saved = status_data.get('last_saved', 'Unknown')
            if last_saved != 'Unknown':
                last_saved_dt = datetime.fromisoformat(last_saved)
                time_str = last_saved_dt.strftime("%B %d, %Y at %I:%M %p")
            else:
                time_str = last_saved
            
            col1, col2 = st.columns([5, 1])
            with col2:
                st.caption(f"📄 Last saved: {time_str}")
        except:
            col1, col2 = st.columns([5, 1])
            with col2:
                st.caption("⚠️ Status file exists but could not read timestamp")
    else:
        col1, col2 = st.columns([5, 1])
        with col2:
            st.caption("📄 Starting fresh - No persistent data found")
        
    # Footer with enhanced styling
    st.markdown("""
    <div style="background: #2c3e50; color: white; padding: 2rem; border-radius: 10px; 
                margin-top: 3rem; text-align: center;">
        <h3 style="color: white; margin-bottom: 1rem;">📧 Email Management Dashboard - Trimark Digital</h3>
        <p style="margin: 0.5rem 0;">💡 <strong>Quick Actions:</strong> Click '📧 Gmail' to compose emails with pre-populated content</p>
        <p style="margin: 0.5rem 0;">📅 <strong>Auto-Reset:</strong> Status tracking resets monthly on the 3rd</p>
        <p style="margin: 0.5rem 0;">📊 <strong>Data Integration:</strong> BigQuery metrics + Email management in one place</p>
        <p style="margin: 0.5rem 0;">💾 <strong>Persistent Storage:</strong> All status changes are automatically saved and restored</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
