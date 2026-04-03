import streamlit as st
import pandas as pd
import cx_Oracle
from groq import Groq
import plotly.express as px
import plotly.graph_objects as go
import io

# ── CONFIG ──────────────────────────────────────────
ORACLE_USER     = "ORACLE_USER"
ORACLE_PASSWORD = "ORACLE_PASSWORD"
ORACLE_DSN      = "ORACLE_DSN"
GROQ_API_KEY    = "GROQ_API_KEY"   # ← paste your new Groq key here
MODEL           = "openai/gpt-oss-120b"
# ────────────────────────────────────────────────────

st.set_page_config(
    page_title="Stack Overflow Developer Intelligence",
    page_icon="💻",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── CUSTOM CSS ───────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }
    
    .main {
        background: #0a0e1a;
        color: #e2e8f0;
    }
    
    .stApp {
        background: linear-gradient(135deg, #0a0e1a 0%, #0f1729 50%, #0a0e1a 100%);
    }

    .title-block {
        background: linear-gradient(135deg, #1e3a5f, #0f2744);
        border-left: 4px solid #38bdf8;
        border-radius: 12px;
        padding: 2rem 2.5rem;
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px rgba(56,189,248,0.15);
    }

    .title-block h1 {
        font-family: 'Space Mono', monospace;
        font-size: 2.2rem;
        color: #38bdf8;
        margin: 0;
        letter-spacing: -1px;
    }

    .title-block p {
        color: #94a3b8;
        margin: 0.5rem 0 0 0;
        font-size: 1rem;
    }

    .metric-card {
        background: linear-gradient(135deg, #1e293b, #162032);
        border: 1px solid #1e3a5f;
        border-radius: 12px;
        padding: 1.5rem;
        text-align: center;
        box-shadow: 0 4px 16px rgba(0,0,0,0.3);
        transition: transform 0.2s;
    }

    .metric-card:hover {
        transform: translateY(-2px);
        border-color: #38bdf8;
    }

    .metric-value {
        font-family: 'Space Mono', monospace;
        font-size: 2rem;
        font-weight: 700;
        color: #38bdf8;
    }

    .metric-label {
        color: #94a3b8;
        font-size: 0.85rem;
        margin-top: 0.3rem;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    .section-header {
        font-family: 'Space Mono', monospace;
        color: #38bdf8;
        font-size: 1.1rem;
        border-bottom: 1px solid #1e3a5f;
        padding-bottom: 0.5rem;
        margin: 1.5rem 0 1rem 0;
    }

    .chat-message-user {
        background: #1e3a5f;
        border-radius: 12px 12px 4px 12px;
        padding: 0.8rem 1.2rem;
        margin: 0.5rem 0;
        color: #e2e8f0;
        text-align: right;
    }

    .chat-message-ai {
        background: #162032;
        border: 1px solid #1e3a5f;
        border-radius: 12px 12px 12px 4px;
        padding: 0.8rem 1.2rem;
        margin: 0.5rem 0;
        color: #e2e8f0;
    }

    .stButton > button {
        background: linear-gradient(135deg, #0369a1, #0284c7);
        color: white;
        border: none;
        border-radius: 8px;
        font-family: 'DM Sans', sans-serif;
        font-weight: 500;
        transition: all 0.2s;
    }

    .stButton > button:hover {
        background: linear-gradient(135deg, #0284c7, #38bdf8);
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(56,189,248,0.3);
    }

    .download-btn {
        background: linear-gradient(135deg, #065f46, #047857) !important;
    }

    div[data-testid="stSidebar"] {
        background: #0f1729;
        border-right: 1px solid #1e3a5f;
    }

    .stSelectbox > div, .stMultiselect > div {
        background: #1e293b;
        border-color: #1e3a5f;
        color: #e2e8f0;
    }
</style>
""", unsafe_allow_html=True)


# ── DATABASE CONNECTION ──────────────────────────────
@st.cache_resource
def get_connection():
    return cx_Oracle.connect(ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN)

@st.cache_data(ttl=300)
def run_query(sql):
    conn = get_connection()
    return pd.read_sql(sql, conn)

def to_csv(df):
    return df.to_csv(index=False).encode('utf-8')


# ── LOAD BASE DATA ───────────────────────────────────
@st.cache_data(ttl=300)
def load_base_data():
    fact = run_query("SELECT * FROM fact_developer_survey")
    dim_dev = run_query("SELECT * FROM dim_developer")
    dim_tech = run_query("SELECT * FROM dim_technology")
    dim_sat = run_query("SELECT * FROM dim_satisfaction_type")
    return fact, dim_dev, dim_tech, dim_sat

try:
    fact_df, dev_df, tech_df, sat_df = load_base_data()
    merged = fact_df.merge(dev_df, on='DEVELOPER_SK', how='left')
    db_connected = True
except Exception as e:
    st.error(f"Database connection failed: {e}")
    db_connected = False
    st.stop()


# ── SIDEBAR ──────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='font-family: Space Mono, monospace; color: #38bdf8; font-size: 1.1rem; 
    padding: 1rem 0; border-bottom: 1px solid #1e3a5f; margin-bottom: 1rem;'>
    🔧 FILTERS & CONTROLS
    </div>
    """, unsafe_allow_html=True)

    # Country filter
    countries = sorted(merged['COUNTRY'].dropna().unique().tolist())
    selected_countries = st.multiselect(
        "🌍 Countries",
        options=countries,
        default=countries[:10] if len(countries) > 10 else countries,
        help="Filter all insights by country"
    )

    # Employment filter
    employment_types = sorted(merged['EMPLOYMENT_TYPE'].dropna().unique().tolist())
    selected_employment = st.multiselect(
        "💼 Employment Type",
        options=employment_types,
        default=employment_types,
        help="Filter by employment type"
    )

    # Salary range
    min_sal = int(merged['SALARY_USD'].dropna().min()) if not merged['SALARY_USD'].dropna().empty else 0
    max_sal = int(merged['SALARY_USD'].dropna().max()) if not merged['SALARY_USD'].dropna().empty else 500000
    salary_range = st.slider(
        "💰 Salary Range (USD)",
        min_value=min_sal,
        max_value=max_sal,
        value=(min_sal, max_sal),
        help="Filter by salary range"
    )

    # Top N selector
    top_n = st.selectbox(
        "📊 Top N Results",
        options=[5, 10, 15, 20],
        index=1,
        help="Number of results to show in charts"
    )

    st.markdown("---")
    st.markdown("""
    <div style='color: #64748b; font-size: 0.75rem; text-align: center;'>
    Data: Stack Overflow Survey 2018<br>
    Pipeline: Informatica IDMC + Oracle
    </div>
    """, unsafe_allow_html=True)


# ── APPLY FILTERS ────────────────────────────────────
filtered = merged.copy()
if selected_countries:
    filtered = filtered[filtered['COUNTRY'].isin(selected_countries)]
if selected_employment:
    filtered = filtered[filtered['EMPLOYMENT_TYPE'].isin(selected_employment)]
filtered = filtered[
    (filtered['SALARY_USD'] >= salary_range[0]) &
    (filtered['SALARY_USD'] <= salary_range[1])
]


# ── TITLE ────────────────────────────────────────────
st.markdown("""
<div class='title-block'>
    <h1>💻 Developer Intelligence Platform</h1>
    <p>Stack Overflow 2018 Survey · Powered by Informatica IDMC + Oracle DWH + Groq AI</p>
</div>
""", unsafe_allow_html=True)


# ── TABS ─────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📊 Insights Dashboard", "🔍 Deep Analysis", "🤖 AI Chatbot"])


# ══════════════════════════════════════════════════════
# TAB 1 — INSIGHTS DASHBOARD
# ══════════════════════════════════════════════════════
with tab1:

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    
    total_devs = len(filtered)
    avg_salary = filtered['SALARY_USD'].mean()
    top_country = filtered.groupby('COUNTRY')['SALARY_USD'].mean().idxmax() if not filtered.empty else "N/A"
    avg_years = filtered['YEARS_CODING'].mode()[0] if not filtered.empty else "N/A"

    with col1:
        st.markdown(f"""
        <div class='metric-card'>
            <div class='metric-value'>{total_devs:,}</div>
            <div class='metric-label'>Total Developers</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class='metric-card'>
            <div class='metric-value'>${avg_salary:,.0f}</div>
            <div class='metric-label'>Avg Salary (USD)</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div class='metric-card'>
            <div class='metric-value'>{top_country[:12] if len(str(top_country)) > 12 else top_country}</div>
            <div class='metric-label'>Highest Paying Country</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
        <div class='metric-card'>
            <div class='metric-value'>{avg_years}</div>
            <div class='metric-label'>Most Common Experience</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Chart Row 1
    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("<div class='section-header'>💰 Average Salary by Country</div>", unsafe_allow_html=True)
        salary_by_country = (
            filtered.groupby('COUNTRY')['SALARY_USD']
            .mean()
            .sort_values(ascending=False)
            .head(top_n)
            .reset_index()
        )
        salary_by_country.columns = ['Country', 'Avg Salary']

        fig1 = px.bar(
            salary_by_country,
            x='Avg Salary', y='Country',
            orientation='h',
            color='Avg Salary',
            color_continuous_scale='Blues',
            template='plotly_dark'
        )
        fig1.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=10, b=0),
            height=350
        )
        st.plotly_chart(fig1, use_container_width=True)
        st.download_button(
            "⬇️ Download CSV",
            to_csv(salary_by_country),
            "salary_by_country.csv",
            "text/csv",
            key="dl_sal_country"
        )

    with col_r:
        st.markdown("<div class='section-header'>👥 Developers by Employment Type</div>", unsafe_allow_html=True)
        emp_dist = (
            filtered['EMPLOYMENT_TYPE']
            .value_counts()
            .head(top_n)
            .reset_index()
        )
        emp_dist.columns = ['Employment Type', 'Count']

        fig2 = px.pie(
            emp_dist,
            values='Count',
            names='Employment Type',
            color_discrete_sequence=px.colors.sequential.Blues_r,
            template='plotly_dark',
            hole=0.4
        )
        fig2.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=0, r=0, t=10, b=0),
            height=350
        )
        st.plotly_chart(fig2, use_container_width=True)
        st.download_button(
            "⬇️ Download CSV",
            to_csv(emp_dist),
            "employment_distribution.csv",
            "text/csv",
            key="dl_emp"
        )

    # Chart Row 2
    col_l2, col_r2 = st.columns(2)

    with col_l2:
        st.markdown("<div class='section-header'>📈 Salary by Years of Experience</div>", unsafe_allow_html=True)
        sal_exp = (
            filtered.groupby('YEARS_CODING')['SALARY_USD']
            .mean()
            .reset_index()
        )
        sal_exp.columns = ['Years Coding', 'Avg Salary']

        fig3 = px.bar(
            sal_exp,
            x='Years Coding', y='Avg Salary',
            color='Avg Salary',
            color_continuous_scale='Blues',
            template='plotly_dark'
        )
        fig3.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=10, b=0),
            height=350,
            xaxis_tickangle=45
        )
        st.plotly_chart(fig3, use_container_width=True)
        st.download_button(
            "⬇️ Download CSV",
            to_csv(sal_exp),
            "salary_by_experience.csv",
            "text/csv",
            key="dl_sal_exp"
        )

    with col_r2:
        st.markdown("<div class='section-header'>🏢 Salary by Company Size</div>", unsafe_allow_html=True)
        sal_comp = (
            filtered.groupby('COMPANY_SIZE')['SALARY_USD']
            .mean()
            .sort_values(ascending=False)
            .reset_index()
        )
        sal_comp.columns = ['Company Size', 'Avg Salary']

        fig4 = px.bar(
            sal_comp,
            x='Company Size', y='Avg Salary',
            color='Avg Salary',
            color_continuous_scale='Blues',
            template='plotly_dark'
        )
        fig4.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=10, b=0),
            height=350,
            xaxis_tickangle=45
        )
        st.plotly_chart(fig4, use_container_width=True)
        st.download_button(
            "⬇️ Download CSV",
            to_csv(sal_comp),
            "salary_by_company_size.csv",
            "text/csv",
            key="dl_sal_comp"
        )

    # Satisfaction Chart
    st.markdown("<div class='section-header'>😊 Satisfaction Scores</div>", unsafe_allow_html=True)
    
    sat_data = fact_df.merge(sat_df, on='SAT_TYPE_SK', how='left')
    sat_avg = (
        sat_data.groupby('SAT_TYPE_NAME')['SAT_SCORE']
        .mean()
        .reset_index()
    )
    sat_avg.columns = ['Satisfaction Type', 'Avg Score']

    fig5 = px.bar(
        sat_avg,
        x='Satisfaction Type', y='Avg Score',
        color='Avg Score',
        color_continuous_scale='Blues',
        template='plotly_dark',
        text='Avg Score'
    )
    fig5.update_traces(texttemplate='%{text:.2f}', textposition='outside')
    fig5.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        coloraxis_showscale=False,
        margin=dict(l=0, r=0, t=30, b=0),
        height=300
    )
    st.plotly_chart(fig5, use_container_width=True)
    st.download_button(
        "⬇️ Download CSV",
        to_csv(sat_avg),
        "satisfaction_scores.csv",
        "text/csv",
        key="dl_sat"
    )


# ══════════════════════════════════════════════════════
# TAB 2 — DEEP ANALYSIS
# ══════════════════════════════════════════════════════
with tab2:

    st.markdown("<div class='section-header'>🔍 Compare Countries Side by Side</div>", unsafe_allow_html=True)

    compare_countries = st.multiselect(
        "Select countries to compare",
        options=countries,
        default=countries[:5] if len(countries) >= 5 else countries,
        key="compare_sel"
    )

    if compare_countries:
        compare_df = filtered[filtered['COUNTRY'].isin(compare_countries)]
        
        col1, col2 = st.columns(2)
        
        with col1:
            avg_sal_compare = (
                compare_df.groupby('COUNTRY')['SALARY_USD']
                .mean()
                .reset_index()
            )
            avg_sal_compare.columns = ['Country', 'Avg Salary']
            
            fig_c1 = px.bar(
                avg_sal_compare,
                x='Country', y='Avg Salary',
                color='Country',
                template='plotly_dark',
                title='Avg Salary Comparison'
            )
            fig_c1.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                showlegend=False,
                height=350
            )
            st.plotly_chart(fig_c1, use_container_width=True)

        with col2:
            dev_count_compare = (
                compare_df.groupby('COUNTRY')
                .size()
                .reset_index(name='Developer Count')
            )
            
            fig_c2 = px.bar(
                dev_count_compare,
                x='COUNTRY', y='Developer Count',
                color='COUNTRY',
                template='plotly_dark',
                title='Developer Count Comparison'
            )
            fig_c2.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                showlegend=False,
                height=350
            )
            st.plotly_chart(fig_c2, use_container_width=True)

        combined = avg_sal_compare.merge(dev_count_compare, left_on='Country', right_on='COUNTRY')
        st.download_button(
            "⬇️ Download Comparison CSV",
            to_csv(combined),
            "country_comparison.csv",
            "text/csv",
            key="dl_compare"
        )

    st.markdown("<div class='section-header'>🏆 Top Earners</div>", unsafe_allow_html=True)

    top_earners = (
        filtered[['COUNTRY', 'EMPLOYMENT_TYPE', 'COMPANY_SIZE', 'YEARS_CODING', 'SALARY_USD']]
        .sort_values('SALARY_USD', ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    top_earners.index += 1

    st.dataframe(
        top_earners,
        use_container_width=True,
        height=350
    )
    st.download_button(
        "⬇️ Download Top Earners CSV",
        to_csv(top_earners),
        "top_earners.csv",
        "text/csv",
        key="dl_top"
    )


# ══════════════════════════════════════════════════════
# TAB 3 — AI CHATBOT
# ══════════════════════════════════════════════════════
with tab3:

    st.markdown("""
    <div style='background: linear-gradient(135deg, #1e293b, #162032); 
    border: 1px solid #1e3a5f; border-radius: 12px; padding: 1rem 1.5rem; 
    margin-bottom: 1rem;'>
        <span style='color: #38bdf8; font-family: Space Mono, monospace;'>🤖 AI Data Assistant</span><br>
        <span style='color: #64748b; font-size: 0.85rem;'>Ask questions about the Stack Overflow developer dataset. 
        I can generate insights, comparisons, and CSV exports.</span>
    </div>
    """, unsafe_allow_html=True)

    # Chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "chat_dataframes" not in st.session_state:
        st.session_state.chat_dataframes = {}

    # Display chat history
    for i, msg in enumerate(st.session_state.messages):
        if msg["role"] == "user":
            st.markdown(f"<div class='chat-message-user'>👤 {msg['content']}</div>", 
                       unsafe_allow_html=True)
        else:
            st.markdown(f"<div class='chat-message-ai'>🤖 {msg['content']}</div>", 
                       unsafe_allow_html=True)
            if f"df_{i}" in st.session_state.chat_dataframes:
                df = st.session_state.chat_dataframes[f"df_{i}"]
                st.dataframe(df, use_container_width=True)
                
                fig = px.bar(
                    df,
                    x=df.columns[0],
                    y=df.columns[1] if len(df.columns) > 1 else df.columns[0],
                    color_discrete_sequence=['#38bdf8'],
                    template='plotly_dark'
                )
                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    height=300
                )
                st.plotly_chart(fig, use_container_width=True)
                st.download_button(
                    "⬇️ Download this CSV",
                    to_csv(df),
                    f"query_result_{i}.csv",
                    "text/csv",
                    key=f"dl_chat_{i}"
                )

    # Input
    user_input = st.chat_input("Ask about the data... e.g. 'Top 10 countries by salary' or 'Compare job vs career satisfaction'")

    if user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})

        # Build data context
        summary_stats = f"""
        Dataset Summary:
        - Total developers: {len(filtered):,}
        - Average salary: ${filtered['SALARY_USD'].mean():,.2f}
        - Countries represented: {filtered['COUNTRY'].nunique()}
        - Employment types: {', '.join(filtered['EMPLOYMENT_TYPE'].dropna().unique()[:5])}
        - Top 5 countries by avg salary: {filtered.groupby('COUNTRY')['SALARY_USD'].mean().sort_values(ascending=False).head(5).to_dict()}
        - Satisfaction scores: {fact_df.merge(sat_df, on='SAT_TYPE_SK', how='left').groupby('SAT_TYPE_NAME')['SAT_SCORE'].mean().to_dict()}
        """

        # Check if data related
        data_keywords = ['salary', 'developer', 'country', 'satisfaction', 'employment', 
                        'experience', 'company', 'top', 'average', 'compare', 'highest', 
                        'lowest', 'technology', 'language', 'year', 'coding', 'data', 'show', 'list']
        
        is_data_query = any(kw in user_input.lower() for kw in data_keywords)

        if not is_data_query:
            response = "I can only answer questions related to the Stack Overflow developer survey data. Try asking about salaries, countries, satisfaction scores, employment types, or experience levels!"
            st.session_state.messages.append({"role": "assistant", "content": response})
        else:
            try:
                client = Groq(api_key=GROQ_API_KEY)

                prompt = f"""
You are a data analyst for Stack Overflow developer survey data.

{summary_stats}

User question: {user_input}

Instructions:
1. Answer the question using the data provided
2. If the user wants a comparison or top N list, provide it as a clean structured answer
3. If you generate tabular data, format it as: COLUMN1|COLUMN2|COLUMN3 with each row on a new line, starting with TABLE_DATA: on its own line
4. Keep answers concise and data-driven
5. Only answer questions about this dataset
"""
                response = client.chat.completions.create(
                    model=MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=800
                )

                ai_text = response.choices[0].message.content
                msg_index = len(st.session_state.messages)

                # Parse table data if present
                if "TABLE_DATA:" in ai_text:
                    parts = ai_text.split("TABLE_DATA:")
                    text_part = parts[0].strip()
                    table_part = parts[1].strip()
                    
                    rows = [r.strip() for r in table_part.split('\n') if '|' in r]
                    if rows:
                        headers = [h.strip() for h in rows[0].split('|')]
                        data_rows = [[c.strip() for c in r.split('|')] for r in rows[1:]]
                        df_result = pd.DataFrame(data_rows, columns=headers)
                        st.session_state.chat_dataframes[f"df_{msg_index}"] = df_result
                    
                    st.session_state.messages.append({"role": "assistant", "content": text_part})
                else:
                    st.session_state.messages.append({"role": "assistant", "content": ai_text})

            except Exception as e:
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": f"Error connecting to AI: {str(e)}"
                })

        st.rerun()