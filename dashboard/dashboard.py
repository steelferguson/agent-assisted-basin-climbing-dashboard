import sys
sys.path.append('./src')
from dash import html, dcc, dash_table, Input, Output
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
from data_pipeline import upload_data 
from data_pipeline import config
import os
import plotly.io as pio

pio.templates.default = "plotly" # start off with plotly template as a clean slate

def load_df_from_s3(bucket, key):
    uploader = upload_data.DataUploader()
    csv_content = uploader.download_from_s3(bucket, key)
    return uploader.convert_csv_to_df(csv_content)

def load_data():
    
    # Load all needed DataFrames from S3
    df_memberships = load_df_from_s3(config.aws_bucket_name, config.s3_path_capitan_memberships)
    df_members = load_df_from_s3(config.aws_bucket_name, config.s3_path_capitan_members)
    df_transactions = load_df_from_s3(config.aws_bucket_name, config.s3_path_combined)
    df_projection = load_df_from_s3(config.aws_bucket_name, config.s3_path_capitan_membership_revenue_projection)

    return df_memberships, df_members, df_transactions, df_projection

def create_dashboard(app):
    
    df_memberships, df_members, df_combined, df_projection = load_data()
    
    app.layout = html.Div([
        # Timeframe toggle
        dcc.RadioItems(
            id='timeframe-toggle',
            options=[
                {'label': 'Day', 'value': 'D'},
                {'label': 'Week', 'value': 'W'},
                {'label': 'Month', 'value': 'M'}
            ],
            value='W',  # Default to "Week"
            inline=True,
            style={
                'backgroundColor': '#213B3F',
                'padding': '10px',
                'borderRadius': '5px',
                'color': '#FFFFFF',
                'marginBottom': '20px'
            }
        ),

        # Data source toggle for all revenue charts
        dcc.Checklist(
            id='source-toggle',
            options=[
                {'label': 'Square', 'value': 'Square'},
                {'label': 'Stripe', 'value': 'Stripe'}
            ],
            value=['Square', 'Stripe'],
            inline=True,
            style={'marginBottom': '20px'}
        ),

        # Total Revenue chart section
        html.H1(children='Total Revenue Over Time',
                style={'color': '#213B3F', 'marginTop': '30px'}),
        dcc.Graph(id='total-revenue-chart'),

        # Square and Stripe Revenue section
        html.Div([
            html.H1(children='Square and Stripe Revenue Analysis',
                    style={'color': '#213B3F', 'marginTop': '30px'}),
            dcc.Graph(id='square-stripe-revenue-chart'),
            dcc.Graph(id='square-stripe-revenue-stacked-chart'),
            dcc.Graph(id='revenue-percentage-chart'),
        ], style={'marginBottom': '40px'}),

        # Day Pass Count chart section
        html.H1(children='Day Pass Count',
                style={'color': '#213B3F', 'marginTop': '30px'}),
        dcc.Graph(id='day-pass-count-chart'),

        # Membership Revenue Projection chart section
        html.H1(children='Membership Revenue Projections (Current Month + 3 Months)',
                style={'color': '#213B3F', 'marginTop': '30px'}),
        html.Div([
            html.H3("Membership Frequency:"),
            dcc.Checklist(
                id='projection-frequency-toggle',
                options=[
                    {'label': 'Annual', 'value': 'yearly'},
                    {'label': 'Monthly', 'value': 'monthly'},
                    {'label': 'Bi-Weekly', 'value': 'bi_weekly'},
                    {'label': 'Prepaid', 'value': 'prepaid'}
                ],
                value=['yearly', 'monthly', 'bi_weekly', 'prepaid'],
                inline=True,
                style={'margin-bottom': '20px'}
            ),
            html.H3("Show Total Line:"),
            dcc.Checklist(
                id='show-total-toggle',
                options=[
                    {'label': 'Show Total', 'value': 'show_total'}
                ],
                value=['show_total'],
                inline=True
            )
        ]),
        dcc.Graph(id='membership-revenue-projection-chart'),

        # Membership Timeline chart section
        html.H1(children='Membership Timeline',
                style={'color': '#213B3F', 'marginTop': '30px'}),
        html.Div([
            html.H3("Membership Status:"),
            dcc.Checklist(
                id='status-toggle',
                options=[
                    {'label': 'Active', 'value': 'ACT'},
                    {'label': 'Ended', 'value': 'END'},
                    {'label': 'Frozen', 'value': 'FRZ'}
                ],
                value=['ACT', 'END'],  # Default to active and ended
                inline=True,
                style={'margin-bottom': '20px'}
            ),
            html.H3("Membership Frequency:"),
            dcc.Checklist(
                id='frequency-toggle',
                options=[
                    {'label': 'Bi-Weekly', 'value': 'bi_weekly'},
                    {'label': 'Monthly', 'value': 'monthly'},
                    {'label': 'Annual', 'value': 'annual'},
                    {'label': '3 Month Prepaid', 'value': 'prepaid_3mo'},
                    {'label': '6 Month Prepaid', 'value': 'prepaid_6mo'},
                    {'label': '12 Month Prepaid', 'value': 'prepaid_12mo'}
                ],
                value=['bi_weekly', 'monthly', 'annual', 'prepaid_3mo', 'prepaid_6mo', 'prepaid_12mo'],
                inline=True,
                style={'margin-bottom': '20px'}
            ),
            html.H3("Membership Size:"),
            dcc.Checklist(
                id='size-toggle',
                options=[
                    {'label': 'Solo', 'value': 'solo'},
                    {'label': 'Duo', 'value': 'duo'},
                    {'label': 'Family', 'value': 'family'},
                    {'label': 'Corporate', 'value': 'corporate'}
                ],
                value=['solo', 'duo', 'family', 'corporate'],
                inline=True,
                style={'margin-bottom': '20px'}
            ),
            html.H3("Special Categories:"),
            dcc.Checklist(
                id='category-toggle',
                options=[
                    {'label': 'Founder', 'value': 'founder'},
                    {'label': 'College', 'value': 'college'},
                    {'label': 'Corporate', 'value': 'corporate'},
                    {'label': 'Mid-Day', 'value': 'mid_day'},
                    {'label': 'Fitness Only', 'value': 'fitness_only'},
                    {'label': 'Has Fitness Addon', 'value': 'has_fitness_addon'},
                    {'label': 'Team Dues', 'value': 'team_dues'},
                    {'label': 'Include BCF Staff', 'value': 'include_bcf'},
                    {'label': 'Not in a Special Category', 'value': 'not_special'}
                ],
                value=['founder', 'college', 'corporate', 'mid_day', 'fitness_only', 'has_fitness_addon', 'team_dues', 'not_special'],
                inline=True,
                style={'margin-bottom': '20px'}
            ),
        ]),
        dcc.Graph(id='membership-timeline-chart'),

        # Youth Teams section
        html.H1(children='Youth Teams Membership',
                style={'color': '#213B3F', 'marginTop': '30px'}),
        dcc.Graph(id='youth-teams-chart'),

        # Birthday Rentals section
        html.H1(children='Birthday Rentals',
                style={'color': '#213B3F', 'marginTop': '30px'}),
        html.Div([
            dcc.Graph(id='birthday-participants-chart'),
            dcc.Graph(id='birthday-revenue-chart')
        ], style={
            'backgroundColor': '#F5F5F5',
            'padding': '20px',
            'borderRadius': '10px',
            'marginBottom': '40px'
        }),

        # Camps Revenue section
        html.H1(children='Camps Revenue',
                style={'color': '#213B3F', 'marginTop': '30px'}),
        html.Div([
            html.Div([
                html.H2("Camp Session Purchases", style={'textAlign': 'center', 'marginBottom': '20px'}),
                dcc.Graph(id='camp-sessions-chart')
            ], style={'marginBottom': '40px'}),
            html.Div([
                html.H2("Camp Revenue", style={'textAlign': 'center', 'marginBottom': '20px'}),
                dcc.Graph(id='camp-revenue-chart')
            ], style={'marginBottom': '40px'})
        ], style={
            'backgroundColor': '#F5F5F5',
            'padding': '20px',
            'borderRadius': '10px',
            'marginBottom': '40px'
        }),
    ], style={
        'margin': '0 auto',
        'maxWidth': '1200px',
        'padding': '20px',
        'backgroundColor': '#FFFFFF',
        'color': '#26241C',
        'fontFamily': 'Arial, sans-serif'
    })

    # Update the color scheme for all charts
    chart_colors = {
        'primary': '#AF5436',    # rust
        'secondary': '#E9C867',  # gold
        'tertiary': '#BCCDA3',   # sage
        'quaternary': '#213B3F', # dark teal
        'background': '#F5F5F5', # light grey background
        'text': '#26241C'       # dark grey
    }

    # Define a sequence of colors for categorical data
    categorical_colors = [
        chart_colors['primary'],    # rust
        chart_colors['secondary'],  # gold
        chart_colors['tertiary'],   # sage
        chart_colors['quaternary'], # dark teal
        '#8B4229',  # darker rust
        '#BAA052',  # darker gold
        '#96A682',  # darker sage
        '#1A2E31'   # darker teal
    ]

    # Callback for Total Revenue chart
    @app.callback(
        Output('total-revenue-chart', 'figure'),
        [Input('timeframe-toggle', 'value'),
         Input('source-toggle', 'value')]
    )
    def update_total_revenue_chart(selected_timeframe, selected_sources):
        df_filtered = df_combined[df_combined['Data Source'].isin(selected_sources)].copy()
        df_filtered['Date'] = pd.to_datetime(df_filtered['Date'], errors='coerce')
        df_filtered['date'] = df_filtered['Date'].dt.to_period(selected_timeframe).dt.start_time
        total_revenue = df_filtered.groupby('date')['Total Amount'].sum().reset_index()

        fig = px.line(total_revenue, x='date', y='Total Amount', title='Total Revenue Over Time')
        fig.update_traces(line_color=chart_colors['primary'])
        fig.update_layout(
            plot_bgcolor=chart_colors['background'],
            paper_bgcolor=chart_colors['background'],
            font_color=chart_colors['text']
        )
        return fig

    # Callback for Square and Stripe charts
    @app.callback(
        [Output('square-stripe-revenue-chart', 'figure'),
         Output('square-stripe-revenue-stacked-chart', 'figure'),
         Output('revenue-percentage-chart', 'figure')],
        [Input('timeframe-toggle', 'value'),
         Input('source-toggle', 'value')]
    )
    def update_square_stripe_charts(selected_timeframe, selected_sources):
        # Define revenue category colors and order
        revenue_category_colors = {
            'New Membership': chart_colors['secondary'],      # Gold
            'Membership Renewal': chart_colors['quaternary'], # Teal
            'Day Pass': chart_colors['primary'],             # Rust
            'Other': chart_colors['tertiary']                # Sage
        }

        # Define the order of categories
        category_order = ['New Membership', 'Membership Renewal', 'Day Pass', 'Other']

        # Filter and resample the Square and Stripe data
        df_filtered = df_combined[df_combined['Data Source'].isin(selected_sources)]
        df_filtered['Date'] = pd.to_datetime(df_filtered['Date'], errors='coerce')
        df_filtered['date'] = df_filtered['Date'].dt.to_period(selected_timeframe).dt.start_time
        revenue_by_category = df_filtered.groupby(['date', 'revenue_category'])['Total Amount'].sum().reset_index()

        # Line chart
        line_fig = px.line(revenue_by_category, x='date', y='Total Amount', color='revenue_category',
                          title='Revenue By Category Over Time',
                          category_orders={'revenue_category': category_order})
        line_fig.update_layout(
            plot_bgcolor=chart_colors['background'],
            paper_bgcolor=chart_colors['background'],
            font_color=chart_colors['text']
        )
        for category in revenue_category_colors:
            line_fig.update_traces(line_color=revenue_category_colors[category], selector=dict(name=category))

        # Stacked column chart
        stacked_fig = px.bar(revenue_by_category, x='date', y='Total Amount', color='revenue_category',
                            title='Revenue (Stacked Column)', barmode='stack',
                            category_orders={'revenue_category': category_order})
        stacked_fig.update_layout(
            plot_bgcolor=chart_colors['background'],
            paper_bgcolor=chart_colors['background'],
            font_color=chart_colors['text']
        )
        for category in revenue_category_colors:
            stacked_fig.update_traces(marker_color=revenue_category_colors[category], selector=dict(name=category))

        # Percentage chart
        total_revenue_per_date = revenue_by_category.groupby('date')['Total Amount'].sum().reset_index()
        total_revenue_per_date.columns = ['date', 'total_revenue']
        revenue_with_total = pd.merge(revenue_by_category, total_revenue_per_date, on='date')
        revenue_with_total['percentage'] = (revenue_with_total['Total Amount'] / revenue_with_total['total_revenue']) * 100

        percentage_fig = px.bar(revenue_with_total, x='date', y='percentage', color='revenue_category',
                              title='Percentage of Revenue by Category', barmode='stack',
                              category_orders={'revenue_category': category_order})
        percentage_fig.update_layout(
            plot_bgcolor=chart_colors['background'],
            paper_bgcolor=chart_colors['background'],
            font_color=chart_colors['text']
        )
        for category in revenue_category_colors:
            percentage_fig.update_traces(marker_color=revenue_category_colors[category], selector=dict(name=category))

        return line_fig, stacked_fig, percentage_fig

    # Callback for Day Pass chart
    @app.callback(
        Output('day-pass-count-chart', 'figure'),
        [Input('timeframe-toggle', 'value')]
    )
    def update_day_pass_chart(selected_timeframe):
        df_filtered = df_combined[df_combined['revenue_category'] == 'Day Pass'].copy()
        df_filtered['Date'] = pd.to_datetime(df_filtered['Date'], errors='coerce')
        df_filtered['date'] = df_filtered['Date'].dt.to_period(selected_timeframe).dt.start_time
        day_pass_sum = df_filtered.groupby('date')['Day Pass Count'].sum().reset_index(name='total_day_passes')

        fig = px.bar(day_pass_sum, x='date', y='total_day_passes', title='Total Day Passes Purchased')
        fig.update_traces(marker_color=chart_colors['quaternary'])  # Using teal color
        fig.update_layout(
            plot_bgcolor=chart_colors['background'],
            paper_bgcolor=chart_colors['background'],
            font_color=chart_colors['text']
        )
        return fig
        
    # Callback to update the Membership Revenue Projection chart
    @app.callback(
        Output('membership-revenue-projection-chart', 'figure'),
        [Input('timeframe-toggle', 'value'),
         Input('projection-frequency-toggle', 'value'),
         Input('show-total-toggle', 'value')]
    )
    def update_membership_revenue_projection_chart(selected_timeframe, selected_frequencies, show_total):
        # Filter for membership-related revenue categories
        membership_cats = ['Membership Renewal', 'New Membership']
        df = df_combined[df_combined['revenue_category'].isin(membership_cats)].copy()

        # Filter by frequency if needed
        if selected_frequencies:
            df = df[df['membership_freq'].isin(selected_frequencies)]

        # Convert Date to period and group
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['period'] = df['Date'].dt.to_period(selected_timeframe).dt.start_time

        # Group by period and (optionally) membership_size or other columns
        revenue_by_period = df.groupby('period')['Total Amount'].sum().reset_index()

        # Plot
        fig = px.bar(
            revenue_by_period,
            x='period',
            y='Total Amount',
            title='Membership Revenue Projection'
        )

        if show_total:
            fig.add_trace(
                go.Scatter(
                    x=revenue_by_period['period'],
                    y=revenue_by_period['Total Amount'],
                    mode='lines+markers',
                    name='Total',
                    line=dict(color='black', width=2)
                )
            )

        return fig

    # Callback to update the Membership Timeline chart
    @app.callback(
        Output('membership-timeline-chart', 'figure'),
        [Input('frequency-toggle', 'value'),
         Input('size-toggle', 'value'),
         Input('category-toggle', 'value'),
         Input('status-toggle', 'value')]
    )
    def update_membership_timeline_chart(frequency_toggle, size_toggle, category_toggle, status_toggle):
        # Load the processed membership DataFrame
        df = pd.read_csv('data/outputs/capitan_memberships.csv', parse_dates=['start_date', 'end_date'])

        # Filter by status
        df = df[df['status'].isin(status_toggle)]

        # Filter by frequency and size
        df = df[df['frequency'].isin(frequency_toggle)]
        df = df[df['size'].isin(size_toggle)]

        # Filter by category toggles (if you want to keep these)
        if 'include_bcf' not in category_toggle:
            df = df[~df['is_bcf']]
        if 'founder' not in category_toggle:
            df = df[~df['is_founder']]
        if 'college' not in category_toggle:
            df = df[~df['is_college']]
        if 'corporate' not in category_toggle:
            df = df[~df['is_corporate']]
        if 'mid_day' not in category_toggle:
            df = df[~df['is_mid_day']]
        if 'fitness_only' not in category_toggle:
            df = df[~df['is_fitness_only']]
        if 'has_fitness_addon' not in category_toggle:
            df = df[~df['has_fitness_addon']]
        if 'team_dues' not in category_toggle:
            df = df[~df['is_team_dues']]

        # Create a date range from the earliest start date to today
        min_date = df['start_date'].min()
        max_date = pd.Timestamp.now()
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')

        # Calculate active memberships for each day by frequency
        daily_counts = []
        for date in date_range:
            active = df[(df['start_date'] <= date) & (df['end_date'] >= date)]
            counts = active['frequency'].value_counts().to_dict()
            daily_counts.append({
                'date': date,
                **{freq: counts.get(freq, 0) for freq in frequency_toggle}
            })

        daily_counts_df = pd.DataFrame(daily_counts)

        # Plot
        fig = go.Figure()
        frequency_colors = {
            'bi_weekly': '#1f77b4',
            'monthly': '#ff7f0e',
            'annual': '#2ca02c',
            'prepaid_3mo': '#8B4229',
            'prepaid_6mo': '#BAA052',
            'prepaid_12mo': '#96A682',
            'unknown': '#1A2E31'
        }
        for freq in frequency_toggle:
            if freq in daily_counts_df.columns:
                fig.add_trace(go.Scatter(
                    x=daily_counts_df['date'],
                    y=daily_counts_df[freq],
                    mode='lines',
                    name=freq.replace('_', ' ').title(),
                    stackgroup='one',
                    line=dict(color=frequency_colors.get(freq, None))
                ))

        # Add total line
        total = daily_counts_df[frequency_toggle].sum(axis=1)
        fig.add_trace(go.Scatter(
            x=daily_counts_df['date'],
            y=total,
            mode='lines',
            name='Total',
            line=dict(color='#222222', width=2, dash='dash'),
            hovertemplate='Total: %{y}<extra></extra>'
        ))

        fig.update_layout(
            title='Active Memberships Over Time by Payment Frequency',
            showlegend=True,
            height=600,
            xaxis_title='Date',
            yaxis_title='Number of Active Memberships',
            hovermode='x unified'
        )
        return fig

    # Callback for Youth Teams chart
    @app.callback(
        Output('youth-teams-chart', 'figure'),
        [Input('timeframe-toggle', 'value')]
    )
    def update_youth_teams_chart(selected_timeframe):

        # Create a list to store youth team memberships
        youth_memberships = []
        
        # Process each membership
        for _, membership in df_memberships.iterrows():
            name = str(membership.get('name', '')).lower()
            status = membership.get('status')
            
            # Only include active memberships
            if status != 'ACT':
                continue
            
            # Determine team type
            team_type = None
            if 'recreation' in name or 'rec team' in name:
                team_type = 'Recreation'
            elif 'development' in name or 'dev team' in name:
                team_type = 'Development'
            elif 'competitive' in name or 'comp team' in name:
                team_type = 'Competitive'
            
            if team_type:
                start_date = pd.to_datetime(membership.get('start_date'), errors='coerce')
                end_date = pd.to_datetime(membership.get('end_date'), errors='coerce')
                
                if not pd.isna(start_date) and not pd.isna(end_date):
                    youth_memberships.append({
                        'team_type': team_type,
                        'start_date': start_date,
                        'end_date': end_date
                    })
        
        if not youth_memberships:
            fig = px.bar(title='No youth teams data available')
            fig.add_annotation(
                text="No youth teams data available",
                xref="paper", yref="paper",
                showarrow=False,
                font=dict(size=16)
            )
            return fig
        
        # Create a DataFrame from youth memberships
        df_youth = pd.DataFrame(youth_memberships)
        
        # Create a date range from the earliest start date to today
        min_date = df_youth['start_date'].min()
        max_date = datetime.now()
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')
        
        # Calculate active memberships for each day by team type
        daily_counts = []
        for date in date_range:
            active_memberships = df_youth[
                (df_youth['start_date'] <= date) & 
                (df_youth['end_date'] >= date)
            ]
            
            counts = active_memberships['team_type'].value_counts().to_dict()
            daily_counts.append({
                'date': date,
                'Recreation': counts.get('Recreation', 0),
                'Development': counts.get('Development', 0),
                'Competitive': counts.get('Competitive', 0)
            })
        
        daily_counts_df = pd.DataFrame(daily_counts)
        
        # Create the stacked line chart
        fig = go.Figure()
        
        # Define colors for each team type
        team_colors = {
            'Recreation': chart_colors['tertiary'],    # sage
            'Development': chart_colors['secondary'],  # gold
            'Competitive': chart_colors['primary']     # rust
        }
        
        # Add a line for each team type
        for team_type in ['Recreation', 'Development', 'Competitive']:
            fig.add_trace(go.Scatter(
                x=daily_counts_df['date'],
                y=daily_counts_df[team_type],
                mode='lines',
                name=team_type,
                stackgroup='one',
                line=dict(color=team_colors[team_type])
            ))
        
        # Add total line
        total = daily_counts_df[['Recreation', 'Development', 'Competitive']].sum(axis=1)
        fig.add_trace(go.Scatter(
            x=daily_counts_df['date'],
            y=total,
            mode='lines',
            name='Total',
            line=dict(color=chart_colors['text'], width=2, dash='dash'),
            hovertemplate='Total: %{y}<extra></extra>'
        ))
        
        # Update layout
        fig.update_layout(
            title='Youth Teams Membership Over Time',
            showlegend=True,
            height=600,
            xaxis_title='Date',
            yaxis_title='Number of Team Members',
            hovermode='x unified',
            plot_bgcolor=chart_colors['background'],
            paper_bgcolor=chart_colors['background'],
            font_color=chart_colors['text']
        )
        
        return fig

    # Callback for Birthday Participants chart
    @app.callback(
        Output('birthday-participants-chart', 'figure'),
        [Input('timeframe-toggle', 'value')]
    )
    def update_birthday_participants_chart(selected_timeframe):
        # Filter for birthday transactions
        df_filtered = df_combined[df_combined['sub_category'] == 'birthday'].copy()
        df_filtered['Date'] = pd.to_datetime(df_filtered['Date'], errors='coerce')
        df_filtered['date'] = df_filtered['Date'].dt.to_period(selected_timeframe).dt.start_time
        
        # Group by date and sub_category_detail to count transactions
        birthday_counts = df_filtered.groupby(['date', 'sub_category_detail']).size().unstack(fill_value=0)
        
        # Create the clustered column chart
        fig = go.Figure()
        
        # Add bars for initial payments
        fig.add_trace(go.Bar(
            x=birthday_counts.index,
            y=birthday_counts.get('initial payment', 0),
            name='Initial Payment',
            marker_color=chart_colors['quaternary']  # dark teal
        ))
        
        # Add bars for second payments
        fig.add_trace(go.Bar(
            x=birthday_counts.index,
            y=birthday_counts.get('second payment', 0),
            name='Second Payment',
            marker_color=chart_colors['primary']  # rust
        ))
        
        # Update layout
        fig.update_layout(
            title='Birthday Party Participants',
            xaxis_title='Date',
            yaxis_title='Number of Transactions',
            barmode='group',
            plot_bgcolor=chart_colors['background'],
            paper_bgcolor=chart_colors['background'],
            font_color=chart_colors['text']
        )
        
        return fig

    # Callback for Birthday Revenue chart
    @app.callback(
        Output('birthday-revenue-chart', 'figure'),
        [Input('timeframe-toggle', 'value')]
    )
    def update_birthday_revenue_chart(selected_timeframe):
        # Filter for birthday transactions
        df_filtered = df_combined[df_combined['sub_category'] == 'birthday'].copy()
        df_filtered['Date'] = pd.to_datetime(df_filtered['Date'], errors='coerce')
        df_filtered['date'] = df_filtered['Date'].dt.to_period(selected_timeframe).dt.start_time
        
        # Calculate total revenue by date
        birthday_revenue = df_filtered.groupby('date')['Total Amount'].sum().reset_index()
        
        # Create the line chart
        fig = px.line(birthday_revenue, x='date', y='Total Amount', title='Birthday Party Revenue')
        
        # Update trace color
        fig.update_traces(line_color=chart_colors['quaternary'])  # dark teal
        
        # Update layout
        fig.update_layout(
            plot_bgcolor=chart_colors['background'],
            paper_bgcolor=chart_colors['background'],
            font_color=chart_colors['text']
        )
        
        return fig

    # Callback for Camp Sessions chart
    @app.callback(
        Output('camp-sessions-chart', 'figure'),
        [Input('timeframe-toggle', 'value')]
    )
    def update_camp_sessions_chart(selected_timeframe):
        # Filter for camp sessions
        camp_data = df_combined[df_combined['sub_category'] == 'camps'].copy()
        
        if camp_data.empty:
            fig = px.bar(title='No camp session data available')
            fig.add_annotation(
                text="No camp session data available",
                xref="paper", yref="paper",
                showarrow=False,
                font=dict(size=16)
            )
            return fig

        print("camp_data for plotting:", camp_data.head())
        print("Columns:", camp_data.columns)

        # Convert dates to the selected timeframe and format them nicely
        camp_data['Date'] = pd.to_datetime(camp_data['Date'], errors='coerce')
        camp_data['date'] = camp_data['Date'].dt.to_period(selected_timeframe).dt.start_time
        camp_data['formatted_date'] = camp_data['date'].dt.strftime('%b %Y')  # Format as "Jan 2024"
        
        # Extract session number and start date from description
        camp_data['session_number'] = camp_data['Description'].str.extract(r'Session (\d+)')
        camp_data['start_date'] = camp_data['Description'].str.extract(r'(\d{1,2}/\d{1,2}/\d{4})')
        
        # Create a combined label with session number and start date
        camp_data['session_label'] = camp_data.apply(
            lambda x: f"Session {x['session_number']} ({x['start_date']})" 
            if pd.notna(x['session_number']) and pd.notna(x['start_date']) 
            else f"Session {x['session_number']}" if pd.notna(x['session_number'])
            else x['Description'].replace('Summer Camp ', ''), 
            axis=1
        )
        
        # Group by session and purchase period
        camp_counts = camp_data.groupby(['session_label', 'formatted_date']).size().reset_index(name='count')
 
        
        # Create stacked bar chart
        fig = px.bar(
            camp_counts,
            x='session_label',
            y='count',
            color='formatted_date',
            title='Camp Session Purchases by Session and Purchase Period',
            labels={
                'session_label': 'Camp Session',
                'count': 'Number of Purchases',
                'formatted_date': 'Purchase Period'
            },
            template='plotly' # can remove this line later
            # color_discrete_sequence=[
            #     chart_colors['primary'],    # rust
            #     chart_colors['secondary'],  # gold
            #     chart_colors['tertiary'],   # sage
            #     chart_colors['quaternary'], # dark teal
            #     '#8B4229',  # darker rust
            #     '#BAA052',  # darker gold
            #     '#96A682',  # darker sage
            #     '#1A2E31'   # darker teal
            # ]
        )
        
        # Format the date display in the legend
        fig.update_traces(
            hovertemplate="<br>".join([
                "Session: %{x}",
                "Purchase Period: %{customdata[0]}",
                "Number of Purchases: %{y}"
            ]),
            customdata=camp_counts[['formatted_date']].values
        )
        
        fig.update_layout(
            barmode='stack',
            xaxis_title='Camp Session',
            yaxis_title='Number of Purchases',
            legend_title='Purchase Period',
            height=500,
            plot_bgcolor=chart_colors['background'],
            paper_bgcolor=chart_colors['background'],
            font_color=chart_colors['text']
        )
        
        return fig

    # Callback for Camp Revenue chart
    @app.callback(
        Output('camp-revenue-chart', 'figure'),
        [Input('timeframe-toggle', 'value')]
    )
    def update_camp_revenue_chart(selected_timeframe):
        # Filter for camp sessions
        camp_data = df_combined[df_combined['sub_category'] == 'camps'].copy()
        
        if camp_data.empty:
            fig = px.bar(title='No camp session data available')
            fig.add_annotation(
                text="No camp session data available",
                xref="paper", yref="paper",
                showarrow=False,
                font=dict(size=16)
            )
            return fig

        # Convert dates to the selected timeframe and group by date
        camp_data['Date'] = pd.to_datetime(camp_data['Date'], errors='coerce')
        camp_data['date'] = camp_data['Date'].dt.to_period(selected_timeframe).dt.start_time
        camp_revenue = camp_data.groupby('date')['Total Amount'].sum().reset_index()
        
        # Create the bar chart
        fig = px.line(
            camp_revenue, 
            x='date', 
            y='Total Amount', 
            title='Camp Session Revenue Over Time'
        )
        
        # Update trace color
        fig.update_traces(line_color=chart_colors['quaternary'])  # dark teal
        
        # Update layout
        fig.update_layout(
            plot_bgcolor=chart_colors['background'],
            paper_bgcolor=chart_colors['background'],
            font_color=chart_colors['text']
        )
        
        return fig
