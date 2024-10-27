from dash import Dash, html, dcc, Input, Output, callback
import plotly.graph_objects as go
import calendar
import plotly.express as px
import pandas as pd

# Read flight data and airport coordinates
df = pd.read_csv('airlinedelaycauses_DelayedFlights.csv')
airports_df = pd.read_csv('us-airports.csv')  

# Take only the necessary columns out of this airports information dataframe
airports_df = airports_df[['local_code', 'latitude_deg', 'longitude_deg','name','iata_code']]

#taking a small 10% random sample to increase speed since it is a super large dataset
sample10_df = df.sample(frac=0.1, random_state=11)


# creating the average arrival delays, grouping them on destination instead of origin
# since arrival delays occur at the destination airport
arrdelay_df = sample10_df.groupby('Dest').agg(
    Avg_ArrDelay=('ArrDelay', 'mean')
).reset_index() 

# calculate averages based on origin, like departure delay, percentage cancelled, and average non-carrier delays
avg_metrics = sample10_df.groupby('Origin').agg(
    Avg_DepDelay=('DepDelay', 'mean'),
    Pct_Cancelled=('Cancelled', lambda x: x.mean() * 100),
    Avg_WeatherDelay=('WeatherDelay', 'mean'),
    Avg_NASDelay = ('NASDelay', 'mean'),
    Avg_SecurityDelay = ('SecurityDelay','mean'),
    Avg_LateAircraft = ('LateAircraftDelay','mean')
).reset_index()

# adding all non-carrier delays together
avg_metrics['Avg_NonCarrierDelay'] = avg_metrics[['Avg_WeatherDelay', 'Avg_NASDelay', 'Avg_SecurityDelay', 'Avg_LateAircraft']].sum(axis=1)


# merging the arrival delay metric with the other calculated metrics
metrics_df = pd.merge(arrdelay_df, avg_metrics, left_on='Dest', right_on='Origin', how='outer')

# using origin column to merge the airport codes on
metrics_df['airport_code'] = metrics_df['Origin'].fillna(metrics_df['Dest'])

# merging calculated metrics with the airport infor dataframe
merged_df = pd.merge(metrics_df, airports_df, left_on='airport_code', right_on='local_code', how='left')


# Initialize the Dash app
app = Dash(__name__)

# Code for generating map on Avg Delays and Delay types tab
def create_airport_map(data,top_airports):
    # scatter geo creates a map with points lat/long coordinates we get from the airports dataset
    fig = px.scatter_geo(data,
                         lat='latitude_deg', lon='longitude_deg',
                         hover_name='Origin',
                         hover_data={'Avg_ArrDelay': True, 'Pct_Cancelled': True, 'Avg_NonCarrierDelay': True},
                         title="US Airports",
                         projection="albers usa")
    
    # highlighting the top 3 in red from whatever metric is chosen for visibility
    fig.add_scattergeo(
        lat=top_airports['latitude_deg'],
        lon=top_airports['longitude_deg'],
        mode='markers',
        marker=dict(size=10, color='red'), 
        text=top_airports['Origin'],
        name="Top 3 Airports"
    )

    # visual things on the map
    fig.update_geos(
        scope='usa',
        showland=True, landcolor="lightgreen",
        showocean=True, oceancolor="lightblue",
        showlakes=True, lakecolor="lightblue",
        showcountries=True, countrycolor="black"
    )
    return fig



# function for creating the bar chart on the first tab, shows whatever metric is selected in the topleft
def create_metric_chart(data, metric):

    if metric == 'ArrDelay':
        # sorting values descending, meaning you get the 10 highest on top 
        top10_avg = data.sort_values('Avg_ArrDelay', ascending=False).head(10)
        fig = px.bar(top10_avg, x='airport_code', y='Avg_ArrDelay', title='Average arrival delay per airport')

    elif metric == 'Cancelled':
        top10_cancelled = data.sort_values('Pct_Cancelled', ascending=False).head(10)
        fig = px.bar(top10_cancelled, x='airport_code', y='Pct_Cancelled', title='Percentage of cancelled flights')

    elif metric == 'NonCarrierDelay':
        top10_noncarry = data.sort_values('Avg_NonCarrierDelay', ascending=False).head(10)
        fig = px.bar(top10_noncarry, x='airport_code', y='Avg_NonCarrierDelay', title='Average non-carrier delays')

    elif metric == 'DepDelay':
        top10_dep = data.sort_values('Avg_DepDelay', ascending=False).head(10)
        fig = px.bar(top10_dep, x='airport_code', y='Avg_DepDelay', title='Average departure delay per airport')

    else:
        fig = px.bar(title="Select a metric to view data")

    return fig



# app layout, this part of the code is used for everything visual, creating the tabs, adding buttons, showing charts
app.layout = html.Div([

    # dcc.Tabs indicate a new tab on the dashboard site, to keep things organized in their own categories
    dcc.Tabs([
        # first tab, shows avg delays, delay types, cancellation average
        dcc.Tab(label='Avg delays, delay types, and cancellations', children=[
            html.Div([
                # radio buttons to choose 
                dcc.RadioItems(
                    id='map-metric',
                    options=[
                        {'label': 'Average arrival delay', 'value': 'ArrDelay'},
                        {'label': 'Percentage of canceled flights', 'value': 'Cancelled'},
                        {'label': 'Non-carrier delays (Weather, NAS, Security, Late aircraft)', 'value': 'NonCarrierDelay'},
                        {'label': 'Average departure delay', 'value': 'DepDelay'},
                    ],
                    value='ArrDelay',  # default value for radiobutton
                    labelStyle={'display': 'block'}
                ),
                # putting map and charts next to each other
                html.Div([
                    dcc.Graph(id='airport-map', style={'display': 'inline-block', 'width': '50%'}),
                    dcc.Graph(id='metric-chart', style={'display': 'inline-block', 'width': '50%'})
                ]),
                # adding chart that shows monthly flights to find correlations
                dcc.Graph(id='monthly-flight-chart', style={'width': '50%', 'float': 'right', 'padding-top': '10px'})

            ]),
            # adding checkboxes so that user can choose what months are included in all analysis
            html.Div([
                dcc.Checklist(
                    id='month-checklist',
                    options=[
                        {'label': 'Include All', 'value': 'All'},
                        {'label': 'January', 'value': 1},
                        {'label': 'February', 'value': 2},
                        {'label': 'March', 'value': 3},
                        {'label': 'April', 'value': 4},
                        {'label': 'May', 'value': 5},
                        {'label': 'June', 'value': 6},
                        {'label': 'July', 'value': 7},
                        {'label': 'August', 'value': 8},
                        {'label': 'September', 'value': 9},
                        {'label': 'October', 'value': 10},
                        {'label': 'November', 'value': 11},
                        {'label': 'December', 'value': 12},
                    ],
                    value=['All'],  # standard value is all months included
                    labelStyle={'display': 'block'}  
                )
            ]),
        ]),
    
        dcc.Tab(label='Origin airport analysis', children=[
            html.Div([
                
                html.Div([
                    dcc.Dropdown(
                        id='origin-airport',
                        options=[{'label': origin, 'value': origin} 
                                for origin in sorted(sample10_df['Origin'].unique())],
                        value='JFK',
                        placeholder="Select Origin Airport",
                        style={'width': '200px', 'margin': '10px'}
                    ),
                    
                    dcc.Dropdown(
                        id='route-metric',
                        options=[
                            {'label': 'Total Flights', 'value': 'flights'},
                            {'label': 'Average Delay', 'value': 'delay'}
                        ],
                        value='flights',
                        style={'width': '200px', 'margin': '10px'}
                    )
                ], style={'display': 'flex', 'gap': '20px'}),
                
                # First row of visualizations
                html.Div([
                    dcc.Graph(id='route-map', style={'width': '50%'}),
                    dcc.Graph(id='carrier-competition', style={'width': '50%'})
                ], style={'display': 'flex'}),
                
                # Second row of visualizations
                html.Div([
                    dcc.Graph(id='route-performance', style={'width': '50%'}),
                    dcc.Graph(id='time-performance', style={'width': '50%'})
                ], style={'display': 'flex'}),
                
                # Route statistics card
                html.Div(id='route-stats', className='stats-card')
            ])
        ]),

        # third tab on flight 
        dcc.Tab(label='Flight connection analysis', children=[
            html.Div([
                html.Div([                
                    dcc.Dropdown(
                    id='Origin',
                    options=[{'label': origin, 'value': origin} 
                                for origin in sorted(sample10_df['Origin'].unique())],
                    value='JFK',
                    placeholder="Select Origin Airport",
                    style={'width': '200px', 'margin': '10px'}
                    ),
                    dcc.Dropdown(
                    id='Destination',
                    placeholder="Select Destination Airport",
                    style={'width': '400px', 'margin': '10px'}
                    ),
                    html.Div(id='Possible Destinations',
                            style={'width': '400px', 'margin': '10px'})
                ], style={'display': 'flex', 'gap': '20px'}),
                html.Div([
                    dcc.Dropdown(
                        id='TimeFrame',
                        options = ['Day', 'Month'],
                        value='Day',
                        style={'width': '100px', 'margin': 'auto'}
                    )
                ],style={}, className='justify-content-center'
                ),
                dcc.Graph(id='delay-each-day'),
                html.Div([
                    dcc.Graph(id='Delay_Pie_Chart'),
                    dcc.Graph(id='Carrier_Pie_Chart'),
                    dcc.Graph(id='Delay_Distribution')
                ], style={'display': 'flex'})
            ])
        ])
    ])
])

# callback for map and charts on first tab
@app.callback(
    [Output('airport-map', 'figure'),
     Output('metric-chart', 'figure'),
     Output('monthly-flight-chart', 'figure')],
    [Input('map-metric', 'value'),
     Input('month-checklist', 'value')] # inputs are what metric and what month(s)
)

###############################
# Flight map and delays chart #
###############################
# This is the code that updates both the map and the charts based on the chosen metric and chosen month(s)
def update_map_and_chart(selected_metric, selected_months):
    
    # filtering data based on selected months
    if 'All' in selected_months:
        filtered_month_data = sample10_df
    else:
        filtered_month_data = sample10_df[sample10_df['Month'].isin(selected_months)]

    # have to recompute based on filtered data
    metrics_df = filtered_month_data.groupby('Dest').agg(
        Avg_ArrDelay=('ArrDelay', 'mean')
    ).reset_index()
    
    # recalculating after filter
    avg_metrics = filtered_month_data.groupby('Origin').agg(
        Avg_DepDelay=('DepDelay', 'mean'),
        Pct_Cancelled=('Cancelled', lambda x: x.mean() * 100),
        Avg_WeatherDelay=('WeatherDelay', 'mean'),
        Avg_NASDelay = ('NASDelay', 'mean'),
        Avg_SecurityDelay = ('SecurityDelay','mean'),
        Avg_LateAircraft = ('LateAircraftDelay','mean')
    ).reset_index()

    # adding all non-carrier delays together again
    avg_metrics['Avg_NonCarrierDelay'] = avg_metrics[['Avg_WeatherDelay', 'Avg_NASDelay', 'Avg_SecurityDelay', 'Avg_LateAircraft']].sum(axis=1)
    
    # merging again
    metrics_df = pd.merge(metrics_df, avg_metrics, left_on='Dest', right_on='Origin', how='outer')
    metrics_df['airport_code'] = metrics_df['Origin'].fillna(metrics_df['Dest'])
    merged_df = pd.merge(metrics_df, airports_df, left_on='airport_code', right_on='local_code', how='left')


    # find top 3 airports based on chosen metric
    if selected_metric == 'ArrDelay':
        top_airports = merged_df.nlargest(3, 'Avg_ArrDelay')
    elif selected_metric == 'Cancelled':
        top_airports = merged_df.nlargest(3, 'Pct_Cancelled')
    elif selected_metric == 'NonCarrierDelay':
        top_airports = merged_df.nlargest(3, 'Avg_NonCarrierDelay')
    elif selected_metric == 'DepDelay':
        top_airports = merged_df.nlargest(3, 'Avg_DepDelay')

    # calling map and chart 
    map_figure = create_airport_map(merged_df, top_airports)
    chart_figure = create_metric_chart(merged_df, selected_metric)

    # line chart for the flights per month, for comparison
    monthly_counts = sample10_df.groupby('Month').size()
    line_chart = px.line(
        x=monthly_counts.index,
        y=monthly_counts.values,
        labels={'x': 'Month', 'y': 'Flight Count'},
        title='Monthly Flight Counts'
    )

    return map_figure, chart_figure, line_chart



# Callback for airport origin tab
@callback(
    [Output('route-map', 'figure'),
     Output('carrier-competition', 'figure'),
     Output('route-performance', 'figure'),
     Output('time-performance', 'figure')],
    [Input('origin-airport', 'value'),
     Input('route-metric', 'value')]
)


def origin_airport_analysis(origin_airport, metric):
   
   # filter based on chosen airport
    if origin_airport: # check if origin airport is empty or not
        df_routes = sample10_df[sample10_df['Origin'] == origin_airport]
    else:
        df_routes = sample10_df
    
    # creating the routes on the map
    route_coords = pd.merge(
        pd.merge(df_routes, airports_df, left_on='Origin', right_on='local_code', how='left'),
        airports_df, left_on='Dest', right_on='local_code', how='left',
        suffixes=('_origin', '_dest')
    )
    
    # creating actual routes
    route_stats = route_coords.groupby(['Origin', 'Dest', 'latitude_deg_origin', 'longitude_deg_origin', 
                                      'latitude_deg_dest', 'longitude_deg_dest']).agg({ 'FlightNum': 'count', 'ArrDelay': 'mean','AirTime': 'mean'}).reset_index()
    
    map_fig = go.Figure()
    
    # Add routes as lines
    for _, route in route_stats.iterrows():
        map_fig.add_trace(go.Scattergeo(
            lon=[route['longitude_deg_origin'], route['longitude_deg_dest']],
            lat=[route['latitude_deg_origin'], route['latitude_deg_dest']],
            mode='lines',
            line=dict(width=1, color='blue'),
            opacity=0.6,
            hoverinfo='text',
            text=f"{route['Origin']} → {route['Dest']}<br>Flights: {route['FlightNum']}"
        ))
    
    # update the map with the routes
    map_fig.update_layout(
        title='Route Network',
        geo=dict(
            scope='usa',
            projection_type='albers usa',
            showland=True,
            landcolor='rgb(243, 243, 243)',
            countrycolor='rgb(204, 204, 204)'
        )
    )
    
    # sunburst charts to see what airlines fly what routes
    top_routes = df_routes.groupby(['Origin', 'Dest']).size().reset_index(name='flights') 
    top_routes = top_routes.nlargest(5, 'flights') # taking 5 most used routes
    
 
    carrier_competition = df_routes.merge(top_routes[['Origin', 'Dest']], on=['Origin', 'Dest'])
    
    # creating sunburst
    competition_fig = px.sunburst( carrier_competition, path=['Origin', 'Dest', 'UniqueCarrier'], values='Distance', title='Biggest carrier per destination')
    

    # amount of flights to destinations
    performance_metrics = df_routes.groupby('Dest').agg({
        'FlightNum': 'count',
        'ArrDelay': 'mean',
        'Cancelled': 'mean',  
        'AirTime': 'mean'
    }).reset_index()

    # cancelled as a percentage rather than a fraction
    performance_metrics['Cancelled'] = performance_metrics['Cancelled'] * 100
    

    # what do we want to display
    if metric == 'flights':
        y_val = 'FlightNum'
        title = 'Number of flights by destination'
        y_label = 'Total flights'
    elif metric == 'delay':
        y_val = 'ArrDelay'
        title = 'Average delay by destination'
        y_label = 'Minutes'
 

    # displaying amount of flights for all destinations
    nr_flights_fig = px.bar(
        performance_metrics.nlargest(10, y_val),
        x='Dest',
        y=y_val,
        title=title
    )

    nr_flights_fig.update_layout(
        xaxis_title='Destination',
        yaxis_title=y_label
    )
    
    # flights for each time of the day
    df_routes['Hour'] = (df_routes['DepTime'] / 100).astype(int)
    time_performance = df_routes.groupby('Hour').agg({
        'FlightNum': 'count',
        'ArrDelay': 'mean',
        'Cancelled': lambda x: (x == 1).mean() * 100
    }).reset_index()
    
    time_fig = go.Figure()
    
    # adding a trace through this time of day scatterplot to make a line chart
    time_fig.add_trace(go.Scatter(
        x=time_performance['Hour'],
        y=time_performance['FlightNum'],
        name='Flights',
        mode='lines+markers'
    ))
    
    
    time_fig.update_layout(
        title='Time of Day Analysis',
        xaxis_title='Hour of Day',
        yaxis_title='Number of Flights',
        legend=dict(x=1.1, y=1)
    )

    return map_fig, competition_fig, nr_flights_fig, time_fig


# Code for tab 'flight connection analysis'
@callback(
        [Output('Destination', 'options'),
         Output('Possible Destinations', 'children')],
        Input('Origin', 'value')
)

def dynamic_dropdown(origin):
    if origin == None:
        return [], html.Div([])
    
    destinationcodes = sample10_df[sample10_df['Origin'] == origin]['Dest'].unique()
    destinations = airports_df[airports_df['iata_code'].isin(destinationcodes)]
    n = destinations.shape[0]
    message = html.Div([html.P(f"There are {n} possible destinations")])
    return [{'label': row['iata_code']+": "+row['name'], 'value': row['iata_code']} for _,row in destinations.iterrows()], message


@callback(
    [Output('delay-each-day', 'figure'),
     Output('Delay_Pie_Chart', 'figure'),
     Output('Carrier_Pie_Chart', 'figure'),
     Output('Delay_Distribution', 'figure')],
    [Input('Origin', 'value'),
     Input('Destination', 'value'),
     Input('TimeFrame', 'value')]
)

def flight_connection_analysis_update(Origin, Destination, timeframe):
    delayfigure = go.Figure()

    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    delayTypes = ['CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay']

    if Origin == None or Destination == None:
        return delayfigure, go.Figure(), go.Figure(), go.Figure()
    
    selected_data = sample10_df[(sample10_df["Origin"] == Origin) & (sample10_df["Dest"] == Destination)]
    if selected_data.empty:
        return delayfigure, go.Figure(), go.Figure(), go.Figure()
    copy = selected_data

    # BAR PLOT

    if timeframe == 'Month':
        selected_data['Month'] = selected_data['Month'].map(lambda x: months[x - 1])
        avg_delay = selected_data.groupby('Month')['ArrDelay'].mean().reindex(months)
        xax = months
    elif timeframe == 'Day':
        selected_data['DayOfWeek'] = selected_data['DayOfWeek'].map(lambda x: weekdays[x - 1])
        avg_delay = selected_data.groupby('DayOfWeek')['ArrDelay'].mean().reindex(weekdays)
        xax = weekdays
        
    delayfigure.add_trace(go.Bar(
        x=avg_delay.index,
        y=avg_delay.values,
        marker_color='Blue',
    ))

    delayfigure.update_layout(
        title="Average Minutes of Arrival Delay in Delayed Flights",
        xaxis_title=timeframe,
        yaxis_title="Minutes of Delay",
        xaxis={'categoryorder': 'array', 'categoryarray': xax}
    )

    # PIE CHART
    delaycontributers = selected_data[delayTypes].mean()
    delayPieChart = go.Figure(
        data=[go.Pie(
            labels=delaycontributers.index,
            values=delaycontributers.values,
        )]
    )

    delayPieChart.update_layout(
        title="Average Delay Contribution by Type",
    )

    # PIE CHART 2
    carriercontribution = selected_data.groupby('UniqueCarrier').size()### DIT IS NOG FOUT
    carriercomparison = go.Figure(
        data=[go.Pie(
            labels=carriercontribution.index,
            values=carriercontribution.values
        )]
    )
    carriercomparison.update_layout(
        title="Carrier Contribution",
    )

    # PIE CHART 3
    def handledelaytype(row):
        if row['Cancelled'] == 1:
            return 'Cancelled'
        elif row['Diverted'] == 1:
            return 'Diverted'
        elif row ['ArrDelay'] > 60:
            return 'Severe Delay (>60 min)'
        elif row ['ArrDelay'] > 15:
            return 'Considerable Delay (>15 min, <60 min)'
        else: return 'Negligable Delay (<15 min)'

    copy['delaytype'] = copy.apply(handledelaytype, axis=1)
    delaycounts = copy['delaytype'].value_counts()

    delaydistribution = go.Figure(
        data=[go.Pie(
            labels=delaycounts.index,
            values=delaycounts.values
        )]
    )
    delaydistribution.update_layout(
        title="Delay Distribution",
    )



    return delayfigure, delayPieChart, carriercomparison, delaydistribution




# run it all
if __name__ == '__main__':
    app.run_server(debug=True)

