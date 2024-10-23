from dash import Dash, html, dcc
import plotly.express as px
import pandas as pd

df = pd.read_csv('airlinedelaycauses_DelayedFlights.csv')
df.to_pickle('airlinedelays.pkl')

sample30_df = df.sample(frac=0.3, random_state=42)  # random_state for reproducibility



# Create pie charts based on the data
def create_pie_chart(data, column):
    pie_chart = px.pie(data, names=column, title=f'Distribution of {column}')
    return pie_chart
# Initialize the Dash app
app = Dash(__name__)

# Define the app layout
app.layout = html.Div([
    dcc.Tabs([
        dcc.Tab(label='UniqueCarrier', children=[
            dcc.Graph(
                id='carrier-pie-chart',
                figure=create_pie_chart(sample30_df, 'UniqueCarrier')  # Replace 'Carrier' with the actual column name for carriers
            )
        ]),
        dcc.Tab(label='Origin', children=[
            dcc.Graph(
                id='month-pie-chart',
                figure=create_pie_chart(sample30_df, 'Origin')  # Replace 'Month' with the actual column name for months
            )
        ])
    ])
])

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
