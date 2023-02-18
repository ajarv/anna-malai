import dash
import dash_html_components as html
from dash.dependencies import Input, Output

app = dash.Dash()

app.layout = html.Div([
    html.Button('Update Options', id='update-button'),
    html.Br(),
    html.Br(),
    html.Label('Select an option'),
    html.Select(id='dropdown', options=[{'label': 'Option 1', 'value': 'option-1'}]),
])

@app.callback(
    Output(component_id='dropdown', component_property='options'),
    [Input(component_id='update-button', component_property='n_clicks')]
)
def update_options(n_clicks):
    if n_clicks is None:
        return [{'label': 'Option 1', 'value': 'option-1'}]
    else:
        return [{'label': 'Option 2', 'value': 'option-2'}, {'label': 'Option 3', 'value': 'option-3'}]

if __name__ == '__main__':
    app.run_server()



grail-galleri-prod-mpk-cache-oregon 1.03 PB
grail-mrd-prod-cache-oregon 0.32 PB
grail-staging-mrd-cache-oregon 0.0 PB
grail-staging-galleri-mpk-cache-oregon 0.0 PB 

Johnson: Menu - Settings - Phone - SIM application - Lycamobile Services - Select Mode - Manual - National/USA Then restart the phone. If still the number is not latched into the network, please follow the below procedure to search the network manually. Menu - Setting - Carrier - Network Selection - Manual It will start searching for a network and then select Lycamobile or T-Mobile. Menu - Settings - Cellular - Turn on Enable data, data roaming & Enable 3G (LTE to access in 4G) Tap on Cellular data network APN: data.lycamobile.com (in lower case) MMSC: http://lyca.mmsmvno.com/mms/wapenc
01:53 PM