from dash import Dash, html, dcc, Input, Output
import numpy as np
import plotly.express as px
import dash_bootstrap_components as dbc
import datetime
import costs_data_svc
from flask import Flask, session
import pandas as pd

# syntax highlighting light or dark
light_hljs = "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.4.0/styles/stackoverflow-light.min.css"
dark_hljs = "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.4.0/styles/stackoverflow-dark.min.css"

# stylesheet with the .dbc class
dbc_css = (
    "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"
)

app = Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.FLATLY,
        dbc.icons.BOOTSTRAP,
        # dbc.icons.FONT_AWESOME,
        # dbc_css,
        # light_hljs,
        "main.css"
    ],
)
app.server.secret_key = 'sformiecekxixnxg'

search_bar = dbc.Row(
    [
        dbc.Col(dbc.Input(type="search", placeholder="Search")),
        dbc.Col(
            dbc.Button("Search", color="primary", className="ms-2",
                       n_clicks=0),
            width="auto",
        ),
    ],
    className="g-0 ms-auto flex-nowrap mt-3 mt-md-0",
    align="center",
)

navbar = dbc.Navbar(
    dbc.Container([
        html.A(
            # Use row and col to control vertical alignment of logo / brand
            dbc.Row(
                [
                    dbc.Col(dbc.NavbarBrand("Navbar", className="ms-2")),
                ],
                align="center",
                className="g-0",
            ),
            href="https://plotly.com",
            style={"textDecoration": "none"},
        ),
        dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
        dbc.Collapse(
            search_bar,
            id="navbar-collapse",
            is_open=False,
            navbar=True,
        ),
    ]),
    color="dark",
    dark=True,
)

heading = html.H4("Normal Distribution Simulation",
                  className="bg-primary text-white p-2")

# sample_size = html.Div(
#     [
#         dbc.Label("Sample Size", html_for="size"),
#         dcc.Slider(
#             1,
#             1000,
#             value=250,
#             id="size",
#             tooltip={
#                 "placement": "bottom",
#                 "always_visible": True
#             },
#         ),
#     ],
#     className="mt-2",
# )


# """
#         <span>
#             <span>Service Usage Type</span>
#             <span class="badge alert-info">Cost</span>
#             |<span class="badge alert-warning text-danger">Anomaly Count</span>
#             <span class="glyphicon glyphicon-question-sign" aria-hidden="true" title="Usage Type"></span>
#         </span>
#         """
accounts_input_group = html.Div(
    [
        dbc.Label(html.Span([
            html.Span("Grail AWS Account", className="category-name"),
            html.Span("Cost", className="badge alert-info"),
            "|",
            html.Span("Anomaly Count", className="badge alert-danger"),
            html.Span("",
                      className="glyphicon glyphicon-question-sign",
                      title="Grail Account"),
        ]),
            html_for="accounts"),
        dcc.Checklist(id="accounts", options=[], value=[], inline=True, inputClassName="cb-checkbox")
    ],
    className="mt-2 user-cb-options",
)

services_input_group = html.Div(
    [
        dbc.Label("AWS Services", html_for="services", className="category-name"),
        dcc.Checklist(id="services", options=[], value=[], inline=False)
    ],
    className="mt-2 user-cb-options",
)

# n_bins = html.Div(
#     [
#         dbc.Label("Number of Bins", html_for="n_bins"),
#         dcc.Slider(
#             1,
#             100,
#             10,
#             value=20,
#             id="n_bins",
#             tooltip={
#                 "placement": "bottom",
#                 "always_visible": True
#             },
#         ),
#     ],
#     className="mt-2",
# )

# mean = html.Div(
#     [
#         dbc.Label("Mean", html_for="mean"),
#         dbc.Input(id="mean", type="number", value=0)
#     ],
#     className="mt-2",
# )

# std_dev = html.Div(
#     [
#         dbc.Label("Standard Deviation", html_for="std_dev"),
#         dbc.Input(id="std_dev", type="number", value=1),
#     ],
#     className="mt-2",
# )

date_picker = html.Div(
    [
        dbc.Label("Date Range", html_for="usage-date-picker-range"),
        dcc.DatePickerRange(id='usage-date-picker-range',
                            min_date_allowed=datetime.date(2021, 12, 1),
                            max_date_allowed=datetime.date.today(),
                            start_date=datetime.date(2022, 10, 1),
                            end_date=datetime.date.today())
    ],
    className="mt-2",
)

control_panel = dbc.Card(
    dbc.CardBody(
        [
            date_picker,
            # sample_size,
            # n_bins,
            # mean,
            # std_dev,
            accounts_input_group,
            services_input_group,
            html.Button('Update Options', id='update-button'),
        ],
        className="bg-light",
    ))

graph = dbc.Card(
    [html.Div(id="error_msg", className="text-danger"),
     dcc.Graph(id="graph")])

app.layout = html.Div([
    navbar,
    dbc.Container(dbc.Row([dbc.Col(control_panel, md=3),
                           dbc.Col(graph, md=8)]),
                  className="selection-menu")
])

cnu_df = costs_data_svc.get_cnu_df_with_anomaly_info()


def get_cost_and_anomaly_weeks_by_category(category, fdf):
    adf = fdf.groupby([category, pd.Grouper(key='timestamp', freq='W-SUN')])[['Cost', 'Anomaly']] \
        .sum() \
        .reset_index()
    avg_cost = adf[adf['Anomaly'] == 0]['Cost'].mean()
    adf['AnomalyImpact'] = (adf['Cost'] - avg_cost) * (adf['Anomaly'] > 0)
    adf[['AnomalyImpact', category]].groupby(by=[category]).sum()
    adf['AnomalyCount'] = adf['Anomaly'] > 0
    sum_df = adf[[category, 'Cost', 'AnomalyImpact', 'AnomalyCount']] \
        .groupby(by=[category]).sum().reset_index() \
        .sort_values(by='AnomalyImpact', ascending=False)
    return sum_df


def get_options(df, category):
    cna_df = df.copy()
    # print(cna_df)
    cna_df['Label'] = cna_df.apply(lambda row: html.Span([
        html.Span(row[category], className="category-name"),
        html.Span(f'{row["Cost"]:,}', className="badge alert-info"),
        "|",
        html.Span(f'{row["AnomalyCount"]}', className="badge alert-danger"),
    ]), axis=1)
    opts_value = [{'label': label, 'value': value} for value, label in
                  cna_df[[category, 'Label']].itertuples(index=False, name=None)]
    return opts_value


@app.callback(Output(component_id='accounts', component_property='options'),
              Input('usage-date-picker-range', 'start_date'),
              Input('usage-date-picker-range', 'end_date'))
def update_accounts_output(start_date, end_date):
    if start_date is not None and end_date is not None:
        print(f"{start_date =} - {end_date =}")
        # significant_svc_df = \
        #     costs_data_svc.get_significant_svc_df(start_date, end_date)
        cnu_df = costs_data_svc.get_cnu_df_with_anomaly_info() \
            .query('timestamp >= @start_date and timestamp < @end_date')
        # print(significant_svc_df)
        category = 'GrailAccount'
        cna_df = get_cost_and_anomaly_weeks_by_category(category, cnu_df)
        return get_options(cna_df, category)
        # # print(cna_df)
        # cna_df['Label'] = cna_df.apply(lambda row: html.Span([
        #     html.Span(row[category],className="category-name"),
        #     html.Span(f'{row["Cost"]:,}', className="badge alert-info"),
        #     "|",
        #     html.Span(f'{row["AnomalyCount"]}', className="badge alert-danger"),
        # ]), axis=1)
        # opts_value = [{'label': label, 'value': value} for value, label in
        #         cna_df[[category, 'Label']].itertuples(index=False, name=None)]
        # return opts_value
        # session['significant_svc_df'] = \
        #     significant_svc_df = \
        #         costs_data_svc.get_significant_svc_df(start_date, end_date)
        # return [{
        #     'label':
        #     html.Span([
        #         html.Span(k),
        #         html.Span("Cost", className="badge alert-info"),
        #         "|",
        #         html.Span("Anomaly Count", className="badge alert-danger"),
        #         html.Span("",
        #                   className="glyphicon glyphicon-question-sign",
        #                   title="Grail Account"),
        #     ]),
        #     'value':
        #     k
        # } for k in significant_svc_df['GrailAccount'].unique().tolist()]
        # return significant_svc_df['GrailAccount'].unique().tolist()
    return None


@app.callback(Output(component_id='services', component_property='options'),
              Input('usage-date-picker-range', 'start_date'),
              Input('usage-date-picker-range', 'end_date'),
              Input('accounts', 'value'))
def update_services_output(start_date, end_date, account_list):
    if account_list is not None:
        cnu_df = costs_data_svc.get_cnu_df_with_anomaly_info() \
            .query('timestamp >= @start_date and timestamp < @end_date'
                   ' and GrailAccount in @account_list')
        category = 'Service'
        cna_df = get_cost_and_anomaly_weeks_by_category(category, cnu_df)
        return get_options(cna_df, category)
        #
        # significant_svc_df = \
        #     costs_data_svc.get_significant_svc_df(start_date, end_date)
        # return significant_svc_df.query('GrailAccount in @account_list') \
        #     ['Service'].unique().tolist()
    return None


# @app.callback(
#     Output("graph", "figure"),
#     Output("error_msg", "children"),
#     Input("mean", "value"),
#     Input("std_dev", "value"),
#     Input("n_bins", "value"),
#     Input("size", "value"),
# )
# def callback(m, std_dev, n_bins, n):
#     if m is None or std_dev is None:
#         return {}, "Please enter Standard Deviation and Mean"
#     if std_dev < 0:
#         return {}, "Please enter Standard Deviation > 0"
#     data = np.random.normal(m, std_dev, n)
#     return px.histogram(data, nbins=n_bins), None
#

# @app.callback(
#     Output(component_id='accounts', component_property='options'),
#     [Input(component_id='update-button', component_property='n_clicks')]
# )
# def update_options(n_clicks):
#     if n_clicks is None:
#         return [{'label': 'Option 1', 'value': 'option-1'}]
#     else:
#         return [{'label': 'Option 2', 'value': 'option-2'}, {'label': 'Option 3', 'value': 'option-3'}]
if __name__ == "__main__":
    app.run_server(debug=True)
