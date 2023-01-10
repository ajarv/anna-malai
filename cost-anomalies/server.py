# diamonds.py
from enum import unique
from pydoc import classname
from unicodedata import category
from shiny import App, ui, render, reactive, req
import plotnine as gg
import pandas as pd
import re
import datetime
from pathlib import Path
import collections
import costs_data_svc
from shiny.types import NavSetArg
from typing import List


# function that creates our UI based on the data
# we give it
def create_ui(cnu_df: pd.DataFrame):
    # calculate the set of unique choices that could be made
    # create our ui object

    max_date = cnu_df['timestamp'].max()
    min_date = cnu_df['timestamp'].min()
    start_date = max_date - datetime.timedelta(days=61)
    significant_svc_ut_df = costs_data_svc.get_significant_svc_df(
        start_date, max_date)

    svc_costs = significant_svc_ut_df[[
        'Service', 'Cost'
    ]].groupby('Service').sum().reset_index()
    svc_costs['Label'] = svc_costs['Service'] + svc_costs['Cost'].apply(
        lambda x: f"( {x:,} )")
    service_dict = dict(
        list(svc_costs[['Service', 'Label']].itertuples(index=False,
                                                        name=None)))

    app_ui = ui.page_bootstrap(
        ui.tags.head(
            ui.tags.link(rel="stylesheet", type="text/css", href="app.css"), ),
        # row and column here are functions
        # to aid laying out our page in an organised fashion
        ui.row(ui.column(8, ui.tags.h3("Cost Anomaly Detector"), offset=1)),
        ui.row(
            ui.column(
                2,
                ui.input_date_range(
                    "date_range",
                    "Date range:",
                    start=(max_date -
                           datetime.timedelta(days=60)).strftime('%Y-%m-%d'),
                    end=max_date.strftime('%Y-%m-%d'),
                    min=min_date.strftime('%Y-%m-%d'),
                    max=max_date.strftime('%Y-%m-%d'),
                    format="mm/dd/yy",
                    separator=" - ",
                ),
                ui.input_switch("granularity_month",
                                "Granularity Month / Day"),
                ui.input_switch("highlight_anomalies",
                                "Highlight cost intervals with Anomalies"),
                offset=1),
            ui.column(
                8,
                # an output container in which to render a plot
                ui.output_ui("title"),
            ),
        ),
        ui.row(
            ui.column(
                2,
                offset=1,
                *[
                    ui.hr(),
                    ui.input_checkbox_group(
                        "grail_account_group",
                        ui.HTML(f'''<span> 
                        <span>AWS Account Name</span>  
                        <span class="badge alert-info">Cost</span>
                        |<span class="badge alert-warning text-danger">Anomaly Count</span>
                        <span class="glyphicon glyphicon-question-sign" aria-hidden="true" 
                        title="Org Friendly Account Name corresponding to an unique AWS Account number"></span>
                    </span>'''),
                        {},
                    ),
                    ui.hr(),
                    ui.input_text("aws_service_group_filter", "Filter:", ""),
                    ui.input_checkbox_group(
                        "aws_service_group",
                        ui.HTML(f'''<span> 
                            <span>AWS Service</span>  
                            <span class="badge alert-info">Cost</span>
                            |<span class="badge alert-warning text-danger">Anomaly Count</span>
                            <span class="glyphicon glyphicon-question-sign" aria-hidden="true" 
                            title="AWS Service Name"></span>
                        </span>'''),
                        {},
                    ),
                    ui.hr(),
                    ui.input_text("aws_service_feature_group_filter",
                                  "Filter:", ""),
                    ui.input_checkbox_group(
                        "aws_service_feature_group",
                        ui.HTML(f'''<span> 
                        <span>Service Usage Type</span>  
                        <span class="badge alert-info">Cost</span>
                        |<span class="badge alert-warning text-danger">Anomaly Count</span>
                        <span class="glyphicon glyphicon-question-sign" aria-hidden="true" title="Usage Type"></span>
                    </span>'''),
                        {},
                    ),
                ],
            ),
            ui.column(
                8,
                # an output container in which to render a plot
                ui.output_plot("out", width="100%", height="600px"),
                # ui.output_text_verbatim("txt"),
                ui.output_text("txt"),
            )))
    return app_ui


# utility function to draw a scatter plot
def create_plot(df, highlight_anomalies, monthly=False, fill='UsageType'):
    if highlight_anomalies and df['AnomalyCount'].sum() > 0:
        usage_types_with_anomalies = df[['UsageType', 'AnomalyCount']] \
            .groupby('UsageType').sum().reset_index() \
            .query('AnomalyCount > 0')['UsageType'].unique().tolist()
        df = df.query('UsageType in @usage_types_with_anomalies')
    plot = (gg.ggplot(
        df, gg.aes(x='timestamp', y='Cost', fill=fill, alpha='AnomalyCount')) +
            gg.scale_alpha_continuous(range=(0.2, 1))
            ) if highlight_anomalies else (gg.ggplot(
                df, gg.aes(x='timestamp', y='Cost', fill=fill)))
    # plot = ( gg.ggplot(df, gg.aes(x = 'timestamp', y='Cost',
    #     fill='AnomalyCount' if highlight_anomalies else 'UsageType')))
    plot = (plot + gg.geom_bar(stat="identity") +
            gg.facet_grid("GrailAccount ~ .", scales="free", space="free") +
            gg.theme(axis_text_x=gg.element_text(angle=30, hjust=1)) +
            gg.labs(title=f"{'Monthly' if monthly else 'Daily'} Costs Plot",
                    x="Date",
                    y=f"{'Monthly' if monthly else 'Daily'} Cost USD"))
    return plot.draw()


def get_cost_and_anomaly_weeks_by_category(category, fdf):
    cost_sums = fdf \
        [[category, 'timestamp', 'Cost']] \
        .groupby(by=[category]).sum().reset_index()
    anomaly_sums = fdf[fdf['timestamp'].apply(lambda d: d.day_name()) == 'Saturday'] \
        [[category, 'timestamp', 'Anomaly']] \
        .groupby(by=[category]).sum().reset_index()
    sum_df = cost_sums.set_index([category]) \
        .join(anomaly_sums.set_index([category]), how='left') \
        .reset_index() \
        .sort_values(by='Cost', ascending=False)
    sum_df["Anomaly"] = sum_df["Anomaly"].fillna(0).astype(int)
    return sum_df


def get_cat_sizes_dict(category, fdf, key_filter):
    cat_sizes = get_cost_and_anomaly_weeks_by_category(category, fdf)

    cat_sizes['Label'] = cat_sizes.apply(lambda row: ui.HTML(f'''<span> 
                <span class="category-name">{row[category]}</span>  
                <span class="badge alert-info">$ {row["Cost"]:,}</span>
                |<span class="badge alert-warning text-danger">{row["Anomaly"]}</span>
            </span>'''),
                                         axis=1)
    cat_dict = dict(cat_sizes[[category, 'Label']].itertuples(index=False,
                                                              name=None))
    if key_filter and key_filter.strip() != '':
        try:
            rx = re.compile(key_filter)
            cat_dict = {k: v for k, v in cat_dict.items() if rx.search(k)}
        except:
            pass
    return cat_dict, cat_sizes


# wrapper function for the server, allows the data
# to be passed in
def create_server(cnu_df):

    def f(input, output, session):
        # --
        my_session_cache = {"selections": collections.defaultdict(set)}
        my_session_cache['df_day'] = cnu_df

        cnu_df['AnomalyCount'] = cnu_df['Anomaly'] + 0

        cnu_df['Month'] = pd.to_datetime(
            cnu_df['timestamp'].apply(lambda x: x.strftime('%Y-%m-01')))

        df_month = cnu_df[['Month', 'Service', 'UsageType', 'UsageUnit', 'GrailAccount', 'Cost', 'AnomalyCount']] \
            .groupby(by=['Month', 'Service', 'UsageType', 'UsageUnit', 'GrailAccount']) \
            .sum().reset_index().copy()
        df_month['timestamp'] = df_month['Month']
        df_month['AnomalyCount'] /= 31
        my_session_cache['df_month'] = df_month

        # --
        @reactive.Effect
        @reactive.event(input.date_range)
        def _a0_account():
            startD, endD = input.date_range()
            old_selected = input.grail_account_group()
            df_query = "timestamp >= @startD and timestamp < @endD"
            fdf = cnu_df.query(df_query)
            category = 'GrailAccount'
            cat_dict, cat_sizes = get_cat_sizes_dict(category, fdf, None)
            choices = cat_dict
            selected = old_selected or list(cat_dict.keys())[:1]
            ui.update_checkbox_group(
                "grail_account_group",
                choices=choices,
                selected=selected,
            )

        # --
        @reactive.Effect
        @reactive.event(input.date_range, input.grail_account_group,
                        input.aws_service_group_filter)
        def _a0_service():

            startD, endD = input.date_range()
            old_selected = input.aws_service_group()
            grail_account = input.grail_account_group()
            key_filter = input.aws_service_group_filter()
            df_query = "timestamp >= @startD and timestamp < @endD and GrailAccount in @grail_account"
            fdf = cnu_df.query(df_query)

            category = 'Service'
            cat_dict, cat_sizes = get_cat_sizes_dict(category, fdf, key_filter)
            choices = cat_dict
            selected = old_selected or list(cat_dict.keys())[:1]

            ui.update_checkbox_group(
                "aws_service_group",
                choices=choices,
                selected=selected,
            )

        # --
        @reactive.Effect
        # @reactive.event(input.date_range,
        #                 input.grail_account_group,
        #                 input.aws_service_group_filter,
        #                 input.aws_service_feature_group_filter)
        def _a0_usagetype():
            aws_services = input.aws_service_group()
            startD, endD = input.date_range()
            grail_account = input.grail_account_group()

            old_selected = input.aws_service_feature_group()
            key_filter = input.aws_service_feature_group_filter()

            df_query = "timestamp >= @startD and timestamp < @endD" + \
                       " and GrailAccount in @grail_account" + \
                       " and Service in @aws_services"

            fdf = cnu_df.query(df_query)
            category = 'UsageType'

            cat_dict, cat_sizes = get_cat_sizes_dict(category, fdf, key_filter)
            choices = cat_dict
            selected = old_selected or list(cat_dict.keys())[:1]

            ui.update_checkbox_group(
                "aws_service_feature_group",
                choices=choices,
                selected=selected,
            )

        # @reactive.Effect
        # def _z():
        #     startD, endD = input.date_range()
        #     significant_svc_ut_df = costs_data_svc.get_significant_svc_df(
        #         startD, endD)

        #     svc_costs = significant_svc_ut_df[['Service','Cost']]\
        #             .groupby('Service').sum().reset_index().sort_values(by='Cost',ascending=False)

        #     svc_costs['Label'] = svc_costs['Service'] + svc_costs[
        #         'Cost'].apply(lambda x: f"( {x:,} )")
        #     service_dict = dict(
        #         list(svc_costs[['Service', 'Label']].itertuples(index=False,
        #                                                         name=None)))

        #     selected_items = my_session_cache.get('selected_services',\
        #             [] if not service_dict else list(service_dict.keys())[0])

        #     ui.update_select(
        #         "service",
        #         choices=service_dict,
        #         selected=selected_items,
        #     )

        #     acct_costs = significant_svc_ut_df[['GrailAccount','Cost']]\
        #         .groupby('GrailAccount').sum().reset_index().sort_values(by='Cost',ascending=False)

        #     acct_costs['Label'] = acct_costs['GrailAccount'] + acct_costs[
        #         'Cost'].apply(lambda x: f"( {x:,} )")
        #     acct_dict = dict(
        #         list(acct_costs[['GrailAccount',
        #                          'Label']].itertuples(index=False, name=None)))

        #     selected_items = my_session_cache.get('selected_grail_accounts',\
        #             [] if not acct_dict else list(acct_dict.keys())[0])

        #     ui.update_select(
        #         "grail_account",
        #         choices=acct_dict,
        #         selected=selected_items,
        #     )

        # @reactive.Effect
        # def _a():
        #     # df_query = f"GrailAccount in {tuple(input.grail_account())} " + \
        #     #            f" and Service in {tuple(input.service())}"

        #     selected_services = input.service()
        #     my_session_cache['selected_services'] = selected_services

        #     df_query = f"Service in {tuple(selected_services)}"
        #     startD, endD = input.date_range()
        #     significant_svc_ut_df = costs_data_svc.get_significant_svc_df(
        #         startD, endD)
        #     significant_svc_ut_df_a = significant_svc_ut_df.query(df_query)[['UsageType','Cost','Anomaly']]\
        #         .groupby("UsageType").sum().reset_index().copy().sort_values(by='Cost',ascending=False)
        #     significant_svc_ut_df_a['Label'] = significant_svc_ut_df_a['UsageType']+\
        #             ' ('+significant_svc_ut_df_a['Cost'].apply(lambda x: f'{x:,}') +' / '+ significant_svc_ut_df_a['Anomaly'].astype(str)  +')'
        #     utypes_list = list(significant_svc_ut_df_a[['UsageType',
        #                                                 'Label']].itertuples(
        #                                                     index=False,
        #                                                     name=None))
        #     my_session_cache['utypes_list'] = utypes_list

        #     utype_dict = dict(utypes_list)

        #     selected_items = my_session_cache.get('selected_usage_types',\
        #             [] if not utype_dict else list(utype_dict.keys())[0])

        #     ui.update_select(
        #         "usage_type",
        #         choices=utype_dict,
        #         selected=selected_items,
        #     )

        #     ut_tags = set()
        #     for ut, _ in utypes_list:
        #         for tok in re.split('[-:.]+', ut):
        #             ut_tags.add(tok)
        #     ut_tags = list(ut_tags)
        #     ui.update_select("usage_type_tags", choices=['ALL'] + ut_tags)

        #     ui.update_select("usage_type_tags_exclude", choices=ut_tags)

        #     pass

        # @reactive.Effect
        # def _b():
        #     utypes_list = my_session_cache.get('utypes_list', None)
        #     if utypes_list is None: return

        #     selected_tags = input.usage_type_tags()
        #     selected_utypes_list = []
        #     if 'ALL' in selected_tags:
        #         selected_utypes_list = [ut for ut, _ in utypes_list]
        #     else:
        #         for ut, _ in utypes_list:
        #             for tag in selected_tags:
        #                 if tag in ut:
        #                     selected_utypes_list.append(ut)

        #     exclude_tags = input.usage_type_tags_exclude()
        #     exclude_set = set()
        #     for ut in selected_utypes_list:
        #         for tag in exclude_tags:
        #             if tag in ut:
        #                 exclude_set.add(ut)
        #     selected_utypes = list(set(selected_utypes_list) - exclude_set)

        #     ui.update_select(
        #         "usage_type",
        #         selected=selected_utypes,
        #     )

        @output
        @render.ui
        @reactive.event(input.aws_service_group,
                        input.aws_service_feature_group,
                        input.grail_account_group)
        def title():
            granularity_month = input.granularity_month()
            highlight_anomalies = input.highlight_anomalies()
            startD, endD = input.date_range()
            aws_services = input.aws_service_group()
            grail_account = input.grail_account_group()
            aws_service_feature = input.aws_service_feature_group()
            selection = [f"Dates in range {startD} - {endD}"]
            df_query_toks = ["timestamp >= @startD and timestamp < @endD"]
            if grail_account != '':
                df_query_toks.append("GrailAccount in @grail_account")
                selection.append(f"GrailAccount in {list(grail_account)}")
                category = 'GrailAccount'
            if aws_services != '':
                df_query_toks.append("Service in @aws_services")
                selection.append(f"Service in {list(aws_services)}")
                category = 'Service'
            if aws_service_feature != '':
                df_query_toks.append("UsageType in @aws_service_feature")
                selection.append(f"UsageType in {list(aws_service_feature)}")
                category = 'UsageType'

            df_query = " and ".join(df_query_toks)
            sub = my_session_cache['df_day'].query(
                df_query).copy()  # use it to create a subset
            if not sub.shape[0]: return

            cat_sizes = get_cost_and_anomaly_weeks_by_category(category, sub)
            total_amount = cat_sizes['Cost'].sum()
            anomaly_count = cat_sizes['Anomaly'].sum()
            # return f"query : {df_query}<br/>Blended Cost (without discount): {total_amount} USD"
            return ui.tags.div(ui.tags.p(
                f"[{' AND '.join(selection)}].",
                class_="code",
            ),
                               ui.tags.h5(f"Total Costs: {total_amount:,}", ),
                               ui.tags.p(f"Anomalies : {anomaly_count}.", ),
                               class_="title-area")

        # @output
        # @render.text
        # def txt():
        #     startD,endD = input.date_range()
        #     df_query = f"GrailAccount in {tuple(input.grail_account())} " + \
        #                f" and timestamp >= '{startD}' and timestamp < '{endD}'" + \
        #                f" and Service in {tuple(input.service())}" + \
        #                f" and UsageType in {tuple(input.usage_type())}"
        #     return df_query

        @output(id="out"
                )  # decorator to link this function to the "out" id in the UI
        @render.plot  # a decorator to indicate we want the plot renderer
        @reactive.event(input.aws_service_feature_group,
                        input.highlight_anomalies, input.grail_account_group)
        def plot():
            granularity_month = input.granularity_month()
            highlight_anomalies = input.highlight_anomalies()

            startD, endD = input.date_range()
            aws_services = input.aws_service_group()
            grail_account = input.grail_account_group()
            aws_service_feature = input.aws_service_feature_group()

            df_query_toks = ["timestamp >= @startD and timestamp < @endD"]
            if grail_account != '':
                df_query_toks.append("GrailAccount in @grail_account")
            if aws_services != '':
                df_query_toks.append("Service in @aws_services")
            if aws_service_feature != '':
                df_query_toks.append("UsageType in @aws_service_feature")

            df_query = " and ".join(df_query_toks)
            sub = my_session_cache['df_day'].query(
                df_query).copy()  # use it to create a subset

            fdf = my_session_cache[
                'df_month'] if granularity_month else my_session_cache['df_day']
            sub = fdf.query(df_query).copy()  # use it to create a subset

            if not sub.shape[0]: return

            fillColor = 'UsageType' if len(aws_services) == 1 else 'Service'

            plot = create_plot(sub,
                               highlight_anomalies,
                               granularity_month,
                               fill=fillColor)  # create our plot
            return plot  # and return it

    return f


cnu_df = costs_data_svc.get_cnu_df_with_anomaly_info()

frontend = create_ui(cnu_df)
server = create_server(cnu_df)

www_dir = Path(__file__).parent / "www"
app = App(frontend, server, static_assets=www_dir)
