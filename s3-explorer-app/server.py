# diamonds.py
from nis import cat
from unicodedata import category
from shiny import App, ui, render, reactive, req
import plotnine as gg
import pandas as pd
import datetime
import data_svc


# function that creates our UI based on the data
# we give it
def create_ui(df: pd.DataFrame):

    max_date = df['timestamp'].max()
    min_date = df['timestamp'].min()
    start_date = max_date - datetime.timedelta(days=61)

    # calculate the set of unique choices that could be made
    # create our ui object
    app_ui = ui.page_fluid(
        # row and column here are functions
        # to aid laying out our page in an organised fashion
        ui.row(
            ui.column(2),
            ui.column(
                6,
                # an output container in which to render a plot
                ui.output_plot("out", width="100%", height="600px"),
                ui.output_text_verbatim("txt"),
            ),
        ),
        ui.row(
            ui.column(2),
            ui.column(
                3,
                ui.input_date_range(
                    "date_range",
                    "Date range:",
                    start=start_date.strftime('%Y-%m-%d'),
                    end=max_date.strftime('%Y-%m-%d'),
                    min=min_date.strftime('%Y-%m-%d'),
                    max=max_date.strftime('%Y-%m-%d'),
                    format="mm/dd/yy",
                    separator=" - ",
                ),
            ),
            ui.column(
                3,
                ui.input_select("facet_column", "Facet", [None,'GrailAccount','BucketName','StorageType']),
            ),
            ui.column(
                3,
                ui.input_select("color_column", "Color", [None,'GrailAccount','BucketName','StorageType']),

            ),
            ui.column(
                1,
                ui.input_action_button("go",
                                       "Go Plot!",
                                       class_="my-3 btn-success"),
            ),
        ),
        ui.row(
            ui.column(2),
            ui.column(
                10,
                ui.row(
                    ui.column(
                        3,
                        ui.input_checkbox_group(
                            "pipeline_stage_group",
                            "Bucket Use by Pipeline Stage",
                            {},
                        ),
                    ),
                    ui.column(
                        3,
                        ui.input_checkbox_group(
                            "grail_account_group",
                            "Bucket Use by Grail Account",
                            {},
                        ),
                    ),
                    ui.column(
                        3,
                        ui.input_text("bucket_name_filter", "Bucket Filter:", ""),
                        ui.input_checkbox_group(
                            "bucket_group",
                            "Buckets",
                            {},
                        ),
                    ),
                    ui.column(
                        3,
                        ui.input_checkbox_group(
                            "storage_tier_group",
                            "S3 Storage Tier",
                            {},
                        ),
                    ),
                ),
                # ui.output_text("txt"),
            )))
    return app_ui


# utility function to draw a scatter plot
def create_plot(df,facet_column=None,fill_column=None):
    xdf = df[['timestamp', 'BucketName', 'Bytes', 'GrailAccount','StorageType']].groupby(
        by=['timestamp', 'BucketName', 'GrailAccount','StorageType']).sum().reset_index()
    xdf['PetaBytes'] = (xdf['Bytes'] * 1e-15).round(2)


    if fill_column:
        plot = (
            gg.ggplot(xdf, gg.aes(x='timestamp', y='PetaBytes', fill=fill_column))+
            gg.geom_bar(stat="identity") +
            gg.theme_linedraw() +
            gg.theme(legend_position="top",
                    axis_text_x=gg.element_text(angle=15, hjust=1),
                    text=gg.element_text(size=12)) +
            gg.labs(
                title=f"Daily Volume Snapshot", x="Date", y=f"Size in Peta Bytes"))
    else:
        plot = (
            gg.ggplot(xdf, gg.aes(x='timestamp', y='PetaBytes'))+
            gg.geom_bar(stat="identity") +
            gg.theme_linedraw() +
            gg.theme(legend_position="top",
                    axis_text_x=gg.element_text(angle=15, hjust=1),
                    text=gg.element_text(size=12)) +
            gg.labs(
                title=f"Daily Volume Snapshot", x="Date", y=f"Size in Peta Bytes"))

    if facet_column:
        plot += gg.facet_grid(f"{facet_column} ~ .", scales="free", space="free")

    return plot.draw()


def create_bucket_plot(df):
    xdf = df[[
        'timestamp', 'StorageType', 'Bytes', 'GrailAccount', "BucketName"
    ]].groupby(by=['timestamp', 'StorageType', 'GrailAccount', "BucketName"
                   ]).sum().reset_index()
    xdf['PetaBytes'] = (xdf['Bytes'] * 1e-15).round(2)
    # print(xdf.head())
    plot = (
        gg.ggplot(xdf, gg.aes(x='timestamp', y='PetaBytes', fill="BucketName"))
        + gg.geom_bar(stat="identity") +
        gg.facet_grid("GrailAccount ~ .", scales="free", space="free") +
        gg.theme_linedraw() +
        gg.theme(legend_position="bottom",
                 axis_text_x=gg.element_text(angle=15, hjust=1),
                 text=gg.element_text(size=14)) +
        gg.labs(
            title=f"Daily Volume Snapshot", x="Date", y=f"Size in Peta Bytes"))

    return plot.draw()


# wrapper function for the server, allows the data
# to be passed in


def get_cat_sizes_dict(category, fdf, session_cache):
    cat_dict_old = session_cache.get(category, {})
    session_cache[category] = {}
    cat_dict = session_cache[category]

    cat_sizes = fdf[[category,'timestamp','Bytes']]\
      .groupby(by=[category,'timestamp']).sum().reset_index()\
      .groupby(by=[category]).mean(numeric_only=True).reset_index().sort_values(by='Bytes',ascending=False)
    cat_sizes['PBytes'] = (cat_sizes["Bytes"] * 1e-15).round(2)
    # cat_sizes['Label'] = cat_sizes.apply(
    #     lambda row: f'{row[category]} ({row["PBytes"]:,}) ', axis=1)
    cat_sizes['Label'] = cat_sizes.apply(lambda row: ui.HTML(
        f'<span> <span class="category-name">{row[category]}</span> <span class="badge alert-info">{row["PBytes"]} PB</span></span>'
    ),
                                         axis=1)

    for k, l in list(cat_sizes[[category, 'Label']].itertuples(index=False,
                                                               name=None)):
        cat_dict[k] = (l, cat_dict_old.get(k, (None, False))[-1])
    return cat_dict, cat_sizes


def create_server(df):

    def f(input, output, session):
        session_cache = {}

        @reactive.Effect
        def _a0a():
            # df = df[['timestamp', 'Bytes', 'BucketName', 'StorageType','AWSAccount', 'GrailAccount','PipelineBucketType']]
            startD, endD = input.date_range()
            df_query = "timestamp >= @startD and timestamp < @endD"
            fdf = df.query(df_query)
            category = 'PipelineBucketType'
            cat_dict, cat_sizes = get_cat_sizes_dict(category, fdf,
                                                     session_cache)
            choices = {k: v[0] for k, v in cat_dict.items()}
            selected = [k for k, v in cat_dict.items() if v[1]] or list(
                cat_dict.keys())[:1]

            ui.update_checkbox_group(
                "pipeline_stage_group",
                choices=choices,
                selected=selected,
            )

        @reactive.Effect
        def _a0b():
            # df = df[['timestamp', 'Bytes', 'BucketName', 'StorageType','AWSAccount', 'GrailAccount','PipelineBucketType']]
            pipeline_stage = input.pipeline_stage_group()
            with reactive.isolate():
                startD, endD = input.date_range()
            df_query =  "timestamp >= @startD and timestamp < @endD" + \
                        f" and PipelineBucketType in @pipeline_stage"
            fdf = df.query(df_query)
            category = 'GrailAccount'
            cat_dict, cat_sizes = get_cat_sizes_dict(category, fdf,
                                                     session_cache)
            choices = {k: v[0] for k, v in cat_dict.items()}
            selected = [k for k, v in cat_dict.items() if v[1]] or list(
                cat_dict.keys())[:1]

            ui.update_checkbox_group(
                "grail_account_group",
                choices=choices,
                selected=selected,
            )

        @reactive.Effect
        def _a0c():
            # df = df[['timestamp', 'Bytes', 'BucketName', 'StorageType','AWSAccount', 'GrailAccount','PipelineBucketType']]
            grail_account = input.grail_account_group()
            pipeline_stage = input.pipeline_stage_group()
            bucket_name_filter = input.bucket_name_filter().strip()
            with reactive.isolate():
                startD, endD = input.date_range()

            df_query =  "timestamp >= @startD and timestamp < @endD" + \
                        f" and GrailAccount in @grail_account" +\
                        f" and PipelineBucketType in @pipeline_stage"
            if bucket_name_filter != '':
                df_query += f" and BucketName.str.contains(@bucket_name_filter)"
            fdf = df.query(df_query)
            category = 'BucketName'
            cat_dict, cat_sizes = get_cat_sizes_dict(category, fdf,
                                                     session_cache)
            choices = {k: v[0] for k, v in cat_dict.items()}
            selected = [k for k, v in cat_dict.items() if v[1]] or list(
                cat_dict.keys())[:1]

            ui.update_checkbox_group(
                "bucket_group",
                choices=choices,
                selected=selected,
            )

        @reactive.Effect
        def _a0d():
            # df = df[['timestamp', 'Bytes', 'BucketName', 'StorageType','AWSAccount', 'GrailAccount','PipelineBucketType']]
            bucket_list = input.bucket_group()
            with reactive.isolate():
                startD, endD = input.date_range()

            df_query = f"BucketName in @bucket_list " + \
                        f" and timestamp >= @startD and timestamp < @endD"
            fdf = df.query(df_query)
            category = 'StorageType'
            cat_dict, cat_sizes = get_cat_sizes_dict(category, fdf,
                                                     session_cache)
            choices = {k: v[0] for k, v in cat_dict.items()}
            selected = [k for k, v in cat_dict.items() if v[1]] or list(
                cat_dict.keys())[:1]

            ui.update_checkbox_group(
                "storage_tier_group",
                choices=choices,
                selected=selected,
            )

        @output
        @render.text
        def txt():
            grail_account = input.grail_account_group()
            pipeline_stage = input.pipeline_stage_group()
            with reactive.isolate():
                startD, endD = input.date_range()
            bucket_list = input.bucket_group()
            storage_tiers = input.storage_tier_group()


            df_query = f"timestamp >= {startD} and timestamp < {endD} "
            if len(bucket_list) > 0:
                df_query += f" and BucketName in {bucket_list}" 
            if len(grail_account) > 0:
                df_query += f" and GrailAccount in {grail_account}" 
            if len(pipeline_stage) > 0 :
                df_query += f" and PipelineBucketType in {pipeline_stage}"
            if len(storage_tiers) > 0:
                df_query += f" and StorageType in {storage_tiers}"
            return df_query

        @output(id="out"
                )  # decorator to link this function to the "out" id in the UI
        @render.plot  # a decorator to indicate we want the plot renderer
        @reactive.event(input.go, ignore_none=False)
        def plot():
            grail_account = input.grail_account_group()
            pipeline_stage = input.pipeline_stage_group()
            bucket_name_filter = input.bucket_name_filter().strip()
            storage_tiers = input.storage_tier_group()
            with reactive.isolate():
                startD, endD = input.date_range()
            bucket_list = input.bucket_group()

            fill_column = input.color_column()
            facet_column = input.facet_column()

            df_query = "timestamp >= @startD and timestamp < @endD "
            if len(bucket_list) > 0:
                df_query += " and BucketName in @bucket_list" 
            if len(grail_account) > 0:
                df_query += " and GrailAccount in @grail_account" 
            if len(pipeline_stage) > 0 :
                df_query += " and PipelineBucketType in @pipeline_stage"
            if len(storage_tiers) > 0:
                df_query += " and StorageType in @storage_tiers"
            sub = df.query(df_query).copy()  # use it to create a subset
            if sub.shape[0] == 0: return
            plot = create_plot(sub,facet_column=facet_column    ,fill_column=fill_column)  # create our plot
            return plot  # and return it

    return f


s3_df = data_svc.get_s3_df()

frontend = create_ui(s3_df)

server = create_server(s3_df)

app = App(frontend, server)