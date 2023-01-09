# diamonds.py
from unicodedata import category
from shiny import App, ui, render, reactive
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
            ui.column(
                2,
                offset=1,
                *[
                    # an input widget that allows us to select multiple values
                    # from the set of choices
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
                    ui.input_selectize("pipeline_stage",
                                       "Pipeline Stage",
                                       choices=[],
                                       multiple=True),
                    ui.input_selectize("grail_account",
                                       "Grail Account",
                                       choices=[],
                                       multiple=True),
                    ui.input_selectize("bucket_name",
                                       "Bucket(s)",
                                       choices=[],
                                       multiple=True),
                    ui.input_text("bucket_name_filter", "Bucket Filter:", ""),
                ]),
            ui.column(1),
            ui.column(
                8,
                # an output container in which to render a plot
                ui.output_plot("out", width="100%", height="400px"),
                ui.output_text_verbatim("txt"),
                ui.row(
                  ui.column(3,ui.output_ui("pipeline_stage_sizes"), ),
                  ui.column(3,ui.output_ui("grail_account_sizes"), ),
                  ui.column(3,ui.output_ui("bucket_sizes"), ),
                ),
                # ui.output_text("txt"),
            )))
    return app_ui


# utility function to draw a scatter plot
def create_plot(df):
    xdf = df[['timestamp', 'BucketName', 'Bytes'
              ]].groupby(by=['timestamp', 'BucketName']).sum().reset_index()
    xdf['PetaBytes'] = (xdf['Bytes'] * 1e-15).round(2)
    plot = (gg.ggplot(
        xdf, gg.aes(x='timestamp', y='PetaBytes', fill="BucketName")) +
            gg.geom_bar(stat="identity") +
            gg.theme(axis_text_x=gg.element_text(angle=15, hjust=1)))
    return plot.draw()


def create_bucket_plot(df):
    xdf = df[['timestamp', 'StorageType', 'Bytes'
              ]].groupby(by=['timestamp', 'StorageType']).sum().reset_index()
    xdf['PetaBytes'] = (xdf['Bytes'] * 1e-15).round(2)
    print(xdf.head())
    plot = (gg.ggplot(
        xdf, gg.aes(x='timestamp', y='PetaBytes', fill="StorageType")) +
            gg.geom_bar(stat="identity") +
            gg.theme(axis_text_x=gg.element_text(angle=15, hjust=1)))
    return plot.draw()


# wrapper function for the server, allows the data
# to be passed in


def get_cat_sizes_dict(category, fdf):
    cat_sizes = fdf[[category,'timestamp','Bytes']]\
      .groupby(by=[category,'timestamp']).sum().reset_index()\
      .groupby(by=[category]).mean().reset_index().sort_values(by='Bytes',ascending=False)
    pbytes = cat_sizes['PBytes'] = (cat_sizes["Bytes"] * 1e-15).round(2)
    cat_sizes['Label'] = cat_sizes[category] + ' ( ' + pbytes.apply(
        lambda x: f"{x:,}") + ' )'
    cat_dict = dict(
        list(cat_sizes[[category, 'Label']].itertuples(index=False,
                                                       name=None)))
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
            cat_dict, cat_sizes = get_cat_sizes_dict(category, fdf)
            selected_items = session_cache.get('pipeline_stage',\
                    [] if not cat_dict else list(cat_dict.keys())[0])
            ui.update_select(
                "pipeline_stage",
                choices=cat_dict,
                selected=selected_items,
            )

        @output
        @render.table
        def pipeline_stage_sizes():
            # df = df[['timestamp', 'Bytes', 'BucketName', 'StorageType','AWSAccount', 'GrailAccount','PipelineBucketType']]
            startD, endD = input.date_range()
            pipeline_stage = input.pipeline_stage()
            df_query =  "timestamp >= @startD and timestamp < @endD" + \
                        f" and PipelineBucketType in @pipeline_stage"
            fdf = df.query(df_query)
            category = 'PipelineBucketType'
            _, cat_sizes = get_cat_sizes_dict(category, fdf)
            cat_sizes = cat_sizes[[category,'PBytes']]

            category = 'GrailAccount'
            cat_dict, _ = get_cat_sizes_dict(category, fdf)
            selected_items = session_cache.get('grail_account',\
                    [] if not cat_dict else list(cat_dict.keys())[0])
            ui.update_select(
                "grail_account",
                choices=cat_dict,
                selected=selected_items,
            )

            return cat_sizes

             

        @output
        @render.table
        def grail_account_sizes():
            # df = df[['timestamp', 'Bytes', 'BucketName', 'StorageType','AWSAccount', 'GrailAccount','PipelineBucketType']]
            startD, endD = input.date_range()
            pipeline_stage = input.pipeline_stage()
            df_query =  "timestamp >= @startD and timestamp < @endD" +\
                        f" and PipelineBucketType in @pipeline_stage" 

            fdf = df.query(df_query)
            
            category = 'GrailAccount'
            _, cat_sizes = get_cat_sizes_dict(category, fdf)
            cat_sizes = cat_sizes[[category,'PBytes']]

            category = 'BucketName'
            cat_dict, _ = get_cat_sizes_dict(category, fdf)
            selected_items = session_cache.get('bucket_name',\
                    [] if not cat_dict else list(cat_dict.keys())[0])
            ui.update_select(
                "bucket_name",
                choices=cat_dict,
                selected=selected_items,
            )
            return cat_sizes

        @output
        @render.table
        def bucket_sizes():
            # df = df[['timestamp', 'Bytes', 'BucketName', 'StorageType','AWSAccount', 'GrailAccount','PipelineBucketType']]
            startD, endD = input.date_range()
            pipeline_stage = input.pipeline_stage()
            grail_account = input.grail_account()
            df_query =  "timestamp >= @startD and timestamp < @endD" +\
                        f" and PipelineBucketType in @pipeline_stage" +\
                        f" and GrailAccount in @grail_account"

            fdf = df.query(df_query)
            
            category = 'BucketName'
            cat_dict, cat_sizes = get_cat_sizes_dict(category, fdf)
            cat_sizes = cat_sizes[[category,'PBytes']]
            
            return cat_sizes


        # @reactive.Effect
        # def _a0c():
        #     # df = df[['timestamp', 'Bytes', 'BucketName', 'StorageType','AWSAccount', 'GrailAccount','PipelineBucketType']]
        #     startD, endD = input.date_range()
        #     pipeline_stage = input.pipeline_stage()
        #     grail_account = input.grail_account()
        #     session_cache['grail_account'] = grail_account
        #     df_query =  "timestamp >= @startD and timestamp < @endD" +\
        #                 f" and PipelineBucketType in @pipeline_stage" +\
        #                 f" and GrailAccount in @grail_account"

        #     fdf = df.query(df_query)
        #     cat_dict, cat_sizes = get_cat_sizes_dict('BucketName', fdf)
        #     selected_items = session_cache.get('bucket_name',\
        #             [] if not cat_dict else list(cat_dict.keys())[0])
        #     ui.update_select(
        #         "bucket_name",
        #         choices=cat_dict,
        #         selected=selected_items,
        #     )

        @output
        @render.text
        def txt():
            grail_account = list(
                (input.grail_account()
                 ))  # access the input value bound to the id "select"
            pipeline_stage = input.pipeline_stage()
            startD, endD = input.date_range()
            df_query = f"GrailAccount in @grail_account " + \
                        f" and timestamp >= @startD and timestamp < @endD" + \
                        f" and PipelineBucketType in @pipeline_stage"
            return df_query

        @output(id="out"
                )  # decorator to link this function to the "out" id in the UI
        @render.plot  # a decorator to indicate we want the plot renderer
        def plot():
            grail_account = list(
                (input.grail_account()
                 ))  # access the input value bound to the id "select"
            pipeline_stage = input.pipeline_stage()
            startD, endD = input.date_range()
            bucket_list = input.bucket_name()
            if len(bucket_list) == 0:
                df_query = f"GrailAccount in @grail_account " + \
                  f" and timestamp >= @startD and timestamp < @endD" + \
                  f" and PipelineBucketType in @pipeline_stage"
                sub = df.query(df_query).copy()  # use it to create a subset
                if sub.shape[0] == 0: return
                plot = create_plot(sub)  # create our plot
                return plot  # and return it
            else:
                df_query = f"BucketName in @bucket_list " + \
                            f" and timestamp >= @startD and timestamp < @endD"
                sub = df.query(df_query).copy()  # use it to create a subset
                if sub.shape[0] == 0: return
                plot = create_bucket_plot(sub)  # create our plot
                return plot  # and return it

    return f


s3_df = data_svc.get_s3_df()

frontend = create_ui(s3_df)

server = create_server(s3_df)

app = App(frontend, server)