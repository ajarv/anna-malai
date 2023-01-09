# diamonds.py
from shiny import App, ui, render
import plotnine as gg
import pandas as pd
from datetime import date

import sqlalchemy


def get_mysql_engine():
    database_username = 'trimble'
    database_password = 'trimble'
    db_url = sqlalchemy.engine.url.URL.create(
        drivername="mysql+mysqlconnector",
        username=database_username,
        password=database_password,
        host='127.0.0.1',
        port=3316,
        database='trimble',
        query={"ssl_ca": '/Users/avashisth/.ssh/rds-combined-ca-bundle.pem'},
    )
    engine = sqlalchemy.create_engine(db_url, pool_recycle=1, pool_timeout=57600)
    return engine

def get_df():
    query = """
        SELECT 
            CONCAT(CallerType,'/',CallerName) Caller ,
            Bucket, 
            GrailAccount,
            SUM(pbam.AccessCount) CallCount  
        FROM PIPELINE_BUCKET_ACCESS_METRICS pbam 
        GROUP BY CallerType,CallerName,AccessType,Bucket,GrailAccount
        ORDER BY CallCount DESC
    """
    df = pd.read_sql(query,get_mysql_engine())
    return df

def get_cnu_df():
    BASE_PATH='/Users/avashisth/workspace/aws-util/cost-explorer/data'
    gaccts = ['clinical', 'grail-sysinfra-eng', 'eng', 'grail-sysinfra-prod', 'grail-prod-galleri', 'msk',
            'aws-grail-sequence-data-archives', 'grail-prod-mrd']
    df = pd.concat([
        pd.read_csv(f'{BASE_PATH}/metrics/{account}/cost-n-usage-x-svc-usetype.csv.gz')
        for account in gaccts
    ])    
    return df


# function that creates our UI based on the data
# we give it
def create_ui(df: pd.DataFrame):
  # calculate the set of unique choices that could be made
  choices = df['GrailAccount'].unique().tolist()
  pipeline_use = ['seq','fastq','results','cache','refer']
  # create our ui object
  app_ui = ui.page_fluid(
    # row and column here are functions
    # to aid laying out our page in an organised fashion
    ui.row(
      ui.column(2, offset=1,*[
        # an input widget that allows us to select multiple values
        # from the set of choices
        ui.input_selectize(
          "grail_account", "Grail Account",
          choices=list(choices),
          multiple=True
        ),
        ui.input_selectize(
          "pipeline_stage", "Pipeline Stage",
          choices=pipeline_use,
          multiple=False
        ),
        ui.input_date_range(
        "daterange3",
        "Date range:",
        start="2021-01-01",
        end="2022-12-31",
        min="2021-01-01",
        max="2022-12-31",
        format="mm/dd/yy",
        separator=" - ",
        ),]
      ),
      ui.column(1),
      ui.column(6,
        # an output container in which to render a plot
        ui.output_plot("out"),
        ui.output_text_verbatim("txt"),
      )
    )
  )
  return app_ui

# utility function to draw a scatter plot
def create_plot(df):
  plot = (
    gg.ggplot(df, gg.aes(x = 'Bucket', y='CallCount', fill='GrailAccount')) + 
      gg.geom_bar(stat="identity")+
      gg.theme(axis_text_x = gg.element_text(angle=15, hjust=1))
   )
  return plot.draw()

# wrapper function for the server, allows the data
# to be passed in
def create_server(df):
  def f(input, output, session):
    @output
    @render.text
    def txt():
        return f"{input.daterange3()}"
    
    @output(id="out") # decorator to link this function to the "out" id in the UI
    @render.plot # a decorator to indicate we want the plot renderer
    def plot():
      grail_account = list(input.grail_account()) # access the input value bound to the id "select"
      pipeline_stage = input.pipeline_stage()
      df_query = f"GrailAccount in {tuple(grail_account)} and Bucket.str.contains('{pipeline_stage}')"
      print(df_query)
      sub = df.query(df_query) # use it to create a subset
      plot = create_plot(sub) # create our plot
      return plot # and return it
  return f

s3_info_df = get_df()

frontend = create_ui(s3_info_df)

server = create_server(s3_info_df)

app = App(frontend, server)