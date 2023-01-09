from shiny import App, render, ui
import requests


app_ui = ui.page_fluid(
    ui.h2("Hello Shiny!"),
    ui.input_slider("n", "N", 0, 100, 20),
    ui.output_text_verbatim("txt"),
    ui.img(src=f"http://libra:7001/S2000/2007/02/16/IMG_0058.JPG"),
)


def server(input, output, session):
    @output
    @render.text
    def txt():
        return f"n*2 is {input.n() * 2}"

url = 'http://libra:7001/S2000__list.json'
resp = requests.get(url=url)
data = resp.json()

app = App(app_ui, server)
