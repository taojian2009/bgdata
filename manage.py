from flask import Flask, render_template

from flask import Flask

app = Flask(__name__,
            static_url_path='',
            static_folder='_book',
            template_folder='_book')


@app.route("/")
def hello():
    return render_template('index.html')


if __name__ == "__main__":
    app.run(host='0.0.0.0')
