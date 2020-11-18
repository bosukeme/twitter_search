from flask_restful import Api
from flask import Flask
from resources import Twitter

app = Flask(__name__)

api = Api(app)

@app.route("/")
def home():
    return "<hi style='color:blue'> This is the Twitter Search Script Pipeline </h1>"


api.add_resource(Twitter, '/twitter_search')

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)