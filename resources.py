from flask_restful import Resource, reqparse
import twitter_search
from flask import request 


class Twitter(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('keyword', type=str, help="The search term word")
        args = parser.parse_args()

        result = twitter_search.call_all_functions(args["keyword"])

        return result

