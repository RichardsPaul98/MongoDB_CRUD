from flask import Flask, request, json, Response
from pymongo import MongoClient
import logging as log

# initialized the Flask APP
app = Flask(__name__)

"""
TODO Test the Flask API. 
TODO Take Screenshot of the response.
TODO complete the documentation. 
"""

class CrudAPI:  # MongoDB Model for ToDo CRUD Implementation
    def __init__(self, data):   # Fetchs the MongoDB, by making use of Request Body
        log.basicConfig(level=log.ALL, format='%(asctime)s %(levelname)s: %(message)s\n')
        self.client = MongoClient("mongodb://localhost:27017/")
        database = data['database']
        collection = data['collection']
        cursor = self.client[database]
        self.collection = cursor[collection]
        self.data = data

    def insert_data(self, data):    # Create - (1) explained in next section
        new_document = data['Document']
        response = self.collection.insert_one(new_document)
        output = {'Status': 'Successfully Inserted',
                  'Document_ID': str(response.inserted_id)}
        return output

    def read(self):                 # Read - (2) explained in next section
        documents = self.collection.find()
        output = [{item: data[item] for item in data if item != '_id'} for data in documents]
        return output

    def update_data(self):          # Update - (3) explained in next section
        filter = self.data['Filter']
        updated_data = {"$set": self.data['DataToBeUpdated']}
        response = self.collection.update_one(filter, updated_data)
        output = {'Status': 'Successfully Updated' if response.modified_count > 0 else "Nothing was updated."}
        return output

    def delete_data(self, data):    # Delete - (4) explained in next section
        filter = data['Filter']
        response = self.collection.delete_one(filter)
        output = {'Status': 'Successfully Deleted' if response.deleted_count > 0 else "Document not found."}
        return output

    def count_discounted_products_data(self):    # count_discounted_products_data
        aggregation = self.data['aggregation']
        documents = self.collection.aggregate(aggregation)
        output = [{item: data[item] for item in data if item != '_id'} for data in documents]
        return output
    
    def list_unique_brands_data(self):    # list_unique_brands
        aggregation = self.data['aggregation']
        documents = self.collection.aggregate(aggregation)
        output = [{item: data[item] for item in data if item != '_id'} for data in documents]
        return output
    
    def count_high_offer_price_data(self):    # count_high_offer_price
        greaterthan = self.data['Filter']['greaterthan']
        field_name = self.data['Filter']['field_name']
        # stri = f"{greaterthan}"
        filter = f"""{{ {field_name}: {{ $gt: {greaterthan} }} }}"""
        documents = self.collection.find(filter)
        output = [{item: data[item] for item in data if item != '_id'} for data in documents]
        return output

    def count_high_discount_data(self):    # count_high_discount
        greaterthan = self.data['percentage']
        documents = self.collection.find()
        output = [
            {
                item: data[item] for item in data if item != '_id' and 
                (((data['regular_price_value'] - data['offer_price_value ']/data['regular_price_value'])*100)>greaterthan)
            } 
            for data in documents
        ]

        return output

# Achieving CRUD through API - '/crud_mongodb'
@app.route('/crud_mongodb', methods=['GET'])     # Read MongoDB Document, through API and METHOD - GET
def read_data():
    data = request.json
    if data is None or data == {}:
        return Response(response=json.dumps({"Error": "Please provide connection information"}),
                        status=400, mimetype='application/json')
    read_obj = CrudAPI(data)
    response = read_obj.read()
    return Response(response=json.dumps(response), status=200,
                    mimetype='application/json')

@app.route('/crud_mongodb', methods=['POST'])    # Create MongoDB Document, through API and METHOD - POST
def create():
    data = request.json
    """
    TODO add condtions
    """
    if data is None or data == {} or 'Document' not in data:
        return Response(response=json.dumps({"Error": "Please provide connection information"}),
                        status=400, mimetype='application/json')
    create_obj = CrudAPI(data)
    response = create_obj.insert_data(data)
    return Response(response=json.dumps(response), status=200,
                    mimetype='application/json')

@app.route('/crud_mongodb', methods=['PUT'])     # Update MongoDB Document, through API and METHOD - PUT
def update():
    data = request.json
    """
    TODO add condtions
    """
    if data is None or data == {} or 'Filter' not in data:
        return Response(response=json.dumps({"Error": "Please provide connection information"}),
                        status=400, mimetype='application/json')
    update_obj = CrudAPI(data)
    response = update_obj.update_data()
    return Response(response=json.dumps(response), status=200,
                    mimetype='application/json')

@app.route('/crud_mongodb', methods=['DELETE'])   # Delete MongoDB Document, through API and METHOD - DELETE
def delete():
    data = request.json
    """
    TODO add condtions
    """
    if data is None or data == {} or 'Filter' not in data:
        return Response(response=json.dumps({"Error": "Please provide connection information"}),
                        status=400, mimetype='application/json')
    delete_obj = CrudAPI(data)
    response = delete_obj.delete_data(data)
    return Response(response=json.dumps(response), status=200,
                    mimetype='application/json')

@app.route('/count_discounted_products', methods=['GET'])  # How many products have a discount on them? and METHOD - GET
def count_discounted_products():
    data = request.json
    if data is None or data == {}:
        return Response(response=json.dumps({"Error": "Please provide connection information"}),
                        status=400, mimetype='application/json')
    read_obj = CrudAPI(data)
    response = read_obj.count_discounted_products_data()
    return Response(response=json.dumps(response), status=200,
                    mimetype='application/json')

@app.route('/list_unique_brands', methods=['GET'])  # How many unique brands are present in the collection? and METHOD - GET
def list_unique_brands():
    data = request.json
    if data is None or data == {}:
        return Response(response=json.dumps({"Error": "Please provide connection information"}),
                        status=400, mimetype='application/json')
    read_obj = CrudAPI(data)
    response = read_obj.list_unique_brands_data()
    return Response(response=json.dumps(response), status=200,
                    mimetype='application/json')

@app.route('/count_high_offer_price', methods=['GET'])  # How many products have offer price greater than 300? and METHOD - GET
def count_high_offer_price():
    data = request.json
    if data is None or data == {}:
        return Response(response=json.dumps({"Error": "Please provide connection information"}),
                        status=400, mimetype='application/json')
    read_obj = CrudAPI(data)
    response = read_obj.count_high_offer_price_data()
    return Response(response=json.dumps(response), status=200,
                    mimetype='application/json')

@app.route('/count_high_discount', methods=['GET'])  # How many products have discount % greater than 30%? and METHOD - GET
def count_high_discount():
    data = request.json
    if data is None or data == {}:
        return Response(response=json.dumps({"Error": "Please provide connection information"}),
                        status=400, mimetype='application/json')
    read_obj = CrudAPI(data)
    response = read_obj.count_high_offer_price_data()
    return Response(response=json.dumps(response), status=200,
                    mimetype='application/json')

if __name__ == '__main__':
    app.run(debug=True, port=5000, host='0.0.0.0')
